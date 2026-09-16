// Copyright 2026 checkpointd authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// completionRetryInitialInterval/completionRetryMaxInterval bound complete()'s
// exponential backoff, retrying indefinitely until ctx ends.
const (
	completionRetryInitialInterval = 5 * time.Second
	completionRetryMaxInterval     = 30 * time.Second
)

// maxHistoryTurns bounds how many prior turns go into the chat request,
// keeping it inside the completion server's context window.
const maxHistoryTurns = 6

// turn is one past (user, assistant) exchange, used to build the next
// chat-completions request's message history.
type turn struct {
	User      string
	Assistant string
}

// chatMessage is one turn of a /v1/chat/completions request, matching OpenAI's
// standard shape.
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func buildMessages(system string, history []turn, input string) []chatMessage {
	if len(history) > maxHistoryTurns {
		history = history[len(history)-maxHistoryTurns:]
	}
	messages := make([]chatMessage, 0, 2*len(history)+2)
	messages = append(messages, chatMessage{Role: "system", Content: system})
	for _, t := range history {
		messages = append(messages, chatMessage{Role: "user", Content: t.User}, chatMessage{Role: "assistant", Content: t.Assistant})
	}
	messages = append(messages, chatMessage{Role: "user", Content: input})
	return messages
}

type chatCompletionRequest struct {
	Messages    []chatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
	MaxTokens   int           `json:"max_tokens"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

// completionClient disables keep-alives, since a pooled idle connection can resume
// as a stale socket after this actor is checkpointed and restored.
var completionClient = &http.Client{
	Transport: &http.Transport{DisableKeepAlives: true},
}

// complete calls addr's stateless llama.cpp /v1/chat/completions endpoint, retrying
// a connection failure with exponential backoff until ctx ends.
func complete(ctx context.Context, addr string, messages []chatMessage) (string, error) {
	body, err := json.Marshal(chatCompletionRequest{
		Messages:    messages,
		Temperature: 0.4,
		MaxTokens:   64,
	})
	if err != nil {
		return "", err
	}

	interval := completionRetryInitialInterval
	for {
		resp, err := sendCompletionRequest(ctx, addr, body)
		if err != nil {
			log.Printf("llama.cpp is unreachable at %s, retrying in %s: %v", addr, interval, err)
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("llama.cpp is unreachable at %s: %w", addr, ctx.Err())
			case <-time.After(interval):
			}
			interval = min(interval*2, completionRetryMaxInterval)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return "", fmt.Errorf("llama chat-completions returned %s: %s", resp.Status, b)
		}
		var out chatCompletionResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return "", fmt.Errorf("decoding llama chat-completions response: %w", err)
		}
		if len(out.Choices) == 0 {
			return "", fmt.Errorf("llama chat-completions returned no choices")
		}
		return strings.TrimSpace(out.Choices[0].Message.Content), nil
	}
}

// sendCompletionRequest issues one attempt of the chat-completions request, with a fresh
// io.Reader since each retry needs its own.
func sendCompletionRequest(ctx context.Context, addr string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := completionClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling llama chat-completions at %s: %w", addr, err)
	}
	return resp, nil
}
