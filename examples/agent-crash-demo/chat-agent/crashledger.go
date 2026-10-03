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
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

// ledgerRetryInitialInterval/ledgerRetryMaxInterval bound claimCrash's exponential backoff, retrying indefinitely until ctx ends.
const (
	ledgerRetryInitialInterval = 1 * time.Second
	ledgerRetryMaxInterval     = 10 * time.Second
)

// ledgerClient disables keep-alives, since a pooled idle connection can resume as a stale socket after this actor is checkpointed and restored.
var ledgerClient = &http.Client{
	Transport: &http.Transport{DisableKeepAlives: true},
	Timeout:   10 * time.Second,
}

// claimCrash asks the crash ledger at addr for permission to crash on turn, and reports whether this is the first time it was asked about that turn.
// An unreachable ledger is retried until ctx ends, since failing the turn would cache the error and replay it on every retry.
func claimCrash(ctx context.Context, addr, turn string) (bool, error) {
	endpoint := "http://" + addr + "/crash?turn=" + url.QueryEscape(turn)
	interval := ledgerRetryInitialInterval
	for {
		first, err := postCrash(ctx, endpoint)
		if err == nil {
			return first, nil
		}
		if ctx.Err() != nil {
			return false, fmt.Errorf("crash ledger at %s: %w", addr, ctx.Err())
		}
		var unexpected *unexpectedStatusError
		if errors.As(err, &unexpected) {
			return false, err
		}
		log.Printf("crash ledger is unreachable at %s, retrying in %s: %v", addr, interval, err)
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("crash ledger at %s is unreachable: %w", addr, ctx.Err())
		case <-time.After(interval):
		}
		interval = min(interval*2, ledgerRetryMaxInterval)
	}
}

// unexpectedStatusError is a ledger answer that is neither "first time" nor "already crashed", which retrying would not change.
type unexpectedStatusError struct{ status string }

func (e *unexpectedStatusError) Error() string {
	return "crash ledger returned " + e.status
}

// postCrash makes one attempt: true for 200, false for 409, an unexpectedStatusError for anything else.
func postCrash(ctx context.Context, endpoint string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return false, err
	}
	resp, err := ledgerClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusConflict:
		return false, nil
	default:
		return false, &unexpectedStatusError{status: resp.Status}
	}
}
