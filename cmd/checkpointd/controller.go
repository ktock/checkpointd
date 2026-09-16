// Copyright 2026 Google LLC
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
	"fmt"
	"log/slog"
	"os"
	"strings"

	config "github.com/ktock/checkpointd/internal/config/checkpointd"
	"github.com/ktock/checkpointd/internal/controller"
	"github.com/ktock/checkpointd/internal/controller/eventlog"
	"github.com/ktock/checkpointd/internal/harness/substrate"
	"github.com/ktock/checkpointd/proto"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

// slogLevel backs the slog handler below and is updated by --log-level once
// flags are parsed, after this file's own init() has already run.
var slogLevel = new(slog.LevelVar)

func init() {
	log.SetFormatter(&logrus.JSONFormatter{})
	// Some internal packages log via slog instead of this file's own logrus logger, so slog's default handler needs to be JSON too.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slogLevel,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.LevelKey {
				a.Value = slog.StringValue(strings.ToLower(a.Value.String()))
			}
			return a
		},
	})))
}

// setLogLevel applies level to both this process's loggers: this file's own
// logrus logger, and the slog default used by internal packages.
func setLogLevel(level logrus.Level) {
	log.SetLevel(level)
	switch {
	case level >= logrus.DebugLevel:
		slogLevel.Set(slog.LevelDebug)
	case level == logrus.InfoLevel:
		slogLevel.Set(slog.LevelInfo)
	case level == logrus.WarnLevel:
		slogLevel.Set(slog.LevelWarn)
	default:
		slogLevel.Set(slog.LevelError)
	}
}

func newController(ctx context.Context, cfg *config.Config, ctrlOpts substrate.ControlAPIOptions) (*controller.Controller, eventlog.EventLog, error) {
	reg := controller.NewRegistry()
	var defaultHarnessID string
	for _, sc := range cfg.Registry.Substrate {
		h, err := sc.NewHarness(sc.Endpoint, ctrlOpts)
		if err != nil {
			return nil, nil, fmt.Errorf("substrate harness %q: %w", sc.ID, err)
		}
		if err := reg.RegisterHarness(sc.ID, h); err != nil {
			return nil, nil, fmt.Errorf("register substrate harness %q: %w", sc.ID, err)
		}
		if sc.Default {
			defaultHarnessID = sc.ID
		}
	}
	if defaultHarnessID != "" {
		if err := reg.SetDefaultHarness(defaultHarnessID); err != nil {
			return nil, nil, fmt.Errorf("set default harness %q: %w", defaultHarnessID, err)
		}
	}

	el, err := buildEventLog(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("building event log: %w", err)
	}

	c, err := controller.New(ctx, controller.Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return el, nil },
	})
	if err != nil {
		return nil, nil, err
	}
	return c, el, nil
}

// buildEventLog constructs cfg.EventLog's configured backend.
func buildEventLog(cfg *config.Config) (eventlog.EventLog, error) {
	if cfg.EventLog.PostgresConfig.DSN != "" {
		dsn := os.ExpandEnv(cfg.EventLog.PostgresConfig.DSN)
		if dsn == "" {
			return nil, fmt.Errorf("eventlog: postgres dsn %q expanded to empty", cfg.EventLog.PostgresConfig.DSN)
		}
		return eventlog.OpenPostgresEventLog(dsn)
	}
	return eventlog.OpenSQLiteEventLog(cfg.EventLog.SQLiteConfig.Filename)
}

// rawExec runs req against c and returns the concatenated text of the response.
func rawExec(ctx context.Context, c *controller.Controller, req *proto.CreateInteractionEvent) (string, error) {
	var out strings.Builder
	var confirmation *proto.ConfirmationContent
	handler := controller.ExecHandler(func(resp *proto.CreateInteractionResponse) error {
		for _, step := range resp.GetOutputs() {
			contentStep := step.GetContent()
			if contentStep == nil {
				continue
			}
			for _, ct := range contentStep.Content {
				if conf := ct.GetConfirmation(); conf != nil {
					confirmation = conf
				}
				if t := ct.GetText(); t != nil {
					out.WriteString(t.Text)
				}
			}
		}
		return nil
	})
	if err := c.Exec(ctx, req, handler); err != nil {
		return "", err
	}
	if confirmation != nil {
		return "", fmt.Errorf("agent requested approval (%q); checkpointd does not support interactive confirmation", confirmation.GetQuestion())
	}
	if out.Len() == 0 {
		return "", fmt.Errorf("agent %s produced no output for this turn", req.AgentId)
	}
	return out.String(), nil
}

// userStep wraps text as a single user content step.
func userStep(text string) *proto.Step {
	return &proto.Step{
		Type: &proto.Step_Content{
			Content: &proto.ContentStep{
				Role: "user",
				Content: []*proto.Content{{
					Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}},
				}},
			},
		},
	}
}
