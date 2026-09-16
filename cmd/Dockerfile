# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# checkpointd image build:
#   --target checkpointd : debian:stable-slim + only the Go `checkpointd`
#                          binary (cmd/checkpointd) -- used by every
#                          checkpointd-server StatefulSet in this repo.
#
# Build context: repository root. Images target the cluster's linux/amd64 nodes,
# so build with `--platform linux/amd64`.
# For a standalone build:
#   docker build --platform linux/amd64 --target checkpointd -f cmd/Dockerfile -t <ref> . && docker push <ref>

# --- Stage 1: build the checkpointd Go binary --------------------------------
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/checkpointd ./cmd/checkpointd

# --- checkpointd image (Go-only) for the checkpointd-server StatefulSet ----
# Deliberately minimal: `checkpointd server` talks to the Agent Substrate Control
# API with InsecureSkipVerify (see internal/harness/substrate/substrate.go),
# so it needs no CA bundle, and no shell tools -- it's never `kubectl exec`ed
# into for debugging.
FROM debian:stable-slim AS checkpointd
WORKDIR /workspace

COPY --from=build /out/checkpointd /checkpointd-app/checkpointd

CMD ["/checkpointd-app/checkpointd"]
