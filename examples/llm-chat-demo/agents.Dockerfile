# Copyright 2026 checkpointd authors
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

# Builds the two examples/llm-chat-demo agents as ActorTemplate containers.
# setup.sh builds both targets and pushes them to the kind cluster's local
# registry:
#
# Build context: repository root.
#   docker build --target chat-agent -f examples/llm-chat-demo/agents.Dockerfile -t <ref> . && docker push <ref>
#   docker build --target reviewer-agent -f examples/llm-chat-demo/agents.Dockerfile -t <ref> . && docker push <ref>

FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/chat-agent ./examples/llm-chat-demo/chat-agent
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/reviewer-agent ./examples/llm-chat-demo/reviewer-agent

# chat-agent: the user-facing agent from the top-level README's Quick Start.
FROM debian:stable-slim AS chat-agent
COPY --from=build /out/chat-agent /app/chat-agent
EXPOSE 80
# This CMD only matters for a standalone `docker run`, since Substrate runs the ActorTemplate's `command` instead.
CMD ["/app/chat-agent", "--harness-addr", "0.0.0.0:80"]

# reviewer-agent: chat-agent's nested SendMessage target.
FROM debian:stable-slim AS reviewer-agent
COPY --from=build /out/reviewer-agent /app/reviewer-agent
EXPOSE 80
CMD ["/app/reviewer-agent", "--harness-addr", "0.0.0.0:80"]
