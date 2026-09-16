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

# Builds the test agents used by script/test-k8s, one --target per binary: echo, long-wait, long-poll, testserver, tck-agent, crash-test-agent, crash-recovery-agent, agent-a, agent-b.
#   docker build --target echo -f script/test-k8s/manifests/agents.Dockerfile -t <ref> . && docker push <ref>

FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/echo-agent ./script/test-k8s/agents/echo_agent
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/long-wait ./script/test-k8s/agents/long_wait
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/long-poll ./script/test-k8s/agents/long_poll
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/testserver ./script/test-k8s/agents/testserver
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/tck-agent ./script/test-k8s/agents/tck_agent
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/crash-test-agent ./script/test-k8s/agents/crash_test_agent
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/crash-recovery-agent ./script/test-k8s/agents/crash_recovery_agent
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/agent-a ./script/test-k8s/agents/agent_a
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/agent-b ./script/test-k8s/agents/agent_b

FROM debian:stable-slim AS echo
COPY --from=build /out/echo-agent /app/echo-agent
EXPOSE 80
# This CMD only matters for a standalone `docker run`, since Substrate runs the ActorTemplate `command` instead.
CMD ["/app/echo-agent", "--harness-only", "--harness-addr", "0.0.0.0:80"]

FROM debian:stable-slim AS long-wait
COPY --from=build /out/long-wait /app/long-wait
EXPOSE 80
CMD ["/app/long-wait", "--harness-only", "--harness-addr", "0.0.0.0:80"]

FROM debian:stable-slim AS long-poll
COPY --from=build /out/long-poll /app/long-poll
EXPOSE 80
CMD ["/app/long-poll", "--harness-only", "--harness-addr", "0.0.0.0:80"]

FROM debian:stable-slim AS testserver
COPY --from=build /out/testserver /app/testserver
EXPOSE 8080
CMD ["/app/testserver", "--addr", "0.0.0.0:8080"]

FROM debian:stable-slim AS tck-agent
COPY --from=build /out/tck-agent /app/tck-agent
EXPOSE 80
CMD ["/app/tck-agent", "--harness-addr", "0.0.0.0:80"]

FROM debian:stable-slim AS crash-test-agent
COPY --from=build /out/crash-test-agent /app/crash-test-agent
EXPOSE 80
CMD ["/app/crash-test-agent", "--harness-addr", "0.0.0.0:80"]

FROM debian:stable-slim AS crash-recovery-agent
COPY --from=build /out/crash-recovery-agent /app/crash-recovery-agent
EXPOSE 80
CMD ["/app/crash-recovery-agent", "--harness-addr", "0.0.0.0:80"]

FROM debian:stable-slim AS agent-a
COPY --from=build /out/agent-a /app/agent-a
EXPOSE 80
CMD ["/app/agent-a", "--harness-addr", "0.0.0.0:80"]

FROM debian:stable-slim AS agent-b
COPY --from=build /out/agent-b /app/agent-b
EXPOSE 80
CMD ["/app/agent-b", "--harness-addr", "0.0.0.0:80"]
