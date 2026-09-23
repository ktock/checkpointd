# Demo: LLM chat demo with 2 agents, multi-instance checkpointd, and crash recovery

- Prerequisites: `docker`, `kind`, `kubectl`, `go`, `git`

This runs two agents and a stateless [llama.cpp](https://github.com/ggml-org/llama.cpp) completion server (SmolLM2 135M on CPU) in a single-node cluster, behind `checkpointd` running as 2 replicas sharing one Postgres-backed event log/session store (see [`../../docs/replication.md`](../../docs/replication.md)).

- `chat-agent` ([`main.go`](./chat-agent/main.go)) is the agent the client talks to. It keeps a conversation log across turns (a `conversation` Go variable in memory), answers each turn itself via the llama.cpp completion server, and asks `reviewer-agent` for a second opinion on its own answer.
- `reviewer-agent` ([`main.go`](./reviewer-agent/main.go)) gives that second opinion, keeping its own separate conversation log the same way.

Start a kind cluster with Agent Substrate, Postgres, checkpointd (2 replicas), and both demo agents deployed:

```sh
./examples/llm-chat-demo/setup.sh
```

Drive the whole scenario, demonstrating two separate resilience properties in turn.

- send a message to `chat-agent`, targeted directly at one checkpointd-server replica
- send a follow-up, targeted directly at the other replica instead: it still continues the same session
- kill both replicas' checkpointd processes with SIGKILL and wait for Kubernetes to restart them
- one more message, sent via the round-robin Service now that both instances are Ready again: the reply's own conversation log still carries every message from before the crash

```sh
./examples/llm-chat-demo/demo.sh
```

When you're done, tear the cluster down.

```sh
./examples/llm-chat-demo/cleanup.sh
```
