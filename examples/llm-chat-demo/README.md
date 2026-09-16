# Demo: LLM chat demo with 2 agents with crash recovery

- Prerequisites: `docker`, `kind`, `kubectl`, `go`, `git`

This runs two agents and a stateless [llama.cpp](https://github.com/ggml-org/llama.cpp) completion server (SmolLM2 135M on CPU) in the cluster.

- `chat-agent` ([`main.go`](./chat-agent/main.go)) is the agent the client talks to. It keeps a conversation log across turns (a `conversation` Go variable in memory), answers each turn itself via the llama.cpp completion server, and asks `reviewer-agent` for a second opinion on its own answer.
- `reviewer-agent` ([`main.go`](./reviewer-agent/main.go)) gives that second opinion, keeping its own separate conversation log the same way.

Start a kind cluster with Agent Substrate, checkpointd, and both demo agents deployed:

```sh
./examples/llm-chat-demo/setup.sh
```

Drive the whole scenario.

- send a couple of messages to `chat-agent`
- restart of one kind worker node
- one more message: the reply's own conversation log still carries every message from before the crash

```sh
./examples/llm-chat-demo/demo.sh
```

When you're done, tear the cluster down.

```sh
./examples/llm-chat-demo/cleanup.sh
```
