# Demo: an agent that crashes itself, and recovers anyway

- Prerequisites: `docker`, `kind`, `kubectl`, `go`, `git`

The agent-side counterpart to [`../llm-chat-demo`](../llm-chat-demo)

- same two-agent (`chat-agent` + `reviewer-agent`) shape and the same stateless [llama.cpp](https://github.com/ggml-org/llama.cpp) completion server, but here it's `chat-agent` itself that crashes on roughly half the turns and at most once per turn, right after it asks `reviewer-agent` for a second opinion
- A small `crash-ledger` service remembers which turns have already crashed used to limit the number of crashes at most 1 per turn.

Start a kind cluster with Agent Substrate, checkpointd, and both demo agents deployed:

> NOTE: the demo downloads Qwen3 1.7B quantized by Unsloth (867MB): https://huggingface.co/unsloth/Qwen3-1.7B-GGUF/blob/main/Qwen3-1.7B-Q3_K_S.gguf

```sh
./examples/agent-crash-demo/setup.sh
```

Drive a several-turn conversation with `chat-agent`, which self-crashes along the way.
After every turn, the script confirms the reply's own conversation log still contains every earlier turn's message:

```sh
./examples/agent-crash-demo/demo.sh
```

When you're done, tear the cluster down.

```sh
./examples/agent-crash-demo/cleanup.sh
```
