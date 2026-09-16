# Writing agents

The standard a2a-go SDK can be used to write an agent.
Our `github.com/ktock/checkpointd/harness` package provides plugins for a2a-go to enable checkpointd-based transport.

## Transferring A2A messages over checkpointd

`harness.NewAgentHarness` wraps a2a-go's `a2asrv.AgentExecutorFunc` to enable message exchange over checkpointd.
This function starts a gRPC server on a port which is connected from checkpointd to exchange A2A messages.

```go
harness.Serve(harnessAddr, harness.NewAgentHarness(a2asrv.AgentExecutorFunc(myAgentFunc)))
```

## Sending a message from an agent to another

### Agent discovery

Checkpointd exposes all agents' AgentCards on the `/agents/{id}/.well-known/agent-card.json` endpoint and the standard `agentcard.DefaultResolver.Resolve` can be used to fetch a card for agent discovery.

### Sending a message

An agent client can be instantiated using the standard `NewFromCard` function of a2a-go.
You need to pass our custom transport plugin `harness.WithTransport(execCtx)` to the constructor so that the messages are sent over checkpointd's relay.

Example:

```go
// Resolves the target agent's card
card, err := agentcard.DefaultResolver.Resolve(ctx, "http://"+*checkpointdAddr+"/agents/"+*reviewerAgentID+"/.well-known/agent-card.json")

// Create a client object for that agent
client, err := a2aclient.NewFromCard(ctx, card, a2aclient.WithDefaultsDisabled(), harness.WithTransport(execCtx))

// Send a message to the agent. Suspended until the response arrives.
ask := fmt.Sprintf("User asked: %s\nAssistant's answer: %s\nWhat's your take?", in, myAnswer)
res, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
	Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(ask)),
})
```

Every time an agent sends a message to another agent using `SendMessage`, checkpointd suspends the source agent and passes that message to the destination agent.
When a response arrives at the source agent, checkpointd resumes this agent and passes the message.

An agent's checkpoint might be called more than once in a turn (e.g. for retrying if the message is lost before being committed) so it should be safe to retry the turn.

See [`./deployment.md`](./deployment.md) about how to deploy agents.
