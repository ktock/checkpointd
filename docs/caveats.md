# Caveats

- Don't delete and recreate an ActorTemplate (e.g. `kubectl delete` + `kubectl apply`) while a conversation using it might still need crash recovery — this assigns a new Kubernetes UID, which breaks snapshot-based recovery. Use an in-place `kubectl apply` update instead.
- `registry.substrate[].suspend_actor_timeout` defaults to 11 minutes and normally doesn't need to be set. If you run a different Agent Substrate deployment with a different `SuspendActor` server-side ceiling, set it a minute or two past that deployment's own value.
- `CancelTask` never signals the agent's own running logic (`AgentExecutor.Cancel` is never called), so don't rely on cancellation to trigger agent-side cleanup.
- Clients should store the `tenant` field from a reply's `Metadata` and resend it on later `GetTask`/`CancelTask`/`SendMessage` calls for the same task, to avoid an ambiguous-TaskID lookup failing as not-found.
- checkpointd's crash recovery deletes and recreates the actor from its last snapshot, so an `externalVolumeTemplate` volume does not survive it: Substrate deletes that volume along with the actor ([`ExternalVolumeTemplate` field docs](https://github.com/agent-substrate/substrate/blob/d909d690532b3e2e06496cfdd9e1200e56b2c27b/pkg/api/v1alpha1/actortemplate_types.go#L200-L202)). A `durableDir` volume is unaffected, since its contents live inside the snapshot itself.
