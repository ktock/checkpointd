# Running multiple checkpointd instances

Checkpointd can be replicated behind a round-robin load balancer, on a shared PostgreSQL event log database.

Either a Deployment or a StatefulSet can be used but StatefulSet is recommended because its stable pod name allows quick recovery of the checkpointd state after crash (see "Salvage of orphaned sessions" below for details).

An example manifest is available at [`../examples/llm-chat-demo/manifests/90-checkpointd-server.yaml`](../examples/llm-chat-demo/manifests/90-checkpointd-server.yaml).

## Session ownership

A session is an entity that tracks the entire conversation history (e.g. agents, tasks and message logs) triggered by an external client.
See [`./session.md`](./session.md) about the basics of a session.

A session is owned by one checkpointd instance at a time.
A checkpointd instance is identified by a tuple `(name, UID)` where:

- name: Pod name assigned by Kubernetes. Pass it to checkpointd via the `--pod-name` flag.
- UID: Pod UID assigned by Kubernetes. Pass it to checkpointd via the `--pod-uid` flag.

Every time a checkpointd instance receives a request message from an external A2A client, it does the following:

- Acquires the ownership of the session.
- Drives the message relay loop until the session reaches a terminal message or an InputRequired message.
- Releases the ownership of the session.

The external client can send a follow-up message to a session as a response to the InputRequired message.
This follow-up message can reach any checkpointd instance (not limited to the previously owned instance) because the receiving instance can take the ownership of the session and restore the relay state from the event log.
So a round-robin-based load balancer can be used in front of the checkpointd instances.

When a Pod shuts down because of scale-in, it performs the following graceful shutdown procedure.

- Refuses new requests.
- Drives the currently owned sessions towards termination or InputRequired.
- Releases the ownership of the sessions.
- Exits.

Later, the external client can send the session's follow-up message to other instances and they can take the ownership of the session to continue the conversation, as described above.

## Salvage of orphaned sessions

If a pod finishes without completing the graceful shutdown procedure because of shutdown timeout, node crash or other reasons, this will create orphaned sessions.

An orphaned session is owned by an instance `(name, UID)` that isn't running anymore.
Kubernetes doesn't reuse a UID, so this session's owner pod never comes back again.

To ensure those orphaned sessions keep running, each checkpointd instance performs automatic salvage of them.
A checkpointd instance periodically (once per minute by default, but configurable using the `--salvage-sweep-interval` flag) performs the following salvage routine:

- Look up every distinct `(name, UID)` pair currently recorded as owning an active session, cluster-wide.
- For each pair, check if the pod is still running on the cluster. This requires a ServiceAccount granting `get` of Pods.
- If it doesn't, this is an orphaned session, so this instance takes ownership of it and continues the relay loop.

Under a Deployment, a replacement pod gets a brand-new random name so it recovers the relay state through the full salvage procedure described above.
A StatefulSet's restarted instance recovers its previously-held sessions directly using its own stable pod name, without waiting on the full salvage loop completes.

## Salvage of sessions from a hanging instance

Checkpointd exposes a liveness endpoint at `/healthz`.
This endpoint can be used for Kubernetes's livenessProbe setting to detect a hanging instance and to exit it.

Kubernetes, by default, tries to restart the exited container without changing the Pod UID.
Therefore, if the hanging checkpointd instance results in a crash loop without proceeding with the relay loop, the sessions in that instance stay stuck forever without being released.

To prevent this, `restartPolicy: Never` should be configured on the checkpointd container (not the Pod, because this value is not allowed at the Pod level).
With this configuration, Kubernetes gives up retrying the exited checkpointd and immediately moves the Pod state to Failed.
From that moment, another healthy instance can salvage the stuck sessions and continue the relay loop.

## Forcible release of ownership

When checkpointd fails to release the ownership of a session (e.g. because of a lost DB connection, etc.), checkpointd panics and exits so that it forcibly loses the ownership of the session, giving another healthy instance a chance to salvage it.

To prevent sessions from getting stuck in the crash-looping checkpointd instance, `restartPolicy: Never` should be configured as explained above.

## Task Cancellation

A Task cancellation request can reach an instance that isn't driving the message relay loop on its own.
In this case, the instance that received the cancellation request marks this session as TERMINATING, indicating that there will be no further request reaching the session and that the relay loop should stop.

Once the instance owning the session and driving the relay loop detects that this session is marked as TERMINATING, it stops the relay loop and proceeds to clean up the actors.
So the cancellation might not be effective instantly but it will eventually interrupt the relay loop.

## Force deletion of Pods

[Force deletion of a pod](https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/) doesn't guarantee that the pod's process is actually terminated.

This can cause a race condition where a session's relay loop is driven by two concurrent checkpointd instances: the "zombie" instance (its Pod object is gone, but its process is still running) and the instance that salvaged the session.

To avoid this race condition, when forcibly deleting a pod, ensure its process is terminated, or that it no longer has access to the database and Substrate, in advance.
