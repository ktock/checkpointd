# Resource lifetime management by sessions

A session is an entity that refers to agents and tasks triggered by an external client.
Every time an external client CLI sends a message to an agent for the first time, a session is created.
The callee agent might create a Task and call other agents to complete the task.
The created task and all called agents' event logs are tracked in that session.

Once the callee agent completes the task for the external client, this session is marked as TERMINATED and associated data will be deleted after the configured retention period (configurable via `--log-retention-period`; the default preserves them forever).
Note that there is no retention period support for actor snapshots as of now. 
They are removed as soon as the session is terminated.

Substrate actors called from checkpointd are isolated per session.
Even if two sessions call the same agent, checkpoints are separately created for each session.
For multi-turn communication in a session while preserving the state, A2A's InputRequired TaskStatus can be used.
