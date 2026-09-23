# Flags

```
$ ./checkpointd -h
Run checkpointd as a long-lived A2A server: waits for SendMessage calls from
external A2A clients and drives each one's relay loop for as long as this
process keeps running.

Usage:
  checkpointd [flags]

Flags:
      --addr string                             address to serve on (default ":80")
      --base-url string                         this server's own externally-reachable base URL, stamped into each agent's AgentCard (e.g. http://checkpointd.<namespace>.svc:80) (default "http://localhost:80")
      --config string                           Path to the configuration file (default "/etc/checkpointd/checkpointd.yaml")
      --discover                                discover agents from Kubernetes ActorTemplate CRDs instead of the config file. Requires an in-cluster ServiceAccount granted get/list/watch on actortemplates.ate.dev
      --discovery-interval duration             how often to re-list ActorTemplate CRDs when --discover is set (default 30s)
  -h, --help                                    help for checkpointd
      --log-level string                        log level: panic, fatal, error, warn, info, debug, or trace (default "info")
      --log-retention-period duration           how long to keep a completed session's history after it becomes terminal. 0 (default) disables retention and keeps every session's history forever
      --log-retention-sweep-interval duration   how often to check for expired session history when --log-retention-period is set (default 1h0m0s)
      --pod-name string                         this replica's own stable identity, durably recorded as each session's owner so ownership survives a restart under the same identity -- pass the pod's own name via the Kubernetes Downward API. Required when the event log is Postgres-backed (multi-replica); ignored for SQLite (single-replica only, ownership is implicit)
      --pod-uid string                          this replica's own pod's Kubernetes UID (Downward API metadata.uid), durably recorded as each session's owner alongside --pod-name. Required when the event log is Postgres-backed (multi-replica); ignored for SQLite, same as --pod-name
      --salvage-sweep-interval duration         how often this instance scans sessions cluster-wide to find sessions whose recorded owner pod no longer exists (crashed, evicted, or force-deleted without a graceful drain) and salvages them by claiming and resuming their relay loop locally. A no-op unless both --pod-name and --pod-uid are set (default 1m0s)
      --substrate-ca-file string                PEM file with the Agent Substrate Control API's trust bundle (e.g. a mounted clusterTrustBundle projected volume), used to verify its server certificate; empty trusts any certificate, which is only appropriate for local testing.
      --substrate-endpoint string               Agent Substrate Control API address each discovered agent's harness dials; empty uses the ate package's own default
      --substrate-router-addr string            atenet-router's in-cluster CONNECT-ingress address, used to reach actor worker ports when --substrate-token-file exists (default "atenet-router.ate-system.svc:8081")
      --substrate-token-file string             bearer token file for the Agent Substrate Control API, re-read on every call; its existence also decides whether a worker is dialed directly (file absent: local/testing) or CONNECT-tunneled through atenet-router (file present: a real cluster's NetworkPolicy requires it) (default "/var/run/secrets/ate-system/token")
```

See [`./../internal/config/checkpointd/config.go`](./../internal/config/checkpointd/config.go) for the config file fields.
