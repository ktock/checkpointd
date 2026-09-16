# Deployment Example on KinD

This document describes how to deploy checkpointd on Kubernetes.

Run every command below from the root of this (checkpointd) repo checkout, unless a command explicitly `cd`s elsewhere first.

## Preparation: Deploying AgentSubstrate

Refer to the [Agent Substrate](https://github.com/agent-substrate/substrate) repo for official setup procedure.

As of `d909d690532b`, the KinD cluster can be set up as the following.

```sh
export SUBSTRATE_DIR=$(mktemp -d)
git clone https://github.com/agent-substrate/substrate "$SUBSTRATE_DIR"
cd "$SUBSTRATE_DIR"
git checkout d909d690532b3e2e06496cfdd9e1200e56b2c27b

# create a kind cluster
hack/create-kind-cluster.sh

# install Substrate
hack/install-ate-kind.sh --deploy-ate-system

# The rest of this guide's commands run from the checkpointd repo root.
cd -
```

In this example, we also deploy a shared llama.cpp server for demo purpose.
This exposes a stateless completion API and doesn't persist the agent's state.

```
# Deploy a shared llama.cpp server
kubectl apply -f examples/llm-chat-demo/manifests/00-namespace.yaml \
              -f examples/llm-chat-demo/manifests/05-llama-completion.yaml
```

## Creating AgentSubstrate WorkerPool

Substrate's `WorkerPool` is a pool of computing capacity which manages standby pods ready to run actors.

See [Substrate's WorkerPool document](https://github.com/agent-substrate/substrate/blob/fda35d0c04e4d55c5ddb9e1ff50ed4e1ceafbf5f/docs/api-guide.md#1-workerpool-the-physical-capacity) for details.

See [`examples/llm-chat-demo/manifests/10-workerpool.yaml`](../examples/llm-chat-demo/manifests/10-workerpool.yaml) for the example manifest.

```
REGISTRY=localhost:5001
export ATEOM_IMAGE=$(cd "$SUBSTRATE_DIR" && KO_DOCKER_REPO="$REGISTRY" ./hack/run-tool.sh ko build ./cmd/ateom-gvisor)
envsubst '${ATEOM_IMAGE}' < examples/llm-chat-demo/manifests/10-workerpool.yaml \
  | kubectl apply -f -
```

## Writing agents

The standard a2a-go SDK can be used.
Our `github.com/ktock/checkpointd/harness` package provides plugins for a2a-go to enable checkpointd-based transport.

See [`./agents.md`](./agents.md) for details.

In this example, two agents are built.
See [`examples/llm-chat-demo/chat-agent/main.go`](../examples/llm-chat-demo/chat-agent/main.go) and [`examples/llm-chat-demo/reviewer-agent/main.go`](../examples/llm-chat-demo/reviewer-agent/main.go) for an example agents implementation.

## Deploying agents

Checkpointd's agent is deployed as an actor on Substrate.
An actor is a sandboxed application managed by Substrate with checkpointing and resumption.
To deploy an actor, an ActorTemplate resource containing its AgentCard in `CHECKPOINTD_AGENT_CARD` needs to be applied to Substrate.

See [Substrate's ActorTemplate document](https://github.com/agent-substrate/substrate/blob/fda35d0c04e4d55c5ddb9e1ff50ed4e1ceafbf5f/docs/api-guide.md#2-actortemplate-the-workload-blueprint) for details.

See [`examples/llm-chat-demo/manifests/20-actortemplate-chat-agent.yaml`](../examples/llm-chat-demo/manifests/20-actortemplate-chat-agent.yaml) and [`examples/llm-chat-demo/manifests/21-actortemplate-reviewer-agent.yaml`](../examples/llm-chat-demo/manifests/21-actortemplate-reviewer-agent.yaml) for the example ActorTemplates.

Supported environment variables:

- `CHECKPOINTD_AGENT`(required): set this to true to enable checkpointd to recognize this agent
- `CHECKPOINTD_AGENT_CARD`(required): the AgentCard in JSON or YAML
- `CHECKPOINTD_AGENT_ID`(optional): override the default agent ID (default: ActorTemplate .metadata.name)
- `CHECKPOINTD_HARNESS_PORT`(optional): Port this agent listens to (default: 80)

In this example, two agents are deployed.

```sh
REGISTRY=localhost:5001

CHAT_AGENT_IMAGE=$REGISTRY/chat-agent:dev
docker build --target chat-agent --metadata-file=/tmp/metadata.json -f examples/llm-chat-demo/agents.Dockerfile -t $CHAT_AGENT_IMAGE .
docker push $CHAT_AGENT_IMAGE
digest=$(jq -r '."containerimage.digest"' /tmp/metadata.json)
export CHAT_AGENT_IMAGE="$CHAT_AGENT_IMAGE@$digest"
envsubst '${CHAT_AGENT_IMAGE}' < examples/llm-chat-demo/manifests/20-actortemplate-chat-agent.yaml \
  | kubectl apply -f -

REVIEWER_AGENT_IMAGE=$REGISTRY/reviewer-agent:dev
docker build --target reviewer-agent --metadata-file=/tmp/metadata.json -f examples/llm-chat-demo/agents.Dockerfile -t $REVIEWER_AGENT_IMAGE .
docker push $REVIEWER_AGENT_IMAGE
digest=$(jq -r '."containerimage.digest"' /tmp/metadata.json)
export REVIEWER_AGENT_IMAGE="$REVIEWER_AGENT_IMAGE@$digest"
envsubst '${REVIEWER_AGENT_IMAGE}' < examples/llm-chat-demo/manifests/21-actortemplate-reviewer-agent.yaml \
  | kubectl apply -f -
```

## Deploying checkpointd

Checkpointd can be deployed as a StatefulSet providing a volume for saving the event log.
Checkpointd needs a ServiceAccount granting `get`, `list` and `watch` of actortemplates.ate.dev so that it can collect AgentCards of agents in the cluster.

- See [`examples/llm-chat-demo/manifests/90-checkpointd-server.yaml`](../examples/llm-chat-demo/manifests/90-checkpointd-server.yaml) for the example manifest (ServiceAccount/Role/RoleBinding/StatefulSet/Service)
- See [`examples/llm-chat-demo/manifests/85-checkpointd-configmap.yaml`](../examples/llm-chat-demo/manifests/85-checkpointd-configmap.yaml) for checkpointd's own config, as a ConfigMap.

Supported environment variable:

- `CHECKPOINTD_KUBERNETES_NAMESPACE`(required when running with `--discover`): Kubernetes namespace checkpointd discovers agents from.

```sh
REGISTRY=localhost:5001
CHECKPOINTD_IMAGE=$REGISTRY/checkpointd:dev
docker build --target checkpointd -f cmd/Dockerfile -t $CHECKPOINTD_IMAGE .
docker push $CHECKPOINTD_IMAGE
export CHECKPOINTD_IMAGE
kubectl apply -f examples/llm-chat-demo/manifests/85-checkpointd-configmap.yaml
envsubst '${CHECKPOINTD_IMAGE}' < examples/llm-chat-demo/manifests/90-checkpointd-server.yaml \
  | kubectl apply -f -
```

## Sending requests to the agent

You can use the standard [`a2a` CLI](https://github.com/a2aproject/a2a-go) to send a request to an agent via checkpointd.

```sh
$ go install github.com/a2aproject/a2a-go/v2/cmd/a2a@v2.5.0
$ kubectl -n checkpointd-llm-chat-demo port-forward svc/checkpointd-server 8080:80 &
$ a2a --transport jsonrpc -o json send http://localhost:8080/agents/chat-agent/ "Hi, I'm Foo."
{
  "id": "01a0a016-644e-7718-a6be-5ae03623da1a",
  "contextId": "",
  "history": [
    {
      "messageId": "01a0a016-3fe4-7f23-8853-5fb43e00fa5a",
      "parts": [
        {
          "text": "Hi, I'm Foo."
        }
      ],
      "role": "ROLE_USER"
    }
  ],
  "metadata": {
    "checkpointd-tenant": "8c98b4eb48334487"
  },
  "status": {
    "message": {
      "messageId": "01a0a016-644e-7618-ada2-819067e16acb",
      "parts": [
        {
          "text": "Assistant: Hello, I'm Foo.\nReviewer: My take: Foo is a great person, but I'd be lying if I said he's a total jerk. He's a bit of a stubborn one, and his stubbornness sometimes gets in the way of his true nature. But as a person, he's just another human being trying to make sense of\n\n--- conversation log ---\nUser: Hi, I'm Foo.\nAssistant: Hello, I'm Foo.\nReviewer: My take: Foo is a great person, but I'd be lying if I said he's a total jerk. He's a bit of a stubborn one, and his stubbornness sometimes gets in the way of his true nature. But as a person, he's just another human being trying to make sense of"
        }
      ],
      "role": "ROLE_AGENT"
    },
    "state": "TASK_STATE_INPUT_REQUIRED"
  }
}
```

For this conversation, task ID `01a0a016-644e-7718-a6be-5ae03623da1a` with a tenant `8c98b4eb48334487` is allocated.
The agent's status is `TASK_STATE_INPUT_REQUIRED` which indicates it can receive further messages for multi-turn conversation.

```
$ a2a --transport jsonrpc -o json send --tenant 8c98b4eb48334487 --task 01a0a016-644e-7718-a6be-5ae03623da1a http://localhost:8080/agents/chat-agent/ "What is your name?"
{
  "id": "01a0a016-644e-7718-a6be-5ae03623da1a",
  "contextId": "",
  "history": [
    {
      "messageId": "01a0a016-3fe4-7f23-8853-5fb43e00fa5a",
      "parts": [
        {
          "text": "Hi, I'm Foo."
        }
      ],
      "role": "ROLE_USER"
    },
    {
      "messageId": "01a0a01e-3ec6-7c28-bc1f-74a0a80aa24d",
      "parts": [
        {
          "text": "What is your name?"
        }
      ],
      "role": "ROLE_USER",
      "taskId": "01a0a016-644e-7718-a6be-5ae03623da1a"
    },
    {
      "messageId": "01a0a016-644e-7618-ada2-819067e16acb",
      "parts": [
        {
          "text": "Assistant: Hello, I'm Foo.\nReviewer: My take: Foo is a great person, but I'd be lying if I said he's a total jerk. He's a bit of a stubborn one, and his stubbornness sometimes gets in the way of his true nature. But as a person, he's just another human being trying to make sense of\n\n--- conversation log ---\nUser: Hi, I'm Foo.\nAssistant: Hello, I'm Foo.\nReviewer: My take: Foo is a great person, but I'd be lying if I said he's a total jerk. He's a bit of a stubborn one, and his stubbornness sometimes gets in the way of his true nature. But as a person, he's just another human being trying to make sense of"
        }
      ],
      "role": "ROLE_AGENT"
    }
  ],
  "metadata": {
    "checkpointd-tenant": "8c98b4eb48334487"
  },
  "status": {
    "message": {
      "messageId": "01a0a01e-5329-78a6-9eb2-30a492436474",
      "parts": [
        {
          "text": "Assistant: My name is Foo.\nReviewer: My take: Foo is a great person, but I'd be lying if I said he's a total jerk. He's a bit of a stubborn one, and his stubbornness sometimes gets in the way of his true nature. But as a person, he's just another human being trying to make sense of\n\n--- conversation log ---\nUser: Hi, I'm Foo.\nAssistant: Hello, I'm Foo.\nReviewer: My take: Foo is a great person, but I'd be lying if I said he's a total jerk. He's a bit of a stubborn one, and his stubbornness sometimes gets in the way of his true nature. But as a person, he's just another human being trying to make sense of\n------------------------\nUser: What is your name?\nAssistant: My name is Foo.\nReviewer: My take: Foo is a great person, but I'd be lying if I said he's a total jerk. He's a bit of a stubborn one, and his stubbornness sometimes gets in the way of his true nature. But as a person, he's just another human being trying to make sense of"
        }
      ],
      "role": "ROLE_AGENT"
    },
    "state": "TASK_STATE_INPUT_REQUIRED"
  }
}
```

You can also inspect the task started by this conversation.

```
$ a2a --transport jsonrpc list tasks http://localhost:8080/agents/chat-agent/
ID                                    STATUS          CONTEXT
01a0a016-644e-7718-a6be-5ae03623da1a  input-required  
```

AgentCard is also available on checkpointd.

```
$ a2a discover http://localhost:8080/agents/chat-agent/.well-known/agent-card.json
Name:         Chat Agent
Description:  Stateful chat agent backed by a local llama.cpp completion server -- answers the user, asks reviewer-agent for a second opinion, and remembers the whole conversation across turns and crashes. See examples/llm-chat-demo
Version:      1.0.0
Interfaces:
  checkpointd  checkpointd:chat-agent
  JSONRPC      http://checkpointd-server.checkpointd-llm-chat-demo.svc:80/agents/chat-agent/
Streaming:    false
```

## Cleanup

In the substrate repo, the KinD cluster can be torn down as the following:

```
(cd "$SUBSTRATE_DIR" && hack/delete-kind-cluster.sh)
```
