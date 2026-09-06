# Distributed actors — DistributedActorSystem

Build an actor system that spans multiple nodes in an HPC cluster or on Kubernetes.

---

## Prerequisites

Choose a transport for inter-node communication.

| Transport | Suitable environment | Dependency |
|-----------|---------------------|------------|
| `HttpTransport` | HPC clusters (Slurm, Grid Engine, etc.) | Standard (no extra dependency) |
| `KafkaTransport` | Kubernetes | `kafka-clients:3.7.0` (optional) |

---

## Node discovery

```java
// Auto-detect the runtime environment (Slurm / K8s / GridEngine)
NodeDiscovery discovery = NodeDiscoveryFactory.autoDetect();
```

| Implementation class | Detection method |
|----------------------|-----------------|
| `SlurmNodeDiscovery` | Environment variable `SLURM_JOB_NODELIST` |
| `K8sNodeDiscovery` | `POD_NAME` + `KUBERNETES_SERVICE_HOST` |
| `GridEngineNodeDiscovery` | `PE_HOSTFILE` |

---

## HTTP transport (for HPC clusters)

```java
NodeDiscovery discovery = NodeDiscoveryFactory.autoDetect();

DistributedActorSystem dist = DistributedActorSystem.builder()
    .localActorSystem(new ActorSystem("node"))
    .transport(new HttpTransport())
    .discovery(discovery)
    .build();

dist.startHttpServer(8080);

// Call a remote actor
RemoteActorRef remote = dist.remoteActorOf(dist.getNodes().get(1), "my-actor");
ActionResult result = remote.callByActionName("process", payload);

dist.close();
```

---

## Kafka transport (for Kubernetes)

```java
NodeDiscovery discovery = NodeDiscoveryFactory.autoDetect();

KafkaTransport transport = new KafkaTransport(
    new NodeInfo(discovery.getMyNodeId(), discovery.getMyHost(), discovery.getMyPort()),
    "kafka:9092"
);

DistributedActorSystem dist = DistributedActorSystem.builder()
    .localActorSystem(new ActorSystem("node"))
    .transport(transport)
    .discovery(discovery)
    .build();

dist.startServer("kafka:9092");

RemoteActorRef remote = dist.remoteActorOf(dist.getNodes().get(1), "my-actor");
ActionResult result = remote.callByActionName("process", payload);

dist.close();
```

---

## CallableByActionName — actors that accept remote calls

Required on the receiving side of `RemoteActorRef.callByActionName()`.
Not needed if you only use local `ActorRef.tell/ask`.

```java
public class MyActor implements CallableByActionName {
    @Override
    public ActionResult callByActionName(String actionName, String args) {
        return switch (actionName) {
            case "process" -> new ActionResult(true, doProcess(args));
            default        -> new ActionResult(false, "Unknown: " + actionName);
        };
    }
}
```

`ActionResult` is a pair of `(boolean success, String result)`.

---

## Key classes

| Class | Package | Role |
|-------|---------|------|
| `DistributedActorSystem` | `core.distributed` | Adds distributed capability to a local ActorSystem |
| `RemoteActorRef` | `core.distributed` | Reference to an actor on a remote node |
| `ActorMessage` | `core.distributed` | Data class for distributed messages |
| `NodeInfo` | `core.distributed` | Node information (nodeId, host, port) |
| `HttpActorServer` | `core.distributed` | HTTP message receiver server |
| `KafkaActorServer` | `core.distributed` | Kafka message receiver server |
| `NodeDiscoveryFactory` | `core.distributed.discovery` | Factory for auto-detecting the environment |
| `HttpTransport` | `core.distributed.transport` | HTTP transport implementation |
| `KafkaTransport` | `core.distributed.transport` | Kafka transport implementation |
