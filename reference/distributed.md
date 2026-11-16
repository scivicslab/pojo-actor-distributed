# Distributed actors — DistributedIIActorSystem

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
| `K8sNodeDiscovery` | `POD_NAME` + `SERVICE_NAME` + `KUBERNETES_SERVICE_HOST` (`REPLICAS` lists the peers) |
| `StaticNodeDiscovery` | Never detected: you give the node list |
| `GridEngineNodeDiscovery` | `PE_HOSTFILE` |

---

## HTTP transport (for HPC clusters)

```java
NodeDiscovery discovery = NodeDiscoveryFactory.autoDetect();

DistributedIIActorSystem dist = DistributedIIActorSystem.builder()
    .transport(new HttpTransport())
    .discovery(discovery)
    .allowedActions(Set.of(new AllowListDispatcher.AllowedAction("my-actor", "process")))
    .noDeduplication()           // or .deduplicateWith(store); one of the two is required
    .build();

dist.startHttpServer(8080);

// Call a remote actor
RemoteActorRef remote = dist.remoteActorOf(dist.getNodes().get(1), "my-actor");
ActionResult result = remote.callByActionName("process", payload);

dist.close();
```

Nothing can be called from outside until it is on `allowedActions`. `GET /health` answers 200
with no body.

---

## Kafka transport (for Kubernetes)

```java
NodeDiscovery discovery = NodeDiscoveryFactory.autoDetect();

KafkaTransport transport = new KafkaTransport(
    new NodeInfo(discovery.getMyNodeId(), discovery.getMyHost(), discovery.getMyPort()),
    "kafka:9092"
);

DistributedIIActorSystem dist = DistributedIIActorSystem.builder()
    .transport(transport)
    .discovery(discovery)
    .allowedActions(Set.of(new AllowListDispatcher.AllowedAction("my-actor", "process")))
    .deduplicateWith(store)      // Kafka can deliver a message twice
    .build();

dist.startServer("kafka:9092");

RemoteActorRef remote = dist.remoteActorOf(dist.getNodes().get(1), "my-actor");
Receipt receipt = remote.send("process", payload);   // the broker's acknowledgement

dist.close();
```

Kafka has no synchronous call. `sendAndWait` returns a failure; send the request with `send`,
and have the actor send its answer back as a new message.

---

## Sending without waiting

```java
Receipt receipt = remote.send("longJob", payload);                       // one try
Receipt sure = remote.sendWithRetry("longJob", payload, 5, Duration.ofSeconds(1));  // same messageId each try
ActionResult quick = remote.callByActionName("ping", "", Duration.ofSeconds(2));
```

`Receipt.accepted()` means the remote mailbox (HTTP) or the broker's inbox (Kafka) has the
message, not that the action ran. With `deduplicateWith(store)` on the receiver, however many
copies arrive, the action runs once.

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

All classes are in `com.scivicslab.pojoactor.distributed` unless a sub-package is shown.

| Class | Role |
|-------|------|
| `DistributedIIActorSystem` | The distributed actor system: an `IIActorSystem` that can call and be called across processes |
| `RemoteActorRef` | Reference to an actor on a remote node (`callByActionName`, `send`, `sendWithRetry`) |
| `Receipt` | The answer to `send`: accepted or not, with the `messageId` |
| `ActorMessage` | Data class for distributed messages |
| `NodeInfo` | Node information (nodeId, host, port) |
| `Dispatcher`, `AllowListDispatcher` | Checks the allow-list and runs a call through the actor's mailbox |
| `DedupDispatcher`, `MessageIdStore`, `InMemoryMessageIdStore`, `FileMessageIdStore` | Drop the second copy of a message |
| `HttpActorServer` | HTTP receiver (`/actor/{name}/invoke`, `/actor/{name}/send`, `/health`) |
| `KafkaActorServer` | Kafka receiver |
| `discovery.NodeDiscoveryFactory`, `discovery.StaticNodeDiscovery` | Environment detection, or a configured node list |
| `transport.HttpTransport`, `transport.KafkaTransport` | Transport implementations |
