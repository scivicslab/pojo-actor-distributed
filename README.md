# pojo-actor-distributed

[![Maven Central](https://img.shields.io/maven-central/v/com.scivicslab/pojo-actor-distributed.svg)](https://central.sonatype.com/artifact/com.scivicslab/pojo-actor-distributed)
[![Java Version](https://img.shields.io/badge/java-21+-blue.svg)](https://openjdk.java.net/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Calling an actor that lives in another process.

An actor system built with [POJO-actor](https://github.com/scivicslab/POJO-actor) reaches only
the actors inside its own JVM. This library publishes an actor system over HTTP and hands out a
reference that looks like a local actor but sends the call to another process. A workflow step
written for [Turing-workflow](https://github.com/scivicslab/Turing-workflow) can then name an
actor that runs on a different machine.

It holds everything that needs both POJO-actor's `ActorSystem` and Turing-workflow's
`IIActorSystem`, so that neither of those two has to know about the other.

## Requirements

- Java 21 or higher
- POJO-actor 4.0.0
- Turing-workflow 4.0.0

## Installation

```xml
<dependency>
    <groupId>com.scivicslab</groupId>
    <artifactId>pojo-actor-distributed</artifactId>
    <version>1.0.0</version>
</dependency>
```

## The process that owns the actor

`DistributedIIActorSystem` starts an HTTP server on the port you give it and answers
`POST /actor/{name}/invoke` for every actor registered with `addIIActor`.

```java
DistributedIIActorSystem system =
        new DistributedIIActorSystem("worker", "0.0.0.0", 8081);
system.addIIActor(new Calculator("calculator", system));
Thread.currentThread().join();
```

## The process that calls it

Name the other node once, then ask for an actor on it. `RemoteActorRef` sends
`callByActionName` over HTTP and returns the `ActionResult` the other side produced, including
a failure the action reported itself.

```java
DistributedIIActorSystem system =
        new DistributedIIActorSystem("driver", "0.0.0.0", 8082);
system.registerRemoteNode("worker", "worker.example.com", 8081);

RemoteActorRef calculator = system.getRemoteActor("worker", "calculator");
ActionResult result = calculator.callByActionName("add", "[\"10\"]");
```

## Driving a remote actor from a workflow

An interpreter resolves a step's `actor:` to an `IIActorRef`, and `RemoteActorRef` is not one.
`RemoteActorIIAR` wraps the remote reference so the step can name it like any local actor.

```java
system.addIIActor(new RemoteActorIIAR("calculator", calculator, system));
```

```yaml
name: remote-calculation
steps:
  - states: ["0", "1"]
    actions:
      - actor: calculator
        method: add
        arguments: "7"
```

## Finding the other nodes

On a cluster you rarely know the addresses in advance. `NodeDiscoveryFactory.autoDetect()`
reads the environment the scheduler sets up and returns the list of peers:
`SLURM_JOB_NODELIST` on Slurm, `POD_NAME` and `KUBERNETES_SERVICE_HOST` on Kubernetes,
`PE_HOSTFILE` on Grid Engine.

```java
NodeDiscovery discovery = NodeDiscoveryFactory.autoDetect();
List<NodeInfo> nodes = discovery.getAllNodes();
```

## Transports

HTTP is the default and needs nothing else. `KafkaTransport` and `KafkaActorServer` carry the
same messages over Kafka instead; `kafka-clients` is an optional dependency, so an HTTP-only
deployment does not pull it in.

## References

- **Javadoc**: [API Reference](https://javadoc.io/doc/com.scivicslab/pojo-actor-distributed/1.0.0)
- **POJO-actor**: [GitHub](https://github.com/scivicslab/POJO-actor)
- **Turing-workflow**: [GitHub](https://github.com/scivicslab/Turing-workflow)

## License

Apache License 2.0. See [LICENSE](LICENSE).
