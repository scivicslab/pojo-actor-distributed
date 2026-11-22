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
    <version>1.1.0</version>
</dependency>
```

## Quick start

One class, `DistributedIIActorSystem`, is both the actor system and the way to reach other
processes. It opens no port by itself: until `startHttpServer` is called nothing outside can
reach it, and even then only what the allow-list names can be called.

### The process that owns the actor

```java
NodeInfo me = new NodeInfo("worker", "127.0.0.1", 28081);

DistributedIIActorSystem system = DistributedIIActorSystem.builder()
        .discovery(new StaticNodeDiscovery(me, List.of()))
        // Only these (actor, action) pairs can be called from outside. Nothing is allowed by default.
        .allowedActions(Set.of(new AllowListDispatcher.AllowedAction("calculator", "add")))
        // You must choose how duplicate messages are handled; build() refuses otherwise.
        .deduplicateWith(new FileMessageIdStore(Path.of("seen-ids.log"), 10_000, Duration.ofHours(1)))
        .build();

system.addIIActor(new Calculator("calculator", system));
system.startHttpServer("127.0.0.1", 28081);   // 127.0.0.1: only this machine can call in
```

- A call from outside runs through the target actor's mailbox, so the actor still handles one
  message at a time. A call that is not on the allow-list, or that names an actor that does not
  exist, is refused with the same `Not allowed: actor/action` answer (HTTP 403).
- `GET /health` answers 200 with no body and needs no allow-list entry.
- `deduplicateWith(store)` drops the second and later copies of a message with the same
  `messageId`. The store remembers at most the count and the age you give; both are required.
  `FileMessageIdStore` survives a restart, `InMemoryMessageIdStore` does not.
  `noDeduplication()` is the other choice: **every duplicate will run**, so choose it only when
  the actions are safe to repeat.

### The process that calls it

```java
NodeInfo me = new NodeInfo("driver", "127.0.0.1", 28082);
NodeInfo worker = new NodeInfo("worker", "127.0.0.1", 28081);

DistributedIIActorSystem system = DistributedIIActorSystem.builder()
        .discovery(new StaticNodeDiscovery(me, List.of(worker)))   // peers are registered for you
        .callTimeout(Duration.ofSeconds(10))                       // wait of callByActionName; default 30 s
        .noDeduplication()                                         // this process receives nothing
        .build();

RemoteActorRef calculator = system.getRemoteActor("worker", "calculator");

// A short call: wait for the result.
ActionResult result = calculator.callByActionName("add", "[\"10\"]");
ActionResult quick  = calculator.callByActionName("add", "[\"10\"]", Duration.ofSeconds(2));

// A long job: send it, get a receipt, and let the actor send its answer back as another message.
Receipt receipt = calculator.send("longJob", "{}");
if (!receipt.accepted()) { /* not taken: send again with the same messageId */ }

// Resend with the same messageId until a receipt comes. You give the tries and the wait.
Receipt sure = calculator.sendWithRetry("longJob", "{}", 5, Duration.ofSeconds(1));
```

A `Receipt` says the remote side accepted the message into the actor's mailbox (HTTP) or the
broker stored it in the inbox (Kafka). It does not say the action ran or succeeded. The module
never resends on its own; `sendWithRetry` is the caller asking for it, and a receiver that
deduplicates runs the action once however many copies arrive.

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
`SLURM_JOB_NODELIST` on Slurm, `POD_NAME`, `SERVICE_NAME` and `KUBERNETES_SERVICE_HOST` on
Kubernetes (`REPLICAS` gives the number of peers), `PE_HOSTFILE` on Grid Engine.

```java
NodeDiscovery discovery = NodeDiscoveryFactory.autoDetect();
List<NodeInfo> nodes = discovery.getAllNodes();
```

On a machine that is none of these, such as several instances on one workstation, list the
nodes yourself. `StaticNodeDiscovery` is never chosen by `autoDetect()`.

```java
NodeDiscovery discovery = new StaticNodeDiscovery(me,
        StaticNodeDiscovery.parsePeers("b=127.0.0.1:28002, c=10.0.0.3:28003"));
```

## Transports

HTTP is the default and needs nothing else. `KafkaTransport` and `KafkaActorServer` carry the
same messages over Kafka instead; `kafka-clients` is an optional dependency, so an HTTP-only
deployment does not pull it in. `system.startServer("kafka:9092")` starts the Kafka receiver,
which hands messages to the same allow-list and mailbox as HTTP and records its position only
after the actor's mailbox has taken the message.

Kafka has no synchronous call: `sendAndWait` is not offered. A request is sent with `send`, and
the actor that finishes the work sends the answer back as a new message to the sender's inbox.

A `send` whose receipt is not accepted may still be delivered: when the broker does not answer
within 30 s the receipt says not accepted, but the producer keeps the message and delivers it once
the broker is back. Send again with the same `messageId`, and let the receiver drop duplicates
(`deduplicateWith(store)`).

### Testing the Kafka path

`mvn test` runs the unit tests, which use Kafka's `MockConsumer` and `MockProducer`. The
integration tests (`*IT.java`) run against a real broker on the W206 k8s cluster:

```bash
tasks/kafka-it.sh
```

The script creates the namespace `pad-kafka-it` with a single-node Kafka (KRaft) and a build pod,
runs `mvn verify -Dgpg.skip=true -Dkafka.it.bootstrap=<broker>` in the pod, copies the reports to
`target/kafka-it-reports/`, and deletes the namespace. The property `kafka.it.bootstrap` turns on
the Maven profile `kafka-it`; without it, `mvn verify` and `mvn install` run the unit tests only.

They check, against the real broker: a `send` is accepted and runs once, and an answer comes back
as a `send` to the sender's inbox (`sendAndWait` is refused); a stopped receiver resumes from its
committed position; the inbox pauses while the message-id store fails and resumes by itself; the
receiver goes on after the broker pod is deleted and comes back; a redelivered message runs once
with a `FileMessageIdStore` across a restart. Not tested: inboxes with more than one partition
(and so message order across partitions), a cluster of several brokers, and TLS or authentication.

## References

- **Javadoc**: [API Reference](https://javadoc.io/doc/com.scivicslab/pojo-actor-distributed/1.1.0)
- **POJO-actor**: [GitHub](https://github.com/scivicslab/POJO-actor)
- **Turing-workflow**: [GitHub](https://github.com/scivicslab/Turing-workflow)

## License

Apache License 2.0. See [LICENSE](LICENSE).
