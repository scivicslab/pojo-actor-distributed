# Plan: DistributedActorSystem redesign (design doc 020_DistributedActorSystemRedesign_261115_oo01)

Rules: build and test on W206 in a k8s pod (never on 5.12); `rm -rf target` before each build;
never `-DskipTests`; write the failing test first; commit at each green step; ask before the first push.
Decided: remote calls go through the target actor's mailbox; a `send` receipt means "accepted into the mailbox".

- [x] Step 0: W206 build pod works (`tasks/w206-build.sh`), baseline `mvn test` is green
- [x] Step 1: shared `Dispatcher` (allow-list check, then run through the target actor's mailbox)
- [x] Step 2: `HttpActorServer` uses the `Dispatcher`
- [x] Step 3: `RemoteActorRef.send` returns a receipt
- [x] Step 4: Kafka path (`KafkaActorServer` uses the `Dispatcher`, explicit success field, acks=all, manual commit after mailbox accept)
- [x] Step 5: `NodeDiscovery` from a configured peer list (`StaticNodeDiscovery`)
- [x] Step 6: merge `DistributedIIActorSystem` onto the shared `Dispatcher` and the transports: remove its own HTTP server and its call path that skips the allow-list. Merge before the reliability layers, so that dedup and resend are written once, on the merged system
- [x] Step 7: `messageId` dedup with a replaceable, persistent store (drops the second copy of a message; HTTP and Kafka alike)
- [x] Step 8: sender resend with the same `messageId`; `callByActionName(name, args, timeout)`

Order decided by the person via the liaison, 2026-11-16: 5, 6 (merge), 7, 8. `Turing-workflow-editor` stays pinned
to 1.0.0 and is not touched; compatibility with it is not kept (decided earlier by the person). The version number
is the person's call: the `pom.xml` version is left alone. Push is on hold.

## Dependent projects (surveyed 2026-11-16 under ~/works; nothing changed in them)

Searched every `pom.xml` and every `*.java` under `~/works` (not `target`, `node_modules`, `retired`) for
`pojo-actor-distributed`, `HttpActorServer`, `DistributedActorSystem`, `DistributedIIActorSystem`,
`TransportLayer`, `RemoteActorRef`.

| Project | How it uses the module | What breaks after Steps 1-3 |
|---|---|---|
| `Turing-workflow-editor` (pom pins `pojo-actor-distributed` **1.0.0**) | `WorkflowRunner`: `new RemoteActorRef(name, node)` wrapped in `RemoteActorIIAR`; `ChildNodeRegistry`: `NodeInfo`, `DistributedActorSystem.publicationPortFor(int)`; tests use `RemoteActorIIAR`, `ChildNodeRegistry` | Nothing at compile time: it uses only the `RemoteActorRef` 2-arg constructor, `callByActionName`, `NodeInfo`, `RemoteActorIIAR`, `publicationPortFor`, none of which changed. It pins 1.0.0, so it keeps the old jar until someone changes the version. It never starts a server and never calls `send`, so the removed `HttpActorServer(ActorSystem, ...)` constructors and the `TransportLayer.send` return type do not touch it. Runtime risk after an upgrade: the editor calls children over `/invoke`; whatever process hosts the children's `HttpActorServer` gets the 403 default and must pass an allow-list. Where that server is started is not in this tree. |
| `Turing-workflow` | Only a comment in `IIActorSystemLookupTest` mentions `HttpActorServer`. It does not depend on the module (the module depends on it) | Nothing. |
| `POJO-actor`, `Turing-workflow` READMEs and `SKILL.md` | Link to the module | Nothing, but the README/SKILL text about `HttpActorServer` and `send` is stale after Step 2-3 (documentation only). |

No other project under `~/works` references the module. In particular `chat-ui-with-audit-trail`,
`quarkus-AI-workspace`, `k8s-pups` and `Turing-workflow-plugins` do not import `pojoactor.distributed`.

What changed that could break a future user: (1) `HttpActorServer` takes a `Dispatcher`, the `ActorSystem`
constructors are gone; (2) `DistributedActorSystem.startHttpServer` serves nothing until
`Builder.allowedActions(...)` lists (actor, action) pairs, so every call is 403; (3) `TransportLayer.send`
and `RemoteActorRef.send` return `Receipt` instead of `void`; (4) a refused `/invoke` is HTTP 403 (it was 200).
Version bump to 1.1.0 is the person's decision (see CLAUDE.md: keep the three repos in sync).

## After Step 8 (left for later, per the person)
- [x] One docs commit: stale `DistributedActorSystem` text in `reference/distributed.md`, the `NodeDiscovery*` javadoc examples, README, and the design doc's discrepancy list.

## Error-handling findings 7 and 8 (chat-81, 2026-11-22)
Build pod: own pod `mvn-c81` on ubuntu:24.04 + GraalVM CE 25 (no GraalVM CE image in the W206 registry).
Tests carry `@Tag("DistributedActorSystemRedesign_261115_oo01")`. One commit per finding; no push; version stays 1.2.0-SNAPSHOT.

- [x] `tasks/w206-build.sh` takes the pod name and image (`POD`, `IMAGE`); a pod without `mvn` gets GraalVM CE and Maven installed. Own commit.
- [x] Baseline: full `mvn test` green in `mvn-c81`
- [x] Finding 7: `KafkaActorServer.handOver` treats `fromJson("null")` (returns null) as unparseable: logged, skipped, position recorded. Failing test first (a `null` record then a valid one: the valid one is dispatched, the commit covers both)
- [x] Finding 8a: `AllowListDispatcher` refuses an allowed actor whose message loop has died (`!ref.isAlive()`), "Not accepted: actor not running"; allow-list check stays first. Failing test first (action throws `StackOverflowError`). `runInMailbox`'s `catch (Exception e)` is not changed (the person's decision)
- [x] Finding 8b: `KafkaActorServer.consumeLoop` guards `subscribe` (retried next turn) and catches `Throwable` around the loop body (SEVERE); a `VirtualMachineError` other than `StackOverflowError` is logged and rethrown. Failing tests first (poll throws `StackOverflowError` once; subscribe throws once)
- [x] Full `mvn test` green after `rm -rf target`; commit 7, commit 8

## Audit item 18: the message-id store cannot record (chat-83, 2026-11-22)
Decided by the person (option A): while the store cannot record, the receiver stops taking messages instead of
re-running the same action. Build pod `mvn-c83` (ubuntu:24.04 + GraalVM CE 25). No push; version stays 1.2.0-SNAPSHOT.

- [x] Baseline: full `mvn test` green in `mvn-c83` (133 run, 0 failures, 0 errors, 0 skipped)
- [x] Failing tests first (140 run, 2 failures, 4 errors; only `recover_withNothingPending_isTrue` passed): (1) HTTP 503 while the store is down, the first message ran once; (2) Kafka pauses, no
      second run, resumes after recovery and commits; (3) `FileMessageIdStore` works again after a failed `compact()`;
      plus `DedupDispatcher` unit tests for the pending id and the backoff (1 s, doubling, 30 s cap)
- [x] `Dispatcher.Dispatch` gets a third outcome, UNAVAILABLE, besides accepted and refused; `Dispatcher.recover()`
      (default: available)
- [x] `DedupDispatcher`: a failed `seen.add` keeps the accepted result, logs SEVERE, keeps the id pending; while
      pending, each call first probes `seen.add` (no more often than the backoff) and otherwise answers UNAVAILABLE
- [x] `HttpActorServer`: UNAVAILABLE answers 503 `message id store unavailable`, not dispatched
- [x] `KafkaActorServer`: UNAVAILABLE seeks back, records nothing from that message on, pauses the assignment; each
      turn probes `recover()` and resumes when it is available. Javadoc: tested only against MockConsumer
- [x] `FileMessageIdStore`: a failed `compact()` leaves no closed channel; the next `add` reopens the file
- [x] Full `mvn test` green after `rm -rf target` (140 run, 0 failures, 0 errors, 0 skipped); commit (store), commit (receivers)
- [x] Follow-up (chat-57's decision): a `seen.contains` that throws is treated like a failed add: UNAVAILABLE
      (HTTP 503, Kafka pauses and probes), nothing dispatched, same backoff; no pending id, the next due try
      reads the store again. Failing tests first (one per receiver and one for the dispatcher)

### Follow-up, not in this job
- [ ] `HttpTransport.parseResponse` (used by `sendAndWait`) should report an answer that is not JSON, or not
      2xx, as `HTTP <status>: <body>`; today a 503 `message id store unavailable` reads as
      `Failed to parse response`. The sender is not changed in audit item 18 (chat-57's decision).
