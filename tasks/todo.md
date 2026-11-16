# Plan: DistributedActorSystem redesign (design doc 020_DistributedActorSystemRedesign_261115_oo01)

Rules: build and test on W206 in a k8s pod (never on 5.12); `rm -rf target` before each build;
never `-DskipTests`; write the failing test first; commit at each green step; ask before the first push.
Decided: remote calls go through the target actor's mailbox; a `send` receipt means "accepted into the mailbox".

- [x] Step 0: W206 build pod works (`tasks/w206-build.sh`), baseline `mvn test` is green
- [x] Step 1: shared `Dispatcher` (allow-list check, then run through the target actor's mailbox)
- [x] Step 2: `HttpActorServer` uses the `Dispatcher`
- [x] Step 3: `RemoteActorRef.send` returns a receipt
- [ ] Step 4: Kafka path (`KafkaActorServer` uses the `Dispatcher`, explicit success field, acks=all, manual commit after mailbox accept)
- [ ] Step 5: peer list `NodeDiscovery` (configured node list)
- [ ] Step 6: `messageId` dedup store (replaceable, survives restart)
- [ ] Step 7: sender resend with the same `messageId`; `callByActionName(name, args, timeout)`
