# Changelog

## 5.0.0

Changes since `v1.1.0`. pojo-actor-distributed now takes the same version as POJO-actor, Turing-workflow and Turing-workflow-plugins, and needs POJO-actor 5.0.0 and Turing-workflow 5.0.0.

### Nodes on other hosts call each other over mutual TLS; a call carries who sent it

Commits `a3a9c32`, `9cab75d`.

- The program builds an `SSLContext` from the keys and certificates its platform gives it (a SPIFFE workload API, cert-manager or key store files) and passes it to `DistributedIIActorSystem.Builder.sslContext`. The system then sends over TLS, and its HTTP server requires a certificate from every caller. The library does not issue, distribute or rotate certificates.
- Listening on an address other than `127.0.0.1` without an `SSLContext` fails at start-up.
- After the handshake the server reads the caller's identity from its certificate: the SPIFFE ID (a `spiffe://` URI in the subject alternative name), otherwise the common name (`CallerIdentity`, `PeerIdentity`). `Builder.callPolicy` with `CallPolicy.perCaller` lets each identity call only the actions listed for it, inside the system's allow-list.

**What 1.1 code must change:** a node that listened on an address other than `127.0.0.1` passes an `SSLContext`, and so do the nodes that call it.

### The HTTP server listens on 127.0.0.1 by default and serves only what a node sends

Commits `350c29d`, `eb961c5`, `091de7c`.

- `HttpActorServer` listens on `127.0.0.1` unless the program names another address, and says so when it does.
- It accepts only a JSON body with no `Origin` header, and on loopback only a loopback `Host`.
- It reads at most 1 MiB of a request and serves at most 256 requests at once; `KafkaActorServer` has the same limits.

### Messages are not lost or run twice when the message-id store fails

Commits `57b1890` and the receiver changes before it.

- A message id is remembered with the message's content, and a full store refuses new messages instead of forgetting old ones.
- While the message-id store cannot be read or cannot record, the receivers stop taking messages and give no receipt for calls that can never run.

### Kafka: the program's own client settings reach the client

Commit `8ac6de1`. `security.protocol`, `ssl.*` and `sasl.*` set by the program reach the Kafka client, so the Kafka path can run over TLS or SASL.

### No real thread outside the `ManagedThreadPool`

Commit `b2d9563`. `HttpTransport` builds its `HttpClient` with a virtual-thread executor; without one the JDK made a pool of real threads of its own. A guard test fails on any real thread made outside the `ManagedThreadPool`, as in the other three repositories.

### Dependencies

`kafka-clients` 3.9.2 with the `at.yawk` fork of `lz4-java` 1.14.1 and `snappy-java` 1.1.10.12; Jackson 2.21.7 and `org.json` 20260719, which fix the advisories found in the 5.0.0 security review; `bcpkix` raised.
