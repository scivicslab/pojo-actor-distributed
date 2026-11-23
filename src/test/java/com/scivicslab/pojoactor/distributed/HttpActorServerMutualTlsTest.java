package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.action.CallableByActionName;
import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.distributed.AllowListDispatcher.AllowedAction;
import com.scivicslab.pojoactor.distributed.TestCertificates.Issued;
import com.scivicslab.pojoactor.distributed.transport.HttpTransport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Mutual TLS on the HTTP receiver. The certificate authority and every certificate are made inside
 * the test and thrown away after it.
 *
 * <p>What is asserted: a caller with a certificate the server trusts is served; a caller with no
 * certificate, or with one from another authority, never reaches the dispatcher; a caller the server
 * trusts but whose identity is not on the policy is refused; and a server asked to listen anywhere but
 * the loopback address without an {@code SSLContext} does not start. Each refusal is checked against a
 * counter in the actor, so "refused" means the action did not run, not only that the answer said no.</p>
 */
@Tag("MutualTlsForActorCalls_261123_oo01")
@DisplayName("HttpActorServer — mutual TLS and who may call what")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class HttpActorServerMutualTlsTest {

    private static final String WORKER_ID = "spiffe://test/ns/app/sa/worker";
    private static final String INTRUDER_ID = "spiffe://test/ns/app/sa/intruder";

    private static Issued authority;
    private static Issued otherAuthority;
    private static SSLContext serverContext;
    private static SSLContext worker;           // SPIFFE ID WORKER_ID, issued by the trusted authority
    private static SSLContext intruder;         // valid certificate, SPIFFE ID not on the policy
    private static SSLContext legacy;           // valid certificate with a common name and no SPIFFE ID
    private static SSLContext spoofer;          // common name is the text of WORKER_ID, no SPIFFE ID
    private static SSLContext noCertificate;    // trusts the server, presents nothing
    private static SSLContext foreign;          // WORKER_ID, issued by another authority
    private static SSLContext distrustful;      // has a valid certificate but trusts another authority only

    /** Counts every run, so that a refused call can be shown not to have run. */
    static final class CountingActor implements CallableByActionName {
        final AtomicInteger runs = new AtomicInteger();

        @Override
        public ActionResult callByActionName(String action, String args) {
            runs.incrementAndGet();
            return switch (action) {
                case "add" -> {
                    String[] parts = args.split(",");
                    yield new ActionResult(true, String.valueOf(
                            Integer.parseInt(parts[0].trim()) + Integer.parseInt(parts[1].trim())));
                }
                case "echo" -> new ActionResult(true, args);
                default -> new ActionResult(false, "Unknown action: " + action);
            };
        }
    }

    private ActorSystem system;
    private CountingActor actor;
    private HttpActorServer server;
    private int port;

    @BeforeAll
    static void makeCertificates() throws Exception {
        authority = TestCertificates.newAuthority("test-authority");
        otherAuthority = TestCertificates.newAuthority("other-authority");
        Issued serverCertificate = TestCertificates.issueOnLoopback(authority, "CN=node-a",
                List.of("spiffe://test/ns/app/sa/node-a"));
        serverContext = TestCertificates.sslContext(serverCertificate, authority);
        worker = TestCertificates.sslContext(
                TestCertificates.issue(authority, "CN=worker-cn", List.of(WORKER_ID), List.of(), List.of()), authority);
        intruder = TestCertificates.sslContext(
                TestCertificates.issue(authority, "CN=intruder-cn", List.of(INTRUDER_ID), List.of(), List.of()), authority);
        legacy = TestCertificates.sslContext(
                TestCertificates.issue(authority, "CN=legacy-client", List.of(), List.of(), List.of()), authority);
        spoofer = TestCertificates.sslContext(
                TestCertificates.issue(authority, "CN=" + WORKER_ID, List.of(), List.of(), List.of()), authority);
        noCertificate = TestCertificates.sslContext(null, authority);
        foreign = TestCertificates.sslContext(
                TestCertificates.issue(otherAuthority, "CN=worker-cn", List.of(WORKER_ID), List.of(), List.of()), authority);
        distrustful = TestCertificates.sslContext(
                TestCertificates.issue(authority, "CN=worker-cn", List.of(WORKER_ID), List.of(), List.of()), otherAuthority);
    }

    @BeforeEach
    void startServer() throws Exception {
        system = new ActorSystem("mutual-tls");
        actor = new CountingActor();
        system.actorOf("calc", actor);
        // the worker may add; the legacy client (common name only) may echo; the intruder may do nothing
        CallPolicy policy = CallPolicy.perCaller(Map.of(
                CallerIdentity.spiffeId(WORKER_ID), Set.of(new AllowedAction("calc", "add")),
                CallerIdentity.commonName("legacy-client"), Set.of(new AllowedAction("calc", "echo"))));
        port = freePort();
        server = new HttpActorServer(new AllowListDispatcher(system, policy), "127.0.0.1", port, serverContext);
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.close();
        system.terminate();
    }

    // -------------------------------------------------------------------- (a) a call with a trusted certificate

    @Test
    @DisplayName("(a) a caller with a certificate the server trusts, and a call its identity may make, is served")
    void aTrustedCallerIsServed() throws Exception {
        HttpResponse<String> response = call(worker, "https://127.0.0.1:" + port, "add", "10,32");

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"result\":\"42\""), response.body());
        assertEquals(1, actor.runs.get());
    }

    @Test
    @DisplayName("(a) HttpTransport with an SSLContext calls over TLS: sendAndWait answers, send is accepted")
    void theTransportCallsOverTls() {
        NodeInfo node = new NodeInfo("node-a", "127.0.0.1", port);
        try (HttpTransport transport = new HttpTransport(worker)) {
            ActionResult answer = transport.sendAndWait(node, new ActorMessage("calc", "add", "1,2"), Duration.ofSeconds(10));
            assertTrue(answer.isSuccess(), answer.getResult());
            assertEquals("3", answer.getResult());
            assertTrue(transport.send(node, new ActorMessage("calc", "add", "4,5")).accepted());
        }
    }

    // -------------------------------------------------------------------- (b) no certificate, another authority

    @Test
    @DisplayName("(b) a caller with no certificate is rejected in the handshake, and nothing runs")
    void aCallerWithNoCertificateIsRejected() {
        assertThrows(IOException.class, () -> call(noCertificate, "https://127.0.0.1:" + port, "add", "1,2"));
        assertEquals(0, actor.runs.get(), "the action must not have run");
    }

    @Test
    @DisplayName("(b) a caller whose certificate comes from another authority is rejected, and nothing runs")
    void aCallerFromAnotherAuthorityIsRejected() {
        assertThrows(IOException.class, () -> call(foreign, "https://127.0.0.1:" + port, "add", "1,2"));
        assertEquals(0, actor.runs.get(), "the action must not have run");
    }

    @Test
    @DisplayName("(b) a caller that does not trust the server's authority does not send the request")
    void aCallerThatDoesNotTrustTheServerDoesNotSend() {
        assertThrows(IOException.class, () -> call(distrustful, "https://127.0.0.1:" + port, "add", "1,2"));
        assertEquals(0, actor.runs.get());
    }

    @Test
    @DisplayName("(b) plain HTTP to the TLS port is not served, and nothing runs")
    void plainHttpToTheTlsPortIsNotServed() {
        assertThrows(IOException.class, () -> call(null, "http://127.0.0.1:" + port, "add", "1,2"));
        assertEquals(0, actor.runs.get());
    }

    // -------------------------------------------------------------------- (c) identity not on the policy

    @Test
    @DisplayName("(c) a caller the server trusts but whose identity is not on the policy is refused, and nothing runs")
    void anIdentityNotOnThePolicyIsRefused() throws Exception {
        HttpResponse<String> response = call(intruder, "https://127.0.0.1:" + port, "add", "1,2");

        assertEquals(403, response.statusCode());
        assertTrue(response.body().contains("Not allowed: calc/add"), response.body());
        assertEquals(0, actor.runs.get(), "the action must not have run");
    }

    @Test
    @DisplayName("(c) an identity may call only what is listed for it")
    void anIdentityMayCallOnlyItsOwnList() throws Exception {
        assertEquals(403, call(worker, "https://127.0.0.1:" + port, "echo", "x").statusCode(), "the worker may only add");
        assertEquals(403, call(legacy, "https://127.0.0.1:" + port, "add", "1,2").statusCode(), "the legacy client may only echo");
        assertEquals(0, actor.runs.get());

        HttpResponse<String> echoed = call(legacy, "https://127.0.0.1:" + port, "echo", "hello");
        assertEquals(200, echoed.statusCode());
        assertTrue(echoed.body().contains("\"result\":\"hello\""), echoed.body());
        assertEquals(1, actor.runs.get());
    }

    @Test
    @DisplayName("(c) a certificate whose common name is the text of a SPIFFE ID is not the holder of that SPIFFE ID")
    void aCommonNameIsNotASpiffeId() throws Exception {
        HttpResponse<String> response = call(spoofer, "https://127.0.0.1:" + port, "add", "1,2");

        assertEquals(403, response.statusCode());
        assertEquals(0, actor.runs.get(), "the action must not have run");
    }

    // -------------------------------------------------------------------- (d) listening beyond the loopback address

    @Test
    @DisplayName("(d) listening on every interface without an SSLContext fails at start, before anything listens")
    void everyInterfaceWithoutAnSslContextFails() throws Exception {
        int open = freePort();
        HttpActorServer unsafe = new HttpActorServer(new AllowListDispatcher(system, Set.of()),
                HttpActorServer.ALL_INTERFACES, open);

        IllegalStateException failure = assertThrows(IllegalStateException.class, unsafe::start);

        assertTrue(failure.getMessage().contains("mutual TLS"), failure.getMessage());
        assertFalse(canConnect(open), "nothing may be listening on the port");
    }

    @Test
    @DisplayName("(d) an SSLContext of null does not help: the address still needs TLS")
    void aNullSslContextOnAnotherAddressFails() throws Exception {
        int open = freePort();
        HttpActorServer unsafe = new HttpActorServer(new AllowListDispatcher(system, Set.of()), "0.0.0.0", open, null);

        assertThrows(IllegalStateException.class, unsafe::start);
        assertFalse(canConnect(open));
    }

    @Test
    @DisplayName("(d) DistributedIIActorSystem.startHttpServer on another address without an SSLContext fails too")
    void theDistributedSystemFailsToo() throws Exception {
        int open = freePort();
        DistributedIIActorSystem distributed = DistributedIIActorSystem.builder().noDeduplication()
                .discovery(new OneNodeDiscovery(open)).build();
        try {
            assertThrows(IllegalStateException.class, () -> distributed.startHttpServer("0.0.0.0", open));
            assertFalse(canConnect(open));
        } finally {
            distributed.close();
        }
    }

    @Test
    @DisplayName("(d) on every interface with an SSLContext it serves a trusted caller and still rejects one with no certificate")
    void everyInterfaceWithAnSslContextRequiresACertificate() throws Exception {
        int open = freePort();
        CallPolicy policy = CallPolicy.perCaller(Map.of(
                CallerIdentity.spiffeId(WORKER_ID), Set.of(new AllowedAction("calc", "add"))));
        try (HttpActorServer all = new HttpActorServer(new AllowListDispatcher(system, policy),
                HttpActorServer.ALL_INTERFACES, open, serverContext)) {
            all.start();

            assertEquals(200, call(worker, "https://127.0.0.1:" + open, "add", "1,2").statusCode());
            assertThrows(IOException.class, () -> call(noCertificate, "https://127.0.0.1:" + open, "add", "1,2"));
            assertEquals(1, actor.runs.get(), "only the call of the trusted caller ran");
        }
    }

    @Test
    @DisplayName("(d) the system built with an SSLContext listens on another address and serves a trusted caller")
    void theDistributedSystemWithAnSslContextListens() throws Exception {
        int open = freePort();
        CallPolicy policy = CallPolicy.perCaller(Map.of(
                CallerIdentity.spiffeId(WORKER_ID), Set.of(new AllowedAction("calc", "add"))));
        DistributedIIActorSystem distributed = DistributedIIActorSystem.builder().noDeduplication()
                .discovery(new OneNodeDiscovery(open)).callPolicy(policy).sslContext(serverContext).build();
        try {
            distributed.startHttpServer("0.0.0.0", open);

            assertTrue(canConnect(open), "the server listens");
            assertThrows(IOException.class, () -> call(noCertificate, "https://127.0.0.1:" + open, "add", "1,2"));
        } finally {
            distributed.close();
        }
    }

    // -------------------------------------------------------------------- helpers

    private static HttpResponse<String> call(SSLContext context, String base, String action, String args)
            throws IOException, InterruptedException {
        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5));
        if (context != null) {
            builder.sslContext(context);
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(base + "/actor/calc/invoke"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(new ActorMessage("calc", action, args).toJson()))
                .build();
        return builder.build().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static boolean canConnect(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /** Test double: this machine, alone. Nothing here reaches the network. */
    private record OneNodeDiscovery(int port)
            implements com.scivicslab.pojoactor.distributed.discovery.NodeDiscovery {

        @Override public String getMyNodeId() { return "me"; }

        @Override public String getMyHost() { return "127.0.0.1"; }

        @Override public int getMyPort() { return port; }

        @Override public List<NodeInfo> getAllNodes() { return List.of(); }

        @Override public boolean isApplicable() { return true; }
    }
}
