package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.action.CallableByActionName;
import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.distributed.discovery.StaticNodeDiscovery;
import com.scivicslab.pojoactor.distributed.transport.HttpTransport;
import com.scivicslab.pojoactor.distributed.transport.StubTransport;
import com.scivicslab.pojoactor.distributed.transport.TransportLayer;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** A sender that did not get a receipt sends again with the same messageId; a call's wait is settable. */
class RemoteActorRefResendTest {

    private final NodeInfo node = new NodeInfo("node-b", "127.0.0.1", 18290);

    @Test
    void sendWithAGivenMessageId_putsThatIdOnTheWire() {
        StubTransport transport = new StubTransport();
        RemoteActorRef ref = new RemoteActorRef("worker", node, transport);

        Receipt receipt = ref.send("run", "x", "id-42");

        assertEquals("id-42", transport.sent.get(0).getMessageId());
        assertEquals("id-42", receipt.messageId());
    }

    @Test
    void sendWithRetry_stopsAtTheFirstReceipt_andEveryTryHasTheSameId() {
        StubTransport transport = new StubTransport();
        transport.refuseFirst = 2;
        RemoteActorRef ref = new RemoteActorRef("worker", node, transport);

        Receipt receipt = ref.sendWithRetry("run", "x", 5, Duration.ofMillis(1));

        assertTrue(receipt.accepted());
        assertEquals(3, transport.sent.size());
        assertEquals(1, transport.sent.stream().map(ActorMessage::getMessageId).distinct().count());
        assertEquals(receipt.messageId(), transport.sent.get(0).getMessageId());
    }

    @Test
    void sendWithRetry_givesUpAfterTheGivenNumberOfTries() {
        StubTransport transport = new StubTransport();
        transport.accept = false;
        RemoteActorRef ref = new RemoteActorRef("worker", node, transport);

        Receipt receipt = ref.sendWithRetry("run", "x", 3, Duration.ofMillis(1));

        assertFalse(receipt.accepted());
        assertEquals(3, transport.sent.size());
    }

    @Test
    void sendWithRetry_waitsBetweenTries() {
        StubTransport transport = new StubTransport();
        transport.accept = false;
        RemoteActorRef ref = new RemoteActorRef("worker", node, transport);

        long start = System.nanoTime();
        ref.sendWithRetry("run", "x", 3, Duration.ofMillis(100));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue(elapsedMs >= 200, "two waits of 100 ms between three tries: " + elapsedMs);
    }

    @Test
    void sendWithRetry_needsBothTheCountAndTheDelay() {
        RemoteActorRef ref = new RemoteActorRef("worker", node, new StubTransport());
        assertThrows(IllegalArgumentException.class, () -> ref.sendWithRetry("run", "x", 0, Duration.ofMillis(1)));
        assertThrows(IllegalArgumentException.class, () -> ref.sendWithRetry("run", "x", 2, null));
        assertThrows(IllegalArgumentException.class, () -> ref.sendWithRetry("run", "x", 2, Duration.ofMillis(-1)));
    }

    // ---- the wait of a synchronous call ----

    @Test
    void callByActionName_withATimeout_passesItToTheTransport() {
        StubTransport transport = new StubTransport();
        new RemoteActorRef("worker", node, transport).callByActionName("ping", "", Duration.ofSeconds(7));
        assertEquals(Duration.ofSeconds(7), transport.lastTimeout);
    }

    @Test
    void callByActionName_withoutATimeout_usesTheWaitTheSystemWasBuiltWith() {
        StubTransport transport = new StubTransport();
        DistributedIIActorSystem system = DistributedIIActorSystem.builder().noDeduplication()
                .transport(transport)
                .callTimeout(Duration.ofSeconds(5))
                .discovery(new StaticNodeDiscovery(new NodeInfo("me", "127.0.0.1", 1),
                        List.of(new NodeInfo("node-b", "127.0.0.1", 2))))
                .build();
        try {
            system.getRemoteActor("node-b", "worker").callByActionName("ping", "");
            assertEquals(Duration.ofSeconds(5), transport.lastTimeout);
        } finally {
            system.terminate();
        }
    }

    @Test
    void callByActionName_withoutAnyConfiguration_waitsThirtySeconds() {
        StubTransport transport = new StubTransport();
        new RemoteActorRef("worker", node, transport).callByActionName("ping", "");
        assertEquals(Duration.ofSeconds(30), transport.lastTimeout);
    }

    // ---- resend against a real receiver that drops duplicates ----

    static class Counter implements CallableByActionName {
        final AtomicInteger runs = new AtomicInteger();
        @Override public ActionResult callByActionName(String action, String args) {
            runs.incrementAndGet();
            return new ActionResult(true, "counted");
        }
    }

    /** Sends for real but throws the first receipt away, as if the answer was lost on the way back. */
    static class LosesTheFirstReceipt implements TransportLayer {
        private final TransportLayer real = new HttpTransport();
        private boolean first = true;
        @Override public Receipt send(NodeInfo target, ActorMessage message) {
            Receipt r = real.send(target, message);
            if (first) {
                first = false;
                return Receipt.refused(message.getMessageId(), "receipt lost");
            }
            return r;
        }
        @Override public ActionResult sendAndWait(NodeInfo t, ActorMessage m, Duration d) { return real.sendAndWait(t, m, d); }
        @Override public void close() { real.close(); }
    }

    @Test
    void aResendAfterALostReceipt_runsTheActionOnce() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        NodeInfo receiverNode = new NodeInfo("receiver", "127.0.0.1", port);
        DistributedIIActorSystem receiver = DistributedIIActorSystem.builder()
                .discovery(new StaticNodeDiscovery(receiverNode, List.of()))
                .allowedActions(Set.of(new AllowListDispatcher.AllowedAction("counter", "tick")))
                .deduplicateWith(new InMemoryMessageIdStore(100, Duration.ofHours(1)))
                .build();
        try {
            Counter counter = new Counter();
            receiver.actorOf("counter", counter);
            receiver.startHttpServer("127.0.0.1", port);

            Receipt receipt = new RemoteActorRef("counter", receiverNode, new LosesTheFirstReceipt())
                    .sendWithRetry("tick", "", 3, Duration.ofMillis(10));
            Thread.sleep(300);

            assertTrue(receipt.accepted());
            assertEquals(1, counter.runs.get(), "two copies reached the receiver; the actor ran once");
        } finally {
            receiver.terminate();
        }
    }
}
