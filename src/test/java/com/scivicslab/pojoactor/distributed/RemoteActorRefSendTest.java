package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.action.CallableByActionName;
import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.distributed.transport.HttpTransport;
import com.scivicslab.pojoactor.distributed.transport.StubTransport;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** {@code RemoteActorRef.send} hands back a receipt: accepted into the remote mailbox, or not. */
class RemoteActorRefSendTest {

    private final NodeInfo node = new NodeInfo("node-b", "127.0.0.1", 18190);

    @Test
    void send_returnsTheTransportsReceiptAndCarriesTheMessageId() {
        StubTransport transport = new StubTransport();
        RemoteActorRef ref = new RemoteActorRef("worker", node, transport);

        Receipt receipt = ref.send("process", "hello");

        assertTrue(receipt.accepted());
        assertEquals(1, transport.sent.size());
        assertEquals("process", transport.sent.get(0).getActionName());
        assertEquals(transport.sent.get(0).getMessageId(), receipt.messageId());
    }

    @Test
    void send_whenRefused_receiptSaysSo() {
        StubTransport transport = new StubTransport();
        transport.accept = false;
        Receipt receipt = new RemoteActorRef("worker", node, transport).send("process", "x");
        assertFalse(receipt.accepted());
        assertEquals("stub refused", receipt.detail());
    }

    static class Slow implements CallableByActionName {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public ActionResult callByActionName(String action, String args) {
            started.countDown();
            try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { }
            return new ActionResult(true, "done");
        }
    }

    /** Over real HTTP: the receipt comes back while the action is still running. */
    @Test
    void send_overHttp_returnsTheReceiptBeforeTheActionFinishes() throws Exception {
        int port = 18191;
        ActorSystem system = new ActorSystem("remote");
        Slow slow = new Slow();
        system.actorOf("slow", slow);
        try (HttpActorServer server = new HttpActorServer(new AllowListDispatcher(system,
                Set.of(new AllowListDispatcher.AllowedAction("slow", "run"))), "127.0.0.1", port)) {
            server.start();
            RemoteActorRef ref = new RemoteActorRef("slow",
                    new NodeInfo("remote", "127.0.0.1", port), new HttpTransport());

            Receipt receipt = ref.send("run", "");

            assertTrue(receipt.accepted(), receipt.detail());
            assertTrue(slow.started.await(5, TimeUnit.SECONDS));
            assertEquals(1, slow.release.getCount(), "the action has not been released: receipt was early");
            slow.release.countDown();
        } finally {
            system.terminate();
        }
    }

    @Test
    void send_overHttp_actionNotOnTheList_isNotAccepted() throws Exception {
        int port = 18192;
        ActorSystem system = new ActorSystem("remote");
        system.actorOf("slow", new Slow());
        try (HttpActorServer server = new HttpActorServer(
                new AllowListDispatcher(system, Set.of()), "127.0.0.1", port)) {
            server.start();
            Receipt receipt = new RemoteActorRef("slow",
                    new NodeInfo("remote", "127.0.0.1", port), new HttpTransport()).send("run", "");
            assertFalse(receipt.accepted());
        } finally {
            system.terminate();
        }
    }

    /** No receipt, no exception: the caller decides whether to send again. */
    @Test
    void send_overHttp_unreachableNode_isNotAcceptedAndDoesNotThrow() {
        Receipt receipt = assertDoesNotThrow(() -> new RemoteActorRef("slow",
                new NodeInfo("remote", "127.0.0.1", 18199), new HttpTransport()).send("run", ""));
        assertFalse(receipt.accepted());
    }
}
