package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The second copy of a message is dropped; a message that was not accepted is not remembered. */
class DedupDispatcherTest {

    private final List<String> reached = Collections.synchronizedList(new ArrayList<>());

    private Dispatcher accepting() {
        return m -> {
            reached.add(m.getMessageId());
            return new Dispatcher.Dispatch(true, CompletableFuture.completedFuture(new ActionResult(true, "ok")));
        };
    }

    private MessageIdStore store() {
        return new InMemoryMessageIdStore(100, Duration.ofHours(1));
    }

    private static ActorMessage msg(String id) {
        return new ActorMessage("worker", "run", "x", id, null);
    }

    @Test
    void theSecondCopyDoesNotReachTheActor_andIsStillAnAcceptedReceipt() {
        Dispatcher d = new DedupDispatcher(accepting(), store());

        Dispatcher.Dispatch first = d.dispatch(msg("m1"));
        Dispatcher.Dispatch second = d.dispatch(msg("m1"));

        assertTrue(first.accepted());
        assertTrue(second.accepted(), "the first copy was accepted, so the sender may stop resending");
        assertEquals(List.of("m1"), reached);
        assertFalse(second.result().join().isSuccess());
        assertTrue(second.result().join().getResult().contains("Duplicate"));
    }

    @Test
    void differentIdsAreBothDelivered() {
        Dispatcher d = new DedupDispatcher(accepting(), store());
        d.dispatch(msg("m1"));
        d.dispatch(msg("m2"));
        assertEquals(List.of("m1", "m2"), reached);
    }

    @Test
    void aRefusedMessageIsNotRemembered() {
        AtomicInteger calls = new AtomicInteger();
        Dispatcher refuseOnce = m -> {
            if (calls.incrementAndGet() == 1) return Dispatcher.Dispatch.refused("Not accepted");
            reached.add(m.getMessageId());
            return new Dispatcher.Dispatch(true, CompletableFuture.completedFuture(new ActionResult(true, "ok")));
        };
        Dispatcher d = new DedupDispatcher(refuseOnce, store());

        assertFalse(d.dispatch(msg("m1")).accepted());
        assertTrue(d.dispatch(msg("m1")).accepted(), "a resend of a refused message must be tried again");
        assertEquals(List.of("m1"), reached);
    }

    @Test
    void anExceptionFromTheInnerDispatcherIsNotRemembered() {
        AtomicInteger calls = new AtomicInteger();
        Dispatcher failOnce = m -> {
            if (calls.incrementAndGet() == 1) throw new IllegalStateException("mailbox down");
            reached.add(m.getMessageId());
            return new Dispatcher.Dispatch(true, CompletableFuture.completedFuture(new ActionResult(true, "ok")));
        };
        Dispatcher d = new DedupDispatcher(failOnce, store());

        assertThrows(IllegalStateException.class, () -> d.dispatch(msg("m1")));
        d.dispatch(msg("m1"));
        assertEquals(List.of("m1"), reached);
    }

    /** Two copies arriving at the same moment (an HTTP resend racing the first try) run once. */
    @Test
    void concurrentCopiesOfOneMessageRunOnce() throws Exception {
        Dispatcher d = new DedupDispatcher(accepting(), store());
        int n = 32;
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            threads.add(Thread.ofVirtual().start(() -> {
                try { go.await(); } catch (InterruptedException e) { }
                d.dispatch(msg("same"));
            }));
        }
        go.countDown();
        for (Thread t : threads) t.join();
        assertEquals(List.of("same"), reached);
    }
}
