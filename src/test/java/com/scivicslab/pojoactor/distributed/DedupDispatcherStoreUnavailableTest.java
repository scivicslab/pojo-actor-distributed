package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * While the message-id store cannot record, the dispatcher takes no call: a call it took could
 * not be told apart from its resend, and the same action would run again for every copy.
 */
@Tag("DistributedActorSystemRedesign_261115_oo01")
class DedupDispatcherStoreUnavailableTest {

    private final List<String> reached = new ArrayList<>();
    private final BreakableMessageIdStore store = new BreakableMessageIdStore();
    private final MessageIdStoreTest.FakeClock clock = new MessageIdStoreTest.FakeClock();
    private final DedupDispatcher dedup = new DedupDispatcher(m -> {
        reached.add(m.getMessageId());
        return new Dispatcher.Dispatch(true, CompletableFuture.completedFuture(new ActionResult(true, "ok")));
    }, store, clock);

    private static ActorMessage msg(String id) {
        return new ActorMessage("worker", "run", "x", id, null);
    }

    @Test
    void dispatch_whenTheIdCannotBeRecorded_keepsTheReceipt_andTakesNoMoreCalls() {
        store.broken = true;

        Dispatcher.Dispatch first = dedup.dispatch(msg("m1"));
        Dispatcher.Dispatch second = dedup.dispatch(msg("m2"));
        Dispatcher.Dispatch resend = dedup.dispatch(msg("m1"));

        assertTrue(first.accepted(), "m1 is in the mailbox and will run: its receipt stands");
        assertTrue(second.unavailable(), "outcome " + second.outcome());
        assertFalse(second.accepted());
        assertEquals("message id store unavailable", second.result().join().getResult());
        assertTrue(resend.unavailable(), "a resend of m1 must not run m1 again");
        assertEquals(List.of("m1"), reached);
    }

    @Test
    void dispatch_afterTheStoreWorksAgain_recordsThePendingIdFirst_thenTakesTheCall() {
        store.broken = true;
        dedup.dispatch(msg("m1"));
        store.broken = false;
        clock.advance(Duration.ofSeconds(1));

        Dispatcher.Dispatch next = dedup.dispatch(msg("m2"));
        Dispatcher.Dispatch resend = dedup.dispatch(msg("m1"));

        assertTrue(next.accepted(), "outcome " + next.outcome());
        assertTrue(store.contains("m1"), "the id that could not be recorded is recorded now");
        assertTrue(store.contains("m2"));
        assertTrue(resend.accepted(), "a resend of m1 is a duplicate: accepted, not run");
        assertEquals(List.of("m1", "m2"), reached);
    }

    /** The store is tried again after 1 s, then 2, 4, 8, 16 s, then every 30 s; never sooner. */
    @Test
    void recover_triesTheStoreAgainOnlyWhenTheBackoffHasPassed() {
        store.broken = true;
        dedup.dispatch(msg("m1"));
        assertEquals(1, store.addAttempts.get());

        int attempts = 1;
        for (long waitSeconds : new long[] {1, 2, 4, 8, 16, 30, 30}) {
            clock.advance(Duration.ofSeconds(waitSeconds).minusMillis(1));
            assertFalse(dedup.recover());
            assertTrue(dedup.dispatch(msg("other")).unavailable());
            assertEquals(attempts, store.addAttempts.get(), "tried before " + waitSeconds + " s had passed");

            clock.advance(Duration.ofMillis(1));
            assertFalse(dedup.recover());
            attempts++;
            assertEquals(attempts, store.addAttempts.get(), "not tried after " + waitSeconds + " s");
        }

        store.broken = false;
        clock.advance(Duration.ofSeconds(30));
        assertTrue(dedup.recover());
        assertTrue(dedup.recover(), "available stays available");
        assertEquals(List.of("m1"), reached, "nothing ran while the store was down");
    }

    @Test
    void recover_withNothingPending_isTrue() {
        assertTrue(dedup.recover());
    }

    /** A store that cannot be read cannot tell a copy from a new message either: nothing runs. */
    @Test
    void dispatch_whenTheStoreCannotBeRead_takesNoCall_untilItCanBeReadAgain() {
        store.containsBroken = true;

        Dispatcher.Dispatch first = dedup.dispatch(msg("m1"));
        Dispatcher.Dispatch second = dedup.dispatch(msg("m2"));

        assertTrue(first.unavailable(), "outcome " + first.outcome());
        assertEquals("message id store unavailable", first.result().join().getResult());
        assertTrue(second.unavailable(), "outcome " + second.outcome());
        assertEquals(1, store.containsAttempts.get(), "the store is not read again before the backoff");
        assertFalse(dedup.recover());

        clock.advance(Duration.ofSeconds(1));
        assertFalse(dedup.recover(), "still cannot be read");
        assertEquals(2, store.containsAttempts.get());

        store.containsBroken = false;
        clock.advance(Duration.ofSeconds(2));
        Dispatcher.Dispatch after = dedup.dispatch(msg("m1"));

        assertTrue(after.accepted(), "outcome " + after.outcome());
        assertEquals(List.of("m1"), reached, "m1 ran once, after the store could be read");
        assertEquals(1, store.addAttempts.get(), "only m1 was recorded: nothing was pending");
    }
}
