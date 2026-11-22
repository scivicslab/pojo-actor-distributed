package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.action.CallableByActionName;
import com.scivicslab.pojoactor.core.ActorRef;
import com.scivicslab.pojoactor.core.ActorSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that a remote call is checked against the allow-list and then runs through the
 * target actor's mailbox (one message at a time), and that the receipt means "accepted".
 */
class AllowListDispatcherTest {

    /** A POJO whose actions record when they ran and how long they overlap. */
    static class Worker implements CallableByActionName {
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final AtomicLong firstEnd = new AtomicLong();
        final AtomicLong secondStart = new AtomicLong();
        volatile String callerThread;

        @Override
        public ActionResult callByActionName(String action, String args) {
            switch (action) {
                case "first" -> {
                    firstStarted.countDown();
                    try { releaseFirst.await(5, TimeUnit.SECONDS); } catch (InterruptedException e) { }
                    firstEnd.set(System.nanoTime());
                    return new ActionResult(true, "first-done");
                }
                case "second" -> {
                    secondStart.set(System.nanoTime());
                    return new ActionResult(true, "second-done");
                }
                case "thread" -> {
                    callerThread = Thread.currentThread().getName();
                    return new ActionResult(true, args);
                }
                case "echo" -> { return new ActionResult(true, args); }
                case "hidden" -> { return new ActionResult(true, "should not be reachable"); }
                default -> { return new ActionResult(false, "Unknown action: " + action); }
            }
        }
    }

    /** An ActorRef that itself knows action names, like Turing-workflow's IIActorRef. */
    static class NamedRef extends ActorRef<Worker> implements CallableByActionName {
        NamedRef(String name, Worker w, ActorSystem s) { super(name, w, s); }
        @Override
        public ActionResult callByActionName(String action, String args) {
            return new ActionResult(true, "via-ref:" + action + ":" + args);
        }
    }

    private ActorSystem system;
    private Worker worker;
    private AllowListDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        system = new ActorSystem("dispatcher-test");
        worker = new Worker();
        system.actorOf("worker", worker);
        system.addActor(new NamedRef("named", new Worker(), system));
        dispatcher = new AllowListDispatcher(system, Set.of(
                new AllowListDispatcher.AllowedAction("worker", "first"),
                new AllowListDispatcher.AllowedAction("worker", "second"),
                new AllowListDispatcher.AllowedAction("worker", "echo"),
                new AllowListDispatcher.AllowedAction("worker", "thread"),
                new AllowListDispatcher.AllowedAction("named", "ping")));
    }

    @AfterEach
    void tearDown() { system.terminate(); }

    @Test
    void emptyAllowList_rejectsEverything() {
        var d = new AllowListDispatcher(system, Set.of());
        Dispatcher.Dispatch r = d.dispatch("worker", "echo", "x");
        assertFalse(r.accepted());
        assertFalse(r.result().join().isSuccess());
    }

    @Test
    void actionNotOnTheList_isRejected() {
        Dispatcher.Dispatch r = dispatcher.dispatch("worker", "hidden", "x");
        assertFalse(r.accepted());
        assertFalse(r.result().join().isSuccess());
    }

    /** An outside caller must not learn whether an actor exists: both failures read alike. */
    @Test
    void missingActorAndForbiddenAction_failWithTheSameSentence() {
        var d = new AllowListDispatcher(system, Set.of(
                new AllowListDispatcher.AllowedAction("ghost", "hidden")));
        var missing = d.dispatch("ghost", "hidden", "x");
        var forbidden = d.dispatch("worker", "hidden", "x");
        assertFalse(missing.accepted());
        assertFalse(forbidden.accepted());
        assertEquals("Not allowed: ghost/hidden", missing.result().join().getResult());
        assertEquals("Not allowed: worker/hidden", forbidden.result().join().getResult());
    }

    @Test
    void allowedAction_runsAndReturnsItsResult() {
        Dispatcher.Dispatch r = dispatcher.dispatch("worker", "echo", "hello");
        assertTrue(r.accepted());
        assertEquals("hello", r.result().join().getResult());
    }

    @Test
    void whenTheRefKnowsActionNames_thatIsUsed() {
        var r = dispatcher.dispatch("named", "ping", "1");
        assertTrue(r.accepted());
        assertEquals("via-ref:ping:1", r.result().join().getResult());
    }

    /** The receipt comes back while the action is still running: accepted into the mailbox. */
    @Test
    void receiptDoesNotWaitForTheAction() throws Exception {
        Dispatcher.Dispatch r = dispatcher.dispatch("worker", "first", "");
        assertTrue(r.accepted());
        assertTrue(worker.firstStarted.await(5, TimeUnit.SECONDS));
        assertFalse(r.result().isDone(), "the action is still running");
        worker.releaseFirst.countDown();
        assertEquals("first-done", r.result().get(5, TimeUnit.SECONDS).getResult());
    }

    /** One message at a time: the second action starts only after the first has finished. */
    @Test
    void remoteCallsRunThroughTheMailboxOneAtATime() throws Exception {
        Dispatcher.Dispatch a = dispatcher.dispatch("worker", "first", "");
        assertTrue(worker.firstStarted.await(5, TimeUnit.SECONDS));
        Dispatcher.Dispatch b = dispatcher.dispatch("worker", "second", "");
        Thread.sleep(200); // a direct call on another thread would have started by now
        assertEquals(0, worker.secondStart.get(), "second must wait in the mailbox");
        worker.releaseFirst.countDown();
        CompletableFuture.allOf(a.result(), b.result()).get(5, TimeUnit.SECONDS);
        assertTrue(worker.secondStart.get() >= worker.firstEnd.get());
    }

    @Test
    void actionDoesNotRunOnTheCallersThread() {
        String me = Thread.currentThread().getName();
        dispatcher.dispatch("worker", "thread", "t").result().join();
        assertNotEquals(me, worker.callerThread);
    }

    /** A POJO whose "crash" action throws an Error, which ends the actor's message loop. */
    static class Fragile implements CallableByActionName {
        @Override
        public ActionResult callByActionName(String action, String args) {
            // A fatal VirtualMachineError (not StackOverflowError) ends the message loop in every
            // POJO-actor version; since 4.9 an ordinary Error such as StackOverflowError no longer does.
            if (action.equals("crash")) throw new InternalError("test: the JVM-level failure ends the loop");
            return new ActionResult(true, args);
        }
    }

    /**
     * An actor whose message loop has died stays registered, but nobody reads its mailbox. A call
     * accepted for it would never run, and its receipt would be a lie: the call is refused.
     * Unknown and forbidden calls still read "Not allowed", because the allow-list is checked first.
     */
    @Test
    @Tag("DistributedActorSystemRedesign_261115_oo01")
    void actorWhoseMessageLoopDied_isRefusedNotAccepted() throws Exception {
        ActorRef<Fragile> fragile = system.actorOf("fragile", new Fragile());
        var d = new AllowListDispatcher(system, Set.of(
                new AllowListDispatcher.AllowedAction("fragile", "crash"),
                new AllowListDispatcher.AllowedAction("fragile", "echo")));

        assertTrue(d.dispatch("fragile", "crash", "").accepted());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (fragile.isAlive() && System.nanoTime() < deadline) Thread.sleep(10);
        assertFalse(fragile.isAlive(), "the Error ended the actor's message loop");

        Dispatcher.Dispatch r = d.dispatch("fragile", "echo", "x");
        assertFalse(r.accepted());
        assertEquals("Not accepted: actor not running", r.result().join().getResult());
        assertEquals("Not allowed: fragile/hidden", d.dispatch("fragile", "hidden", "x").result().join().getResult());
    }
}
