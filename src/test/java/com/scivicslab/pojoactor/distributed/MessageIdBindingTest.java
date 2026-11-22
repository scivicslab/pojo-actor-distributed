package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.action.CallableByActionName;
import com.scivicslab.pojoactor.core.ActorSystem;

/**
 * A message id is a memory of one message, so another sender cannot use it to stop that message, and a
 * flood of new ids cannot make the server forget an old one (chat-86, HIGH 4).
 *
 * <p>Before the change the id alone was the key: whoever sent {@code order-42} first made the real
 * {@code order-42} a "duplicate" that the server dropped while still answering 202, and sending
 * {@code maxEntries} fresh ids pushed {@code pay-1} out of memory so that a copy of it ran again. A
 * message whose id is known is also answered 202 before the allow-list is asked, so the answer told
 * a caller which ids exist for an actor that is not on the list.</p>
 */
@Tag("MessageIdBinding_261123_oo01")
@DisplayName("Message ids — bound to the message, and not forgotten under load")
@Timeout(90)
class MessageIdBindingTest {

    static class Worker implements CallableByActionName {
        final List<String> calls = new CopyOnWriteArrayList<>();

        @Override
        public ActionResult callByActionName(String action, String args) {
            calls.add(action + ":" + args);
            return new ActionResult(true, "ran " + args);
        }
    }

    private ActorSystem system;
    private Worker worker;
    private HttpActorServer server;
    private int port;

    @BeforeEach
    void setUp() {
        system = new ActorSystem("ids");
        worker = new Worker();
        system.actorOf("worker", worker);
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.close();
        system.terminate();
    }

    private void start(int maxEntries) throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        AllowListDispatcher allow = new AllowListDispatcher(system,
                Set.of(new AllowListDispatcher.AllowedAction("worker", "run")));
        server = new HttpActorServer(new DedupDispatcher(allow,
                new InMemoryMessageIdStore(maxEntries, Duration.ofHours(1))), "127.0.0.1", port);
        server.start();
    }

    private RawHttp.Answer send(String actor, String action, String args, String id) throws Exception {
        return RawHttp.post(port, "/actor/" + actor + "/send", "127.0.0.1:" + port, "application/json", null,
                RawHttp.message(actor, action, args, id));
    }

    @Test
    @DisplayName("a different message sent first under the same id does not stop the real one")
    void anotherSendersMessage_underTheSameId_doesNotPreemptTheRealOne() throws Exception {
        start(1000);

        send("worker", "run", "attacker-args", "order-42");
        RawHttp.Answer real = send("worker", "run", "real-args", "order-42");
        Thread.sleep(500);

        assertEquals(202, real.code());
        assertTrue(worker.calls.contains("run:real-args"), "the real message must run, calls were " + worker.calls);
    }

    @Test
    @DisplayName("the same message sent twice still runs once")
    void theSameMessageTwice_runsOnce() throws Exception {
        start(1000);

        send("worker", "run", "pay", "pay-7");
        RawHttp.Answer again = send("worker", "run", "pay", "pay-7");
        Thread.sleep(500);

        assertEquals(202, again.code());
        assertEquals(List.of("run:pay"), worker.calls);
    }

    @Test
    @DisplayName("a flood of new ids does not make the server run an old message again")
    void aFloodOfFreshIds_doesNotAllowAReplay() throws Exception {
        start(100);
        send("worker", "run", "pay-once", "pay-1");
        for (int i = 0; i < 150; i++) {
            send("worker", "run", "flood", "flood-" + i);
        }

        send("worker", "run", "pay-once", "pay-1");
        Thread.sleep(800);

        long runs = worker.calls.stream().filter("run:pay-once"::equals).count();
        assertEquals(1, runs, "pay-once must run once, calls were " + worker.calls.size());
    }

    @Test
    @DisplayName("a full store refuses new messages, still recognises copies, and takes new ones once ids expire")
    void aFullStore_refusesNewMessages_recognisesCopies_andRecoversWhenIdsExpire() {
        MessageIdStoreTest.FakeClock clock = new MessageIdStoreTest.FakeClock();
        List<String> ran = new CopyOnWriteArrayList<>();
        DedupDispatcher dedup = new DedupDispatcher(m -> {
            ran.add(m.getMessageId());
            return new Dispatcher.Dispatch(true,
                    java.util.concurrent.CompletableFuture.completedFuture(new ActionResult(true, "ok")));
        }, new InMemoryMessageIdStore(2, Duration.ofMinutes(10), clock), clock);

        ActorMessage a = new ActorMessage("worker", "run", "a", "id-a", null);
        ActorMessage b = new ActorMessage("worker", "run", "b", "id-b", null);
        ActorMessage c = new ActorMessage("worker", "run", "c", "id-c", null);
        assertTrue(dedup.dispatch(a).accepted());
        assertTrue(dedup.dispatch(b).accepted());

        Dispatcher.Dispatch refused = dedup.dispatch(c);
        assertTrue(refused.unavailable(), "a new message while the store is full: " + refused.outcome());
        assertEquals("message id store full", refused.result().join().getResult());

        assertTrue(dedup.dispatch(a).accepted(), "a copy of a message the store holds is still recognised");
        assertEquals(List.of("id-a", "id-b"), ran, "neither the refused message nor the copy ran");

        clock.advance(Duration.ofMinutes(11));
        assertTrue(dedup.dispatch(c).accepted(), "once the ids have expired the store has room again");
        assertEquals(List.of("id-a", "id-b", "id-c"), ran);
    }

    @Test
    @DisplayName("an id seen for one actor does not turn a refusal for an unlisted actor into an acceptance")
    void aSeenId_forAnUnlistedActor_isStillRefused() throws Exception {
        start(1000);
        send("worker", "run", "a", "seen-1");

        RawHttp.Answer seen = send("secret-admin", "shutdown", "", "seen-1");
        RawHttp.Answer fresh = send("secret-admin", "shutdown", "", "fresh-1");

        assertEquals(403, fresh.code());
        assertEquals(403, seen.code(), "the answer must not depend on whether the id was seen");
    }
}
