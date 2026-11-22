package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.action.CallableByActionName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Over HTTP, while the message-id store cannot record, the receiver answers 503 and runs
 * nothing; the message whose id could not be recorded keeps its normal answer.
 */
@Tag("DistributedActorSystemRedesign_261115_oo01")
class HttpActorServerStoreUnavailableTest {

    private static final int PORT = 18189;

    /** Remembers the arguments of every run, in order. */
    static class Recorder implements CallableByActionName {
        final List<String> runs = Collections.synchronizedList(new ArrayList<>());

        @Override
        public ActionResult callByActionName(String action, String args) {
            runs.add(args);
            return new ActionResult(true, "ran " + args);
        }
    }

    private static HttpResponse<String> post(String verb, String id) throws Exception {
        ActorMessage msg = new ActorMessage("recorder", "run", id, id, null);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + PORT + "/actor/recorder/" + verb))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(msg.toJson()))
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void sendAndInvoke_whileTheStoreCannotRecord_answer503AndRunNothing() throws Exception {
        ActorSystem system = new ActorSystem("test");
        Recorder recorder = new Recorder();
        system.actorOf("recorder", recorder);
        BreakableMessageIdStore store = new BreakableMessageIdStore();
        MessageIdStoreTest.FakeClock clock = new MessageIdStoreTest.FakeClock();
        Dispatcher dispatcher = new DedupDispatcher(new AllowListDispatcher(system,
                Set.of(new AllowListDispatcher.AllowedAction("recorder", "run"))), store, clock);

        try (HttpActorServer server = new HttpActorServer(dispatcher, "127.0.0.1", PORT)) {
            server.start();
            store.broken = true;

            HttpResponse<String> first = post("invoke", "m1");
            assertEquals(200, first.statusCode(), first.body());
            assertTrue(first.body().contains("ran m1"), first.body());

            HttpResponse<String> send = post("send", "m2");
            assertEquals(503, send.statusCode(), send.body());
            assertEquals("message id store unavailable", send.body());

            HttpResponse<String> invoke = post("invoke", "m3");
            assertEquals(503, invoke.statusCode(), invoke.body());
            assertEquals("message id store unavailable", invoke.body());

            store.broken = false;
            clock.advance(Duration.ofSeconds(1));

            HttpResponse<String> after = post("invoke", "m4");
            assertEquals(200, after.statusCode(), after.body());
            assertTrue(after.body().contains("ran m4"), after.body());

            // m4 ran after anything put in the mailbox before it, so m2 and m3 would show here.
            assertEquals(List.of("m1", "m4"), recorder.runs);
            assertTrue(store.contains("m1"), "the id that could not be recorded is recorded now");
        } finally {
            system.terminate();
        }
    }
}
