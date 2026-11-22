package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
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
 * A web page in a browser on the same machine cannot make the server run an action.
 *
 * <p>A page can send a POST to {@code http://127.0.0.1:port/...} with a body it chooses, as long as the
 * request is one a browser sends without asking first: {@code text/plain} or a form type, and an
 * {@code Origin} header naming the page. A page on a name that its owner points at 127.0.0.1 (DNS
 * rebinding) also gets the server's own answer back, with a {@code Host} header naming that page. The
 * server accepts what a node sends: a JSON body, no {@code Origin}, and, when it listens on the loopback
 * address, a {@code Host} that names the loopback address (chat-86, HIGH 2).</p>
 */
@Tag("HttpActorServerCsrf_261123_oo01")
@DisplayName("HttpActorServer — a browser page cannot make it run an action")
@Timeout(60)
class HttpActorServerCsrfTest {

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
    void setUp() throws Exception {
        system = new ActorSystem("csrf");
        worker = new Worker();
        system.actorOf("worker", worker);
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        server = new HttpActorServer(new AllowListDispatcher(system,
                Set.of(new AllowListDispatcher.AllowedAction("worker", "run"))), "127.0.0.1", port);
        server.start();
    }

    @AfterEach
    void tearDown() {
        server.close();
        system.terminate();
    }

    private String body() {
        return RawHttp.message("worker", "run", "x", null);
    }

    private void assertRefusedAndNotRun(RawHttp.Answer answer) throws Exception {
        Thread.sleep(300);
        assertTrue(answer.code() >= 400 && answer.code() < 500, "must be refused, was " + answer.status());
        assertEquals(List.of(), worker.calls, "the action must not have run");
    }

    @Test
    @DisplayName("a text/plain body from another origin does not run the action")
    void textPlain_fromAnotherOrigin_isRefused() throws Exception {
        assertRefusedAndNotRun(RawHttp.post(port, "/actor/worker/invoke", "attacker.example:" + port,
                "text/plain", "http://attacker.example", body()));
    }

    @Test
    @DisplayName("a text/plain body is refused as unsupported even with no Origin")
    void textPlain_withoutOrigin_is415() throws Exception {
        RawHttp.Answer answer = RawHttp.post(port, "/actor/worker/invoke", "127.0.0.1:" + port, "text/plain", null, body());
        assertRefusedAndNotRun(answer);
        assertEquals(415, answer.code());
    }

    @Test
    @DisplayName("a form body is refused as unsupported")
    void formEncoded_is415() throws Exception {
        RawHttp.Answer answer = RawHttp.post(port, "/actor/worker/invoke", "127.0.0.1:" + port,
                "application/x-www-form-urlencoded", null, body());
        assertRefusedAndNotRun(answer);
        assertEquals(415, answer.code());
    }

    @Test
    @DisplayName("a request with no Content-Type is refused as unsupported")
    void noContentType_is415() throws Exception {
        assertEquals(415, RawHttp.post(port, "/actor/worker/invoke", "127.0.0.1:" + port, null, null, body()).code());
        assertEquals(List.of(), worker.calls);
    }

    @Test
    @DisplayName("a JSON body that carries an Origin header is refused: a node sends none")
    void json_withAnOrigin_isForbidden() throws Exception {
        RawHttp.Answer answer = RawHttp.post(port, "/actor/worker/invoke", "127.0.0.1:" + port,
                "application/json", "http://attacker.example", body());
        assertRefusedAndNotRun(answer);
        assertEquals(403, answer.code());
    }

    @Test
    @DisplayName("a loopback server refuses a request whose Host is not the loopback address (DNS rebinding)")
    void json_withAForeignHost_isForbidden() throws Exception {
        RawHttp.Answer answer = RawHttp.post(port, "/actor/worker/invoke", "attacker.example:" + port,
                "application/json", null, body());
        assertRefusedAndNotRun(answer);
        assertEquals(403, answer.code());
    }

    @Test
    @DisplayName("a node's request still runs: JSON, no Origin, Host of the loopback address")
    void aNodesRequest_runs() throws Exception {
        for (String host : new String[] {"127.0.0.1:" + port, "localhost:" + port}) {
            worker.calls.clear();
            RawHttp.Answer answer = RawHttp.post(port, "/actor/worker/invoke", host, "application/json", null, body());
            assertEquals(200, answer.code(), host);
            assertEquals(List.of("run:x"), worker.calls, host);
        }
    }

    @Test
    @DisplayName("application/json with a charset is still JSON")
    void jsonWithCharset_runs() throws Exception {
        RawHttp.Answer answer = RawHttp.post(port, "/actor/worker/invoke", "127.0.0.1:" + port,
                "application/json; charset=utf-8", null, body());
        assertEquals(200, answer.code());
        assertFalse(worker.calls.isEmpty());
    }
}
