package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
 * One request cannot make the server hold an unbounded amount of memory, and one client cannot hold an
 * unbounded number of the server's threads (chat-86, HIGH 5).
 *
 * <p>Before the limits, the server read the whole body into memory before looking at it, and every
 * {@code /invoke} held a connection and a virtual thread for up to 300 s with no cap on how many.</p>
 */
@Tag("HttpActorServerLimits_261123_oo01")
@DisplayName("HttpActorServer — limits on the size of a request and on how many are in flight")
@Timeout(60)
class HttpActorServerLimitsTest {

    static class Worker implements CallableByActionName {
        final List<String> calls = new CopyOnWriteArrayList<>();
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public ActionResult callByActionName(String action, String args) {
            calls.add(action);
            if ("block".equals(action)) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return new ActionResult(true, "ran");
        }
    }

    private ActorSystem system;
    private Worker worker;
    private HttpActorServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        system = new ActorSystem("limits");
        worker = new Worker();
        system.actorOf("worker", worker);
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        server = new HttpActorServer(new AllowListDispatcher(system, Set.of(
                new AllowListDispatcher.AllowedAction("worker", "run"),
                new AllowListDispatcher.AllowedAction("worker", "block"))), "127.0.0.1", port);
    }

    @AfterEach
    void tearDown() {
        worker.release.countDown();
        server.close();
        system.terminate();
    }

    private static String head(String path, String extra, long length) {
        return "POST " + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n" + extra
                + (length >= 0 ? "Content-Length: " + length + "\r\n" : "") + "Connection: close\r\n\r\n";
    }

    @Test
    @DisplayName("a Content-Length over the limit is refused with 413 before the body is read")
    void declaredLength_overTheLimit_is413WithoutReadingTheBody() throws Exception {
        server.setMaxBodyBytes(4096);
        server.start();

        // Only the headers are sent, claiming 100 MiB. A server that waits for the body never answers.
        RawHttp.Answer answer = RawHttp.send(port, head("/actor/worker/send", "", 100L * 1024 * 1024), null, 5000);

        assertEquals(413, answer.code());
        assertEquals(List.of(), worker.calls);
    }

    @Test
    @DisplayName("a chunked body that grows past the limit is refused with 413")
    void chunkedBody_overTheLimit_is413() throws Exception {
        server.setMaxBodyBytes(4096);
        server.start();

        StringBuilder body = new StringBuilder();
        String chunk = "x".repeat(1024);
        for (int i = 0; i < 16; i++) {
            body.append(Integer.toHexString(chunk.length())).append("\r\n").append(chunk).append("\r\n");
        }
        body.append("0\r\n\r\n");
        RawHttp.Answer answer = RawHttp.send(port, head("/actor/worker/send", "Transfer-Encoding: chunked\r\n", -1),
                body.toString().getBytes(StandardCharsets.UTF_8), 10000);

        assertEquals(413, answer.code());
        assertEquals(List.of(), worker.calls);
    }

    @Test
    @DisplayName("a body within the limit is served")
    void bodyWithinTheLimit_isServed() throws Exception {
        server.setMaxBodyBytes(4096);
        server.start();

        RawHttp.Answer answer = RawHttp.post(port, "/actor/worker/invoke", "127.0.0.1", "application/json", null,
                RawHttp.message("worker", "run", "y".repeat(2000), null));

        assertEquals(200, answer.code());
        assertEquals(List.of("run"), worker.calls);
    }

    @Test
    @DisplayName("the default limit is 1 MiB")
    void defaultLimit_is1MiB() throws Exception {
        server.start();

        RawHttp.Answer over = RawHttp.send(port, head("/actor/worker/send", "", 1024 * 1024 + 1), null, 5000);
        assertEquals(413, over.code());
    }

    @Test
    @DisplayName("requests past the cap on requests in flight are refused with 429, and served again once one ends")
    void requestsInFlight_pastTheCap_are429() throws Exception {
        server.setMaxConcurrentRequests(2);
        server.start();
        byte[] body = RawHttp.message("worker", "block", "", null).getBytes(StandardCharsets.UTF_8);
        List<Socket> held = new ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                Socket s = new Socket("127.0.0.1", port);
                OutputStream out = s.getOutputStream();
                out.write(head("/actor/worker/invoke", "", body.length).getBytes(StandardCharsets.ISO_8859_1));
                out.write(body);
                out.flush();
                held.add(s);
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (worker.calls.isEmpty() && System.nanoTime() < deadline) Thread.sleep(20);
            Thread.sleep(300);

            RawHttp.Answer third = RawHttp.post(port, "/actor/worker/invoke", "127.0.0.1", "application/json", null,
                    RawHttp.message("worker", "run", "", null));
            assertEquals(429, third.code(), "the third request must be refused while two are held");

            worker.release.countDown();
            for (Socket s : held) {
                s.setSoTimeout(10000);
                assertTrue(RawHttp.read(s.getInputStream()).status().contains("200"));
            }
            RawHttp.Answer after = RawHttp.post(port, "/actor/worker/invoke", "127.0.0.1", "application/json", null,
                    RawHttp.message("worker", "run", "", null));
            assertEquals(200, after.code(), "served again once the held ones ended");
        } finally {
            for (Socket s : held) s.close();
        }
    }
}
