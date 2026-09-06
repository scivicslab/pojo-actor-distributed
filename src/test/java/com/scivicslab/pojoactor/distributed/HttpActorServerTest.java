package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.core.ActorRef;
import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.action.CallableByActionName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HttpActorServer.
 * Path parsing tests run without a network. E2E test uses loopback.
 */
@Tag("S_load.02")
class HttpActorServerTest {

    // ---- Path extraction tests (no server needed) ----

    @Test
    void extractActorName_validPath() {
        assertEquals("math", HttpActorServer.extractActorName("/actor/math/invoke"));
    }

    @Test
    void extractActorName_hyphenatedName() {
        assertEquals("order-saga", HttpActorServer.extractActorName("/actor/order-saga/invoke"));
    }

    /**
     * Actor names are hierarchical: a conversation in chat-ui-with-audit-trail is
     * {@code project1/chat-01}. The name occupies every path segment between the prefix and
     * {@code /invoke}, so the slashes inside it must not be mistaken for the boundary — otherwise
     * no actor below the root can be called from another process at all.
     */
    @Test
    void extractActorName_nameWithSlashes() {
        assertEquals("project1/chat-01",
                HttpActorServer.extractActorName("/actor/project1/chat-01/invoke"));
    }

    @Test
    void extractActorName_missingInvoke() {
        assertNull(HttpActorServer.extractActorName("/actor/math/run"));
    }

    @Test
    void extractActorName_noActorSegment() {
        assertNull(HttpActorServer.extractActorName("/actor//invoke"));
    }

    @Test
    void extractActorName_wrongPrefix() {
        assertNull(HttpActorServer.extractActorName("/other/math/invoke"));
    }

    @Test
    void extractActorName_nullPath() {
        assertNull(HttpActorServer.extractActorName(null));
    }

    // ---- E2E test: real HTTP server on loopback ----

    /** Simple actor that supports add/echo via CallableByActionName. */
    static class CalcActor implements com.scivicslab.pojoactor.action.CallableByActionName {
        @Override
        public ActionResult callByActionName(String action, String args) {
            return switch (action) {
                case "add" -> {
                    String[] parts = args.split(",");
                    int sum = Integer.parseInt(parts[0].trim()) + Integer.parseInt(parts[1].trim());
                    yield new ActionResult(true, String.valueOf(sum));
                }
                case "echo" -> new ActionResult(true, args);
                default -> new ActionResult(false, "Unknown action: " + action);
            };
        }
    }

    @Test
    void e2e_serverDispatchesRequestToLocalActor() throws Exception {
        int port = 18181;
        ActorSystem system = new ActorSystem("test");
        CalcActor calc = new CalcActor();
        ActorRef<CalcActor> ref = system.actorOf("calc", calc);

        HttpActorServer server = new HttpActorServer(system, port);
        server.start();

        try {
            HttpClient client = HttpClient.newHttpClient();
            ActorMessage msg = new ActorMessage("calc", "add", "10,32");
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/actor/calc/invoke"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(msg.toJson()))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"success\":true"));
            assertTrue(response.body().contains("\"result\":\"42\""));
        } finally {
            server.close();
            system.terminate();
        }
    }

    /**
     * An {@code ActorRef} subclass can itself know how to dispatch by action name — that is what
     * {@code IIActorRef} in Turing-workflow is, and every actor a workflow can call is one. The
     * POJO it holds knows nothing of action names, so unwrapping to it and asking there finds
     * nothing: the ref has to be asked first.
     */
    static class DispatchingRef extends ActorRef<Object> implements CallableByActionName {
        DispatchingRef(String name, ActorSystem system) {
            super(name, new Object(), system);
        }

        @Override
        public ActionResult callByActionName(String actionName, String args) {
            return new ActionResult(true, "ref handled " + actionName);
        }
    }

    @Test
    void e2e_refThatDispatchesByActionNameIsAskedItself() throws Exception {
        int port = 18183;
        ActorSystem system = new ActorSystem("test");
        system.addActor(new DispatchingRef("workflowCallable", system));

        HttpActorServer server = new HttpActorServer(system, "127.0.0.1", port);
        server.start();

        try {
            HttpClient client = HttpClient.newHttpClient();
            ActorMessage msg = new ActorMessage("workflowCallable", "step", "");
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + port + "/actor/workflowCallable/invoke"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(msg.toJson()))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"success\":true"), response.body());
            assertTrue(response.body().contains("ref handled step"), response.body());
        } finally {
            server.close();
            system.terminate();
        }
    }

    @Test
    void e2e_unknownActorReturnsFailure() throws Exception {
        int port = 18182;
        ActorSystem system = new ActorSystem("test");

        HttpActorServer server = new HttpActorServer(system, port);
        server.start();

        try {
            HttpClient client = HttpClient.newHttpClient();
            ActorMessage msg = new ActorMessage("nonexistent", "action", "args");
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/actor/nonexistent/invoke"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(msg.toJson()))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"success\":false"));
        } finally {
            server.close();
            system.terminate();
        }
    }
}
