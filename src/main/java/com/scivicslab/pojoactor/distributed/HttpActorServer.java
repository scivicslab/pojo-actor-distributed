/*
 * Copyright 2025 devteam@scivicslab.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.scivicslab.pojoactor.distributed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scivicslab.pojoactor.action.ActionResult;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HTTP server that receives actor invocation requests from remote nodes and hands them to a
 * {@link Dispatcher}.
 *
 * <p>Only what the dispatcher allows can be called. Every accepted call runs through the target
 * actor's mailbox, so the actor still handles one message at a time.</p>
 *
 * <h2>Endpoints</h2>
 * <pre>
 * GET  /health                      200, no body: the server is up (needs no allow-list entry)
 *
 * POST /actor/{actorName}/invoke   runs the action and answers with its result
 *   200 {"success":true,"result":"8"}      the action ran (success says whether it worked)
 *   403 {"success":false,"result":"..."}   refused: not allowed
 *
 * POST /actor/{actorName}/send     answers as soon as the call is in the actor's mailbox
 *   202 {"accepted":true}                  the receipt: accepted, not yet run
 *   403 {"accepted":false,"result":"..."}  refused: no receipt
 * </pre>
 * The body of both is an {@link ActorMessage} as JSON. The actor name may contain slashes.
 *
 * <h2>Lifecycle</h2>
 * <pre>{@code
 * Dispatcher dispatcher = new AllowListDispatcher(actorSystem, allowedActions);
 * HttpActorServer server = new HttpActorServer(dispatcher, "127.0.0.1", port);
 * server.start();
 * // ...
 * server.close();
 * }</pre>
 *
 * @since 1.0.0
 */
public class HttpActorServer implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(HttpActorServer.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String CONTEXT_PREFIX = "/actor/";
    private static final String INVOKE = "/invoke";
    private static final String SEND = "/send";
    private static final long INVOKE_WAIT_SECONDS = 300;

    private final Dispatcher dispatcher;
    private final String bindAddress;
    private final int port;
    private HttpServer httpServer;

    /**
     * Creates an HttpActorServer that listens on every interface.
     *
     * <p>Whatever the dispatcher allows becomes callable by anything that can reach this
     * port. Use {@link #HttpActorServer(Dispatcher, String, int)} with {@code "127.0.0.1"}
     * when the caller is on the same machine, and only choose this one when the callers are
     * genuinely other hosts — a cluster of nodes that must reach each other.
     *
     * @param dispatcher decides what may be called and runs it through the actor's mailbox
     * @param port       the port to listen on
     */
    public HttpActorServer(Dispatcher dispatcher, int port) {
        this(dispatcher, null, port);
    }

    /**
     * Creates an HttpActorServer that listens on one address.
     *
     * @param dispatcher  decides what may be called and runs it through the actor's mailbox
     * @param bindAddress the address to listen on, e.g. {@code "127.0.0.1"} to accept only
     *                    callers on this machine; {@code null} listens on every interface
     * @param port        the port to listen on
     */
    public HttpActorServer(Dispatcher dispatcher, String bindAddress, int port) {
        this.dispatcher = dispatcher;
        this.bindAddress = bindAddress;
        this.port = port;
    }

    /**
     * Starts the HTTP server. Registers a wildcard handler for {@code /actor/}.
     *
     * <p>Logs the address it bound to, not only the port: whether this server accepts callers
     * from other hosts is the single most consequential thing about it, and reading it back
     * should not require inspecting the process from outside.
     *
     * @throws IOException if the server cannot bind to the address and port
     */
    public synchronized void start() throws IOException {
        InetSocketAddress address = (bindAddress == null)
                ? new InetSocketAddress(port)                 // every interface
                : new InetSocketAddress(bindAddress, port);
        httpServer = HttpServer.create(address, 0);
        httpServer.createContext(CONTEXT_PREFIX, this::handleRequest);
        httpServer.createContext("/health", this::handleHealth);
        httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        httpServer.start();
        logger.info("HttpActorServer started on " + address.getAddress().getHostAddress() + ":" + port);
    }

    @Override
    public synchronized void close() {
        if (httpServer != null) {
            httpServer.stop(1);
            logger.info("HttpActorServer stopped on port " + port);
        }
    }

    // Readiness: 200 with no body. It names no actor, so it needs no allow-list entry.
    private void handleHealth(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            exchange.sendResponseHeaders(200, -1);
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        // Path: /actor/{actorName}/invoke or /actor/{actorName}/send
        String path = exchange.getRequestURI().getPath();
        boolean send = path.endsWith(SEND);
        String actorName = extractActorName(path, send ? SEND : INVOKE);
        if (actorName == null) {
            sendResponse(exchange, 400, "{\"error\":\"Invalid path\"}");
            return;
        }

        try (InputStream body = exchange.getRequestBody()) {
            String json = new String(body.readAllBytes(), StandardCharsets.UTF_8);
            ActorMessage message = ActorMessage.fromJson(json);
            if (message == null) {
                sendResponse(exchange, 400, "{\"error\":\"Invalid message\"}");
                return;
            }

            Dispatcher.Dispatch dispatch = dispatcher.dispatch(
                    actorName, message.getActionName(), message.getArgs());

            if (send) {
                // The receipt: the call is in the mailbox. Do not wait for it to run.
                if (dispatch.accepted()) {
                    sendResponse(exchange, 202, "{\"accepted\":true}");
                } else {
                    sendResponse(exchange, 403, objectMapper.writeValueAsString(
                            new RefusedBody(false, dispatch.result().join().getResult())));
                }
                return;
            }

            ActionResult result = dispatch.result().get(INVOKE_WAIT_SECONDS, TimeUnit.SECONDS);
            String responseJson = objectMapper.writeValueAsString(
                    new ResponseBody(result.isSuccess(), result.getResult()));
            sendResponse(exchange, dispatch.accepted() ? 200 : 403, responseJson);

        } catch (Exception e) {
            logger.log(Level.WARNING, "Error handling actor request for " + actorName, e);
            sendResponse(exchange, 500, "{\"error\":\"Internal error\"}");
        }
    }

    private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    // Path: /actor/{actorName}/invoke → actorName, where actorName may itself contain slashes
    static String extractActorName(String path) {
        return extractActorName(path, INVOKE);
    }

    // Path: /actor/{actorName}{suffix} → actorName
    static String extractActorName(String path, String suffix) {
        if (path == null) return null;
        if (!path.startsWith(CONTEXT_PREFIX)) return null;
        String after = path.substring(CONTEXT_PREFIX.length());
        // The name runs to the end of the path, not to the first slash: actor names are
        // hierarchical (project1/chat-01), so a slash inside one is part of the name.
        if (!after.endsWith(suffix)) return null;
        String name = after.substring(0, after.length() - suffix.length());
        if (name.isEmpty()) return null;
        return name;
    }

    record RefusedBody(boolean accepted, String result) {}

    record ResponseBody(boolean success, String result) {}
}
