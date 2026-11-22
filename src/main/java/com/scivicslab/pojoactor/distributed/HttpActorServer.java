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
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
 *
 * Both answer, when the dispatcher cannot take calls for the moment (the message-id store cannot
 * record):
 *   503 message id store unavailable       (text/plain) not run; send it again later
 *
 * Both refuse, before the dispatcher is asked:
 *   403 {"error":"..."}   an Origin header, or a Host that is not the loopback address on a loopback server
 *   415 {"error":"..."}   a Content-Type other than application/json

 * </pre>
 * The body of both is an {@link ActorMessage} as JSON. The actor name may contain slashes.
 *
 * <h2>What a request must look like</h2>
 * <p>A POST is served only when it looks like what a node sends, so that a web page in a browser on the
 * same machine cannot make the server run an action. A browser sends a cross-origin POST without asking
 * first when its type is {@code text/plain} or a form type, so the server requires
 * {@code Content-Type: application/json} (415 otherwise: a JSON type makes the browser ask first, and the
 * server never answers that question). A browser also names the page in an {@code Origin} header and a
 * node never sends one, so a request with an {@code Origin} is refused with 403. A server on the
 * loopback address refuses, with 403, a request whose {@code Host} is not the loopback address, which
 * is how a page on a name rebound to 127.0.0.1 would arrive.</p>
 *
 * <h2>Who can reach it</h2>
 * <p>The server has no TLS and no authentication. Whatever the dispatcher allows can be called by
 * anything that can open a connection to the port, and what travels (actions, arguments, results)
 * is readable on the network. So it listens on {@code 127.0.0.1} unless the program names another
 * address: {@link #HttpActorServer(Dispatcher, int)} and a {@code null} address both mean the
 * loopback address, and {@link #ALL_INTERFACES} or any other address is a choice the program makes
 * for nodes that must reach each other. A server bound to an address other than the loopback one
 * logs a warning at start-up saying so. Put such a port inside a network only those nodes can
 * reach, or behind something that provides TLS and authentication.</p>
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

    /** The address the server listens on unless the program names another. */
    private static final String LOOPBACK = "127.0.0.1";

    /** The address that listens on every interface; see {@link #HttpActorServer(Dispatcher, String, int)}. */
    public static final String ALL_INTERFACES = "0.0.0.0";

    private final Dispatcher dispatcher;
    private final String bindAddress;
    private final int port;
    private HttpServer httpServer;
    // Set in start(): whether a Host header must name the loopback address.
    private volatile boolean loopbackOnly;

    /**
     * Creates an HttpActorServer that listens on {@code 127.0.0.1}, callable from this machine only.
     *
     * <p>Use {@link #HttpActorServer(Dispatcher, String, int)} with {@link #ALL_INTERFACES} or a
     * host's address when the callers are other hosts. The server has no TLS and no
     * authentication; see the class description.</p>
     *
     * @param dispatcher decides what may be called and runs it through the actor's mailbox
     * @param port       the port to listen on
     */
    public HttpActorServer(Dispatcher dispatcher, int port) {
        this(dispatcher, LOOPBACK, port);
    }

    /**
     * Creates an HttpActorServer that listens on one address.
     *
     * <p>An address other than the loopback one makes every action the dispatcher allows callable
     * by whatever can reach that address, with no TLS and no authentication. {@link #start()}
     * logs a warning saying so.</p>
     *
     * @param dispatcher  decides what may be called and runs it through the actor's mailbox
     * @param bindAddress the address to listen on: {@code "127.0.0.1"} accepts only callers on this
     *                    machine, {@link #ALL_INTERFACES} accepts callers on every interface;
     *                    {@code null} means {@code "127.0.0.1"}
     * @param port        the port to listen on
     */
    public HttpActorServer(Dispatcher dispatcher, String bindAddress, int port) {
        this.dispatcher = dispatcher;
        this.bindAddress = bindAddress == null ? LOOPBACK : bindAddress;
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
        InetSocketAddress address = new InetSocketAddress(bindAddress, port);
        loopbackOnly = address.getAddress().isLoopbackAddress();
        httpServer = HttpServer.create(address, 0);
        httpServer.createContext(CONTEXT_PREFIX, this::handleRequest);
        httpServer.createContext("/health", this::handleHealth);
        httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        httpServer.start();
        logger.info("HttpActorServer started on " + address.getAddress().getHostAddress() + ":" + port);
        if (!address.getAddress().isLoopbackAddress()) {
            logger.warning("HttpActorServer listens on " + address.getAddress().getHostAddress() + ":" + port
                    + ", which other hosts can reach. There is no TLS and no authentication: anything that"
                    + " reaches this port can call every action the dispatcher allows, and what travels is"
                    + " readable on the network. Listen on 127.0.0.1 unless the callers are nodes on a"
                    + " network only they can reach.");
        }
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

        // What a browser page can send is refused before anything else is looked at.
        if (exchange.getRequestHeaders().containsKey("Origin")) {
            sendResponse(exchange, 403, "{\"error\":\"Cross-origin request refused\"}");
            return;
        }
        if (loopbackOnly && !isLoopbackHost(exchange.getRequestHeaders().getFirst("Host"))) {
            sendResponse(exchange, 403, "{\"error\":\"Host not accepted\"}");
            return;
        }
        if (!isJson(exchange.getRequestHeaders().getFirst("Content-Type"))) {
            sendResponse(exchange, 415, "{\"error\":\"Content-Type must be application/json\"}");
            return;
        }

        serve(exchange);
    }

    private void serve(HttpExchange exchange) throws IOException {
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

            // The name in the path decides the target; the rest comes from the body.
            Dispatcher.Dispatch dispatch = dispatcher.dispatch(new ActorMessage(
                    actorName, message.getActionName(), message.getArgs(),
                    message.getMessageId(), message.getReplyTo()));

            if (dispatch.unavailable()) {
                // Nothing was put in a mailbox; the sender may send the same message again later.
                sendResponse(exchange, 503, "text/plain; charset=utf-8", dispatch.result().join().getResult());
                return;
            }

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

    /** True for {@code application/json}, with or without parameters such as a charset. */
    static boolean isJson(String contentType) {
        if (contentType == null) return false;
        int semicolon = contentType.indexOf(';');
        String type = (semicolon < 0 ? contentType : contentType.substring(0, semicolon)).trim();
        return type.toLowerCase(Locale.ROOT).equals("application/json");
    }

    /** True when the {@code Host} header (without its port) names the loopback address of this machine. */
    static boolean isLoopbackHost(String hostHeader) {
        if (hostHeader == null) return false;
        String host = hostHeader.trim().toLowerCase(Locale.ROOT);
        if (host.startsWith("[")) {
            int close = host.indexOf(']');
            host = close < 0 ? host : host.substring(0, close + 1);
        } else {
            int colon = host.lastIndexOf(':');
            if (colon >= 0 && host.indexOf(':') == colon) host = host.substring(0, colon);
        }
        return host.equals("127.0.0.1") || host.equals("localhost") || host.equals("[::1]");
    }

    private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        sendResponse(exchange, status, "application/json", body);
    }

    private void sendResponse(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
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
