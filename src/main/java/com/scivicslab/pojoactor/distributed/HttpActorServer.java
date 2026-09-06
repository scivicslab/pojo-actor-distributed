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
import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.action.CallableByActionName;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HTTP server that receives actor invocation requests from remote nodes
 * and dispatches them to the local {@link ActorSystem}.
 *
 * <p>Completes the server side of {@link com.scivicslab.pojoactor.distributed.transport.HttpTransport},
 * enabling direct node-to-node communication for HPC clusters (Slurm, Grid Engine).</p>
 *
 * <h2>Endpoint</h2>
 * <pre>
 * POST /actor/{actorName}/invoke
 * Content-Type: application/json
 *
 * {"actorName":"math","actionName":"add","args":"5,3","messageId":"uuid"}
 *
 * Response:
 * {"success":true,"result":"8"}
 * </pre>
 *
 * <h2>Lifecycle</h2>
 * <pre>{@code
 * HttpActorServer server = new HttpActorServer(actorSystem, port);
 * server.start();
 * // ...
 * server.close();
 * }</pre>
 *
 * @since 3.1.0
 */
public class HttpActorServer implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(HttpActorServer.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String CONTEXT_PREFIX = "/actor/";
    private static final String CONTEXT_SUFFIX = "/invoke";

    private final ActorSystem actorSystem;
    private final String bindAddress;
    private final int port;
    private HttpServer httpServer;

    /**
     * Creates an HttpActorServer that listens on every interface.
     *
     * <p>Every actor in {@code actorSystem} becomes callable by anything that can reach this
     * port. Use {@link #HttpActorServer(ActorSystem, String, int)} with {@code "127.0.0.1"}
     * when the caller is on the same machine, and only choose this one when the callers are
     * genuinely other hosts — a cluster of nodes that must reach each other.
     *
     * @param actorSystem the local actor system to dispatch messages to
     * @param port        the port to listen on
     */
    public HttpActorServer(ActorSystem actorSystem, int port) {
        this(actorSystem, null, port);
    }

    /**
     * Creates an HttpActorServer that listens on one address.
     *
     * @param actorSystem the local actor system to dispatch messages to
     * @param bindAddress the address to listen on, e.g. {@code "127.0.0.1"} to accept only
     *                    callers on this machine; {@code null} listens on every interface
     * @param port        the port to listen on
     */
    public HttpActorServer(ActorSystem actorSystem, String bindAddress, int port) {
        this.actorSystem = actorSystem;
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

    private void handleRequest(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        // Extract actorName from path: /actor/{actorName}/invoke
        String path = exchange.getRequestURI().getPath();
        String actorName = extractActorName(path);
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

            ActionResult result = dispatch(actorName, message);
            String responseJson = objectMapper.writeValueAsString(
                    new ResponseBody(result.isSuccess(), result.getResult()));
            sendResponse(exchange, 200, responseJson);

        } catch (Exception e) {
            logger.log(Level.WARNING, "Error handling actor request for " + actorName, e);
            sendResponse(exchange, 500, "{\"error\":\"Internal error: " + e.getMessage() + "\"}");
        }
    }

    private ActionResult dispatch(String actorName, ActorMessage message) {
        try {
            var ref = actorSystem.getActor(actorName);
            if (ref == null) {
                return new ActionResult(false, "Actor not found: " + actorName);
            }
            // The reference itself may be what knows the action names — Turing-workflow's
            // IIActorRef is an ActorRef that dispatches by name, and every actor a workflow can
            // call is one. The object it holds is a plain POJO that knows nothing of them, so
            // unwrapping first would report every such actor as uncallable.
            if (ref instanceof CallableByActionName callableRef) {
                return callableRef.callByActionName(message.getActionName(), message.getArgs());
            }
            Object obj = ref.ask(a -> a).join();
            if (obj instanceof CallableByActionName callable) {
                return callable.callByActionName(message.getActionName(), message.getArgs());
            }
            return ref.ask(a -> {
                if (a instanceof CallableByActionName c) {
                    return c.callByActionName(message.getActionName(), message.getArgs());
                }
                return new ActionResult(false, "Actor does not implement CallableByActionName");
            }).join();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Dispatch failed for actor " + actorName, e);
            return new ActionResult(false, "Dispatch error: " + e.getMessage());
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
        if (path == null) return null;
        if (!path.startsWith(CONTEXT_PREFIX)) return null;
        String after = path.substring(CONTEXT_PREFIX.length());
        // The name runs to the end of the path, not to the first slash: actor names are
        // hierarchical (project1/chat-01), so a slash inside one is part of the name.
        if (!after.endsWith(CONTEXT_SUFFIX)) return null;
        String name = after.substring(0, after.length() - CONTEXT_SUFFIX.length());
        if (name.isEmpty()) return null;
        return name;
    }

    record ResponseBody(boolean success, String result) {}
}
