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

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.ActorMessage;
import com.scivicslab.pojoactor.distributed.NodeInfo;
import com.scivicslab.pojoactor.distributed.RemoteActorRef;
import com.scivicslab.turingworkflow.workflow.IIActorRef;
import com.scivicslab.turingworkflow.workflow.IIActorSystem;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Distributed actor system with HTTP-based inter-node communication.
 *
 * <p>This class extends {@link IIActorSystem} to provide distributed actor
 * capabilities. Each instance runs an embedded HTTP server that accepts
 * actor invocation requests from remote nodes.</p>
 *
 * <h2>Architecture</h2>
 * <pre>{@code
 * Node 1 (host1:8081)                Node 2 (host2:8082)
 * ┌────────────────────────┐         ┌────────────────────────┐
 * │ DistributedIIActorSystem │         │ DistributedIIActorSystem │
 * │ ┌────────────────────┐ │         │ ┌────────────────────┐ │
 * │ │ HttpServer :8081   │ │         │ │ HttpServer :8082   │ │
 * │ └────────────────────┘ │         │ └────────────────────┘ │
 * │ Local Actors:          │◄────────│ HTTP POST              │
 * │ - math                 │         │ Local Actors:          │
 * │ - calculator           │         │ - logger               │
 * └────────────────────────┘         └────────────────────────┘
 * }</pre>
 *
 * <h2>Usage Example</h2>
 *
 * <p><strong>Node 1 Program (ServerNode.java) - Deploy to host1:8081</strong></p>
 * <pre>{@code
 * public class ServerNode {
 *     public static void main(String[] args) throws IOException {
 *         // Create distributed actor system on this node
 *         DistributedIIActorSystem system =
 *             new DistributedIIActorSystem("node1", "0.0.0.0", 8081);
 *
 *         // Register math actor on this node
 *         MathPlugin math = new MathPlugin();
 *         MathIIAR mathActor = new MathIIAR("math", math, system);
 *         system.addIIActor(mathActor);
 *
 *         System.out.println("Server node ready on port 8081");
 *         // Keep running to accept requests
 *         Thread.currentThread().join();
 *     }
 * }
 * }</pre>
 *
 * <p><strong>Node 2 Program (ClientNode.java) - Deploy to host2:8082</strong></p>
 * <pre>{@code
 * public class ClientNode {
 *     public static void main(String[] args) throws IOException {
 *         // Create distributed actor system on this node
 *         DistributedIIActorSystem system =
 *             new DistributedIIActorSystem("node2", "0.0.0.0", 8082);
 *
 *         // Register remote node1 (must know its address)
 *         system.registerRemoteNode("node1", "host1.example.com", 8081);
 *
 *         // Get remote actor reference
 *         RemoteActorRef remoteMath = system.getRemoteActor("node1", "math");
 *
 *         // Call remote actor
 *         ActionResult result = remoteMath.callByActionName("add", "5,3");
 *         System.out.println("Result: " + result.getResult());
 *     }
 * }
 * }</pre>
 *
 * @author devteam@scivicslab.com
 * @since 2.5.0
 * @since 2.5.0
 */
public class DistributedIIActorSystem extends IIActorSystem {

    private static final Logger logger = Logger.getLogger(DistributedIIActorSystem.class.getName());

    private final String nodeId;
    private final String host;
    private final int port;
    private final HttpServer httpServer;
    private final ConcurrentHashMap<String, NodeInfo> remoteNodes;

    /**
     * Constructs a new DistributedIIActorSystem.
     *
     * @param nodeId unique identifier for this node
     * @param host hostname or IP address for this node
     * @param port HTTP port number to listen on
     * @throws IOException if HTTP server cannot be created
     */
    public DistributedIIActorSystem(String nodeId, String host, int port) throws IOException {
        super(nodeId);
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.remoteNodes = new ConcurrentHashMap<>();

        // Create and configure HTTP server
        this.httpServer = HttpServer.create(new InetSocketAddress(host, port), 0);
        setupEndpoints();
        this.httpServer.setExecutor(null); // Use default executor
        this.httpServer.start();

        logger.info(String.format("DistributedIIActorSystem '%s' started on %s:%d",
                nodeId, host, port));
    }

    /**
     * Constructs a new DistributedIIActorSystem listening on all interfaces.
     *
     * @param nodeId unique identifier for this node
     * @param port HTTP port number to listen on
     * @throws IOException if HTTP server cannot be created
     */
    public DistributedIIActorSystem(String nodeId, int port) throws IOException {
        this(nodeId, "0.0.0.0", port);
    }

    /**
     * Sets up HTTP endpoints for actor invocation.
     */
    private void setupEndpoints() {
        httpServer.createContext("/actor", this::handleActorInvocation);
        httpServer.createContext("/health", this::handleHealthCheck);
    }

    /**
     * Handles HTTP requests for actor invocation.
     *
     * <p>Expected request format:</p>
     * <pre>{@code
     * POST /actor/math/invoke
     * Content-Type: application/json
     *
     * {
     *   "actionName": "add",
     *   "args": "5,3",
     *   "messageId": "uuid-xxx"
     * }
     * }</pre>
     *
     * @param exchange the HTTP exchange
     */
    private void handleActorInvocation(HttpExchange exchange) {
        try {
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
                return;
            }

            String path = exchange.getRequestURI().getPath();
            String[] parts = path.split("/");

            // Expected: /actor/{actorName}/invoke
            if (parts.length != 4 || !"invoke".equals(parts[3])) {
                sendResponse(exchange, 400, "{\"error\":\"Invalid path format\"}");
                return;
            }

            String actorName = parts[2];

            // Read request body
            String requestBody = readRequestBody(exchange);
            ActorMessage message = ActorMessage.fromJson(requestBody);

            // Get local actor
            IIActorRef<?> actor = getIIActor(actorName);
            if (actor == null) {
                String errorJson = String.format(
                    "{\"success\":false,\"result\":\"Actor '%s' not found on node '%s'\",\"messageId\":\"%s\"}",
                    actorName, nodeId, message.getMessageId());
                sendResponse(exchange, 404, errorJson);
                return;
            }

            // Invoke actor action
            ActionResult result = actor.callByActionName(
                message.getActionName(),
                message.getArgs()
            );

            // Build response
            String responseJson = String.format(
                "{\"success\":%b,\"result\":\"%s\",\"messageId\":\"%s\"}",
                result.isSuccess(),
                escapeJson(result.getResult()),
                message.getMessageId()
            );

            sendResponse(exchange, 200, responseJson);

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error handling actor invocation", e);
            try {
                String errorJson = String.format(
                    "{\"success\":false,\"result\":\"Internal error: %s\"}",
                    escapeJson(e.getMessage())
                );
                sendResponse(exchange, 500, errorJson);
            } catch (IOException ioException) {
                logger.log(Level.SEVERE, "Error sending error response", ioException);
            }
        }
    }

    /**
     * Handles health check requests.
     *
     * @param exchange the HTTP exchange
     */
    private void handleHealthCheck(HttpExchange exchange) {
        try {
            String response = String.format(
                "{\"nodeId\":\"%s\",\"status\":\"healthy\",\"actors\":%d}",
                nodeId, getIIActorCount()
            );
            sendResponse(exchange, 200, response);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error handling health check", e);
        }
    }

    /**
     * Reads the HTTP request body as a string.
     *
     * @param exchange the HTTP exchange
     * @return the request body
     * @throws IOException if reading fails
     */
    private String readRequestBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Sends an HTTP response.
     *
     * @param exchange the HTTP exchange
     * @param statusCode the HTTP status code
     * @param response the response body
     * @throws IOException if writing fails
     */
    private void sendResponse(HttpExchange exchange, int statusCode, String response)
            throws IOException {
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    /**
     * Escapes special characters for JSON strings.
     *
     * @param str the string to escape
     * @return the escaped string
     */
    private String escapeJson(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }

    /**
     * Registers a remote node in the node registry.
     *
     * @param nodeId the remote node's identifier
     * @param host the remote node's hostname or IP
     * @param port the remote node's HTTP port
     */
    public void registerRemoteNode(String nodeId, String host, int port) {
        NodeInfo nodeInfo = new NodeInfo(nodeId, host, port);
        remoteNodes.put(nodeId, nodeInfo);
        logger.info(String.format("Registered remote node: %s at %s:%d",
                nodeId, host, port));
    }

    /**
     * Gets a remote actor reference.
     *
     * @param nodeId the node hosting the actor
     * @param actorName the name of the actor
     * @return a RemoteActorRef for the specified actor
     * @throws IllegalArgumentException if the node is not registered
     */
    public RemoteActorRef getRemoteActor(String nodeId, String actorName) {
        NodeInfo nodeInfo = remoteNodes.get(nodeId);
        if (nodeInfo == null) {
            throw new IllegalArgumentException("Remote node not registered: " + nodeId);
        }
        return new RemoteActorRef(actorName, nodeInfo);
    }

    /**
     * Returns the node identifier for this system.
     *
     * @return the node ID
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Returns the hostname for this system.
     *
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the HTTP port for this system.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * Terminates this actor system and stops the HTTP server.
     */
    @Override
    public void terminate() {
        logger.info(String.format("Terminating DistributedIIActorSystem '%s'", nodeId));
        httpServer.stop(0);
        super.terminate();
    }
}
