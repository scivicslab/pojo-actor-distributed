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
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

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
 *   413 {"error":"..."}   a body over the limit
 *   415 {"error":"..."}   a Content-Type other than application/json
 *   429 {"error":"..."}   too many requests in flight
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
 * <h2>Limits</h2>
 * <p>A body larger than {@link #setMaxBodyBytes} (default {@value #DEFAULT_MAX_BODY_BYTES} bytes) is
 * refused with 413, from the declared length before any of it is read, or from the bytes received when
 * the body is chunked. At most {@link #setMaxConcurrentRequests} requests (default
 * {@value #DEFAULT_MAX_CONCURRENT_REQUESTS}) are served at once; the next gets 429 and may be sent again
 * later. Each request holds one virtual thread, and an {@code /invoke} holds it until the action ends, up
 * to {@value #INVOKE_WAIT_SECONDS} s.</p>
 *
 * <h2>Who can reach it</h2>
 * <p>On the loopback address ({@code 127.0.0.1}, the default) only callers on this machine can open a
 * connection, and the server may run without TLS. Any other address can be reached from other hosts, so
 * a server that listens on one <b>requires mutual TLS</b>: the program passes an {@link SSLContext}, the
 * server listens with TLS, and it asks every caller for a certificate that the context's trust anchors
 * accept ({@link SSLParameters#setNeedClientAuth(boolean) needClientAuth}); a caller with no certificate,
 * or with one from another CA, never gets as far as a request. {@link #start()} on an address other than
 * the loopback one, without an {@code SSLContext}, throws {@link IllegalStateException} before it listens.
 * {@link #HttpActorServer(Dispatcher, int)} and a {@code null} address mean the loopback address.
 * Passing an {@code SSLContext} makes the server use mutual TLS on the loopback address too.</p>
 *
 * <p>The library does not issue, distribute or renew certificates and keys. The program or the
 * platform does that, and builds the {@code SSLContext} from whatever it has: certificates from a
 * SPIFFE workload API (for example with java-spiffe), from cert-manager, or from key store files. The
 * context also decides which certificates are accepted; the server only requires that the caller
 * presents one.</p>
 *
 * <h2>Who is calling</h2>
 * <p>After a handshake the server reads the caller's identity from its certificate: the SPIFFE ID (a
 * {@code spiffe://} URI in the subject alternative name), otherwise the common name
 * ({@link PeerIdentity}). It hands the identity to {@link Dispatcher#dispatch(ActorMessage, CallerIdentity)},
 * so an {@link AllowListDispatcher} with a {@link CallPolicy#perCaller} policy lets each identity call
 * only the actions listed for it. A call on a server without TLS has the identity
 * {@link CallerIdentity#NONE}.</p>
 *
 *  * <h2>Lifecycle</h2>
 * <pre>{@code
 * Dispatcher dispatcher = new AllowListDispatcher(actorSystem, allowedActions);
 * HttpActorServer server = new HttpActorServer(dispatcher, "127.0.0.1", port);
 * server.start();
 * // ...
 * server.close();
 *
 * // for callers on other hosts: mutual TLS with an SSLContext the program built
 * SSLContext tls = ...;   // from SPIFFE, cert-manager or key store files
 * HttpActorServer open = new HttpActorServer(dispatcher, HttpActorServer.ALL_INTERFACES, port, tls);
 * open.start();
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

    /** The default of {@link #setMaxBodyBytes}: 1 MiB. */
    public static final long DEFAULT_MAX_BODY_BYTES = 1024 * 1024;

    /** The default of {@link #setMaxConcurrentRequests}. */
    public static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 256;

    /** The address the server listens on unless the program names another. */
    private static final String LOOPBACK = "127.0.0.1";

    /** The address that listens on every interface; which needs an SSLContext; see {@link #HttpActorServer(Dispatcher, String, int, SSLContext)}. */
    public static final String ALL_INTERFACES = "0.0.0.0";

    private final Dispatcher dispatcher;
    private final String bindAddress;
    private final int port;
    private final SSLContext sslContext;
    private HttpServer httpServer;
    private volatile long maxBodyBytes = DEFAULT_MAX_BODY_BYTES;
    private volatile Semaphore inFlight = new Semaphore(DEFAULT_MAX_CONCURRENT_REQUESTS);
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
        this(dispatcher, LOOPBACK, port, null);
    }

    /**
     * Creates an HttpActorServer that listens on one address, without TLS.
     *
     * <p>Only the loopback address can be used without TLS: {@link #start()} throws
     * {@link IllegalStateException} for any other address. Use
     * {@link #HttpActorServer(Dispatcher, String, int, SSLContext)} to listen where other hosts can
     * reach.</p>
     *
     * @param dispatcher  decides what may be called and runs it through the actor's mailbox
     * @param bindAddress the address to listen on; {@code null} means {@code "127.0.0.1"}
     * @param port        the port to listen on
     */
    public HttpActorServer(Dispatcher dispatcher, String bindAddress, int port) {
        this(dispatcher, bindAddress, port, null);
    }

    /**
     * Creates an HttpActorServer that listens on one address, with mutual TLS when an
     * {@code SSLContext} is given.
     *
     * @param dispatcher  decides what may be called and runs it through the actor's mailbox
     * @param bindAddress the address to listen on: {@code "127.0.0.1"} accepts only callers on this
     *                    machine, {@link #ALL_INTERFACES} accepts callers on every interface;
     *                    {@code null} means {@code "127.0.0.1"}
     * @param port        the port to listen on
     * @param sslContext  the TLS configuration: this server's key and certificate, and the trust anchors
     *                    that decide which callers' certificates are accepted. With it the server
     *                    listens with TLS and requires a certificate from every caller. {@code null}
     *                    means no TLS, which {@link #start()} accepts only on the loopback address.
     * @since 5.0.0
     */
    public HttpActorServer(Dispatcher dispatcher, String bindAddress, int port, SSLContext sslContext) {
        this.dispatcher = dispatcher;
        this.bindAddress = bindAddress == null ? LOOPBACK : bindAddress;
        this.port = port;
        this.sslContext = sslContext;
    }

    /**
     * Sets the largest request body the server reads; a larger one gets 413. Takes effect for the
     * requests that arrive after the call.
     *
     * @param maxBodyBytes the limit in bytes; must be positive
     */
    public void setMaxBodyBytes(long maxBodyBytes) {
        if (maxBodyBytes <= 0) {
            throw new IllegalArgumentException("maxBodyBytes must be positive: " + maxBodyBytes);
        }
        this.maxBodyBytes = maxBodyBytes;
    }

    /**
     * Sets how many requests are served at once; the next gets 429. Call it before {@link #start()}.
     *
     * @param maxConcurrentRequests the limit; must be positive
     */
    public void setMaxConcurrentRequests(int maxConcurrentRequests) {
        if (maxConcurrentRequests <= 0) {
            throw new IllegalArgumentException("maxConcurrentRequests must be positive: " + maxConcurrentRequests);
        }
        this.inFlight = new Semaphore(maxConcurrentRequests);
    }

    /**
     * Starts the HTTP server. Registers a wildcard handler for {@code /actor/}.
     *
     * <p>Logs the address it bound to, not only the port: whether this server accepts callers
     * from other hosts is the single most consequential thing about it, and reading it back
     * should not require inspecting the process from outside.
     *
     * @throws IllegalStateException if the address is not the loopback address and there is no
     *                               {@code SSLContext}: such a server would be open to other hosts without
     *                               TLS and without a check of who calls. Nothing is listening then.
     * @throws IOException if the server cannot bind to the address and port
     */
    public synchronized void start() throws IOException {
        InetSocketAddress address = new InetSocketAddress(bindAddress, port);
        if (address.isUnresolved()) {
            throw new IOException("Cannot resolve the address to listen on: " + bindAddress);
        }
        loopbackOnly = address.getAddress().isLoopbackAddress();
        if (!loopbackOnly && sslContext == null) {
            throw new IllegalStateException("HttpActorServer would listen on " + address.getAddress().getHostAddress()
                    + ":" + port + ", which other hosts can reach, without TLS. Listening anywhere but the"
                    + " loopback address requires mutual TLS: pass an SSLContext, or listen on 127.0.0.1.");
        }
        httpServer = sslContext == null ? HttpServer.create(address, 0) : httpsServer(address);
        httpServer.createContext(CONTEXT_PREFIX, this::handleRequest);
        httpServer.createContext("/health", this::handleHealth);
        httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        httpServer.start();
        logger.info("HttpActorServer started on " + address.getAddress().getHostAddress() + ":" + port
                + (sslContext == null ? " (no TLS, loopback only)" : " with mutual TLS"));
    }

    /** An HTTPS server that asks every caller for a certificate and accepts only TLS 1.3 and 1.2. */
    private HttpsServer httpsServer(InetSocketAddress address) throws IOException {
        HttpsServer https = HttpsServer.create(address, 0);
        https.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
            @Override
            public void configure(HttpsParameters parameters) {
                SSLParameters ssl = getSSLContext().getDefaultSSLParameters();
                ssl.setProtocols(new String[] {"TLSv1.3", "TLSv1.2"});
                ssl.setNeedClientAuth(true);
                parameters.setSSLParameters(ssl);
            }
        });
        return https;
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

        Semaphore slots = inFlight;
        if (!slots.tryAcquire()) {
            exchange.getResponseHeaders().set("Retry-After", "1");
            sendResponse(exchange, 429, "{\"error\":\"Too many requests\"}");
            return;
        }
        try {
            serve(exchange);
        } finally {
            slots.release();
        }
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

        long limit = maxBodyBytes;
        String declared = exchange.getRequestHeaders().getFirst("Content-Length");
        if (declared != null && parseLength(declared) > limit) {
            tooLarge(exchange);
            return;
        }

        try (InputStream body = exchange.getRequestBody()) {
            byte[] bytes = body.readNBytes((int) Math.min(limit + 1, Integer.MAX_VALUE - 8));
            if (bytes.length > limit) {
                tooLarge(exchange);
                return;
            }
            String json = new String(bytes, StandardCharsets.UTF_8);
            ActorMessage message = ActorMessage.fromJson(json);
            if (message == null) {
                sendResponse(exchange, 400, "{\"error\":\"Invalid message\"}");
                return;
            }

            // The name in the path decides the target; the rest comes from the body.
            CallerIdentity caller = exchange instanceof HttpsExchange tls
                    ? PeerIdentity.of(tls.getSSLSession()) : CallerIdentity.NONE;
            Dispatcher.Dispatch dispatch = dispatcher.dispatch(new ActorMessage(
                    actorName, message.getActionName(), message.getArgs(),
                    message.getMessageId(), message.getReplyTo()), caller);

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

    // The body is not read: the connection is closed after the answer, so the rest is never taken in.
    private void tooLarge(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("Connection", "close");
        sendResponse(exchange, 413, "{\"error\":\"Request too large\"}");
    }

    private static long parseLength(String declared) {
        try {
            return Long.parseLong(declared.trim());
        } catch (NumberFormatException e) {
            return Long.MAX_VALUE;
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
