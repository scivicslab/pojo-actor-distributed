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

import com.scivicslab.pojoactor.distributed.discovery.NodeDiscovery;
import com.scivicslab.pojoactor.distributed.discovery.NodeDiscoveryFactory;
import com.scivicslab.pojoactor.distributed.transport.HttpTransport;
import com.scivicslab.pojoactor.distributed.transport.TransportLayer;
import com.scivicslab.turingworkflow.workflow.IIActorSystem;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * An {@link IIActorSystem} whose actors can call, and be called by, actors in other processes.
 *
 * <p>This is the one distributed actor system. A workflow names a remote actor through
 * {@link RemoteActorIIAR}; a call from outside arrives at an {@link HttpActorServer} or a
 * {@link KafkaActorServer}, and both hand it to the same {@link Dispatcher}: only the
 * (actor, action) pairs on the allow-list can be called, and a call runs through the target
 * actor's mailbox.</p>
 *
 * <p>The system opens no port by itself. {@link #startHttpServer} and {@link #startServer} do,
 * and until one is called nothing outside can reach it.</p>
 *
 * <pre>{@code
 * DistributedIIActorSystem system = DistributedIIActorSystem.builder()
 *     .discovery(new StaticNodeDiscovery(me, peers))
 *     .allowedActions(Set.of(new AllowListDispatcher.AllowedAction("math", "add")))
 *     .build();
 * system.startHttpServer("127.0.0.1", port);
 * system.addIIActor(mathActor);
 * }</pre>
 *
 * @since 1.0.0
 */
public class DistributedIIActorSystem extends IIActorSystem implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(DistributedIIActorSystem.class.getName());

    private final TransportLayer transport;
    private final NodeDiscovery discovery;
    private final NodeInfo myNode;
    private final java.time.Duration callTimeout;
    private final Dispatcher dispatcher;
    private final ConcurrentHashMap<String, NodeInfo> remoteNodes = new ConcurrentHashMap<>();
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private HttpActorServer httpServer;
    private KafkaActorServer kafkaServer;

    private DistributedIIActorSystem(Builder builder) {
        super(builder.discovery.getMyNodeId());
        this.transport = builder.transport;
        this.discovery = builder.discovery;
        this.callTimeout = builder.callTimeout;
        this.myNode = new NodeInfo(discovery.getMyNodeId(), discovery.getMyHost(), discovery.getMyPort());
        Dispatcher allowList = new AllowListDispatcher(this, builder.allowedActions);
        this.dispatcher = builder.messageIds == null ? allowList
                : new DedupDispatcher(allowList, builder.messageIds);
        for (NodeInfo node : discovery.getAllNodes()) {
            if (!node.getNodeId().equals(myNode.getNodeId())) {
                remoteNodes.put(node.getNodeId(), node);
            }
        }
    }

    /**
     * Starts an {@link HttpActorServer} on every interface. Whatever the allow-list names becomes
     * callable by anything that can reach the port; see {@link #startHttpServer(String, int)}.
     *
     * @param port the port to listen on (should match {@code getMyNode().getPort()})
     * @throws IOException if the port cannot be bound
     */
    public synchronized void startHttpServer(int port) throws IOException {
        startHttpServer(null, port);
    }

    /**
     * Starts an {@link HttpActorServer} on one address.
     *
     * <p>Pass {@code "127.0.0.1"} when the callers are on this machine, and leave it open only
     * when they are other hosts. What may be called is decided by the allow-list alone.</p>
     *
     * @param bindAddress the address to listen on; {@code null} listens on every interface
     * @param port        the port to listen on (should match {@code getMyNode().getPort()})
     * @throws IOException if the address and port cannot be bound
     */
    public synchronized void startHttpServer(String bindAddress, int port) throws IOException {
        httpServer = new HttpActorServer(dispatcher, bindAddress, port);
        httpServer.start();
    }

    /**
     * Starts a {@link KafkaActorServer} that reads this node's inbox.
     *
     * @param brokers Kafka bootstrap servers
     */
    public synchronized void startServer(String brokers) {
        kafkaServer = new KafkaActorServer(dispatcher, myNode, brokers);
        kafkaServer.start();
    }

    /** Registers a node that this system can call. */
    public void registerRemoteNode(String nodeId, String host, int port) {
        remoteNodes.put(nodeId, new NodeInfo(nodeId, host, port));
        logger.info("Registered remote node: " + nodeId + " at " + host + ":" + port);
    }

    /**
     * Forgets a node: afterwards {@link #getRemoteActor} and {@link #remoteActorOf} fail for it.
     * A node that is not registered is ignored. References already handed out keep working until
     * the node stops answering; this only stops new ones from being made.
     *
     * @param nodeId the id of the node to forget
     */
    public void unregisterRemoteNode(String nodeId) {
        if (remoteNodes.remove(nodeId) != null) {
            logger.info("Unregistered remote node: " + nodeId);
        }
    }

    /**
     * Returns a reference to an actor on a registered node.
     *
     * @throws IllegalArgumentException if the node is not registered
     */
    public RemoteActorRef getRemoteActor(String nodeId, String actorName) {
        NodeInfo node = remoteNodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Remote node not registered: " + nodeId);
        }
        return new RemoteActorRef(actorName, node, transport, callTimeout);
    }

    /**
     * Returns a {@link RemoteActorRef} for an actor on the given node.
     *
     * @return a proxy that reaches the actor through the configured transport
     * @throws IllegalArgumentException if no node with that id is registered
     */
    public RemoteActorRef remoteActorOf(NodeInfo node, String actorName) {
        if (!remoteNodes.containsKey(node.getNodeId())) {
            throw new IllegalArgumentException("Remote node not registered: " + node.getNodeId());
        }
        return new RemoteActorRef(actorName, node, transport, callTimeout);
    }

    /** Returns all nodes in the cluster (including this node). */
    public List<NodeInfo> getNodes() {
        return discovery.getAllNodes();
    }

    /** Returns this node's identity. */
    public NodeInfo getMyNode() {
        return myNode;
    }

    /** Stops the servers and the transport, then terminates the actors. Safe to call twice. */
    @Override
    public void terminate() {
        if (!terminated.compareAndSet(false, true)) {
            return;
        }
        logger.info("Terminating DistributedIIActorSystem '" + myNode.getNodeId() + "'");
        synchronized (this) {
            if (kafkaServer != null) kafkaServer.close();
            if (httpServer != null) httpServer.close();
        }
        transport.close();
        super.terminate();
    }

    @Override
    public void close() {
        terminate();
    }

    /**
     * How far above an application's own HTTP port its actors are published.
     *
     * <p>1000 puts the publication ports in a band of their own: an application on 28030 publishes
     * on 29030, and no application's publication port can collide with another's HTTP port as long
     * as the HTTP ports stay within a thousand of each other.
     */
    public static final int PUBLICATION_PORT_OFFSET = 1000;

    /**
     * The port an application publishes its actors on, given the port it serves HTTP on.
     *
     * <p>This is the one definition of that number. The application that publishes and the process
     * that calls in both use it, so neither has to be configured with the other's port: they agree
     * on the one port a person can already see, and derive the rest.
     *
     * @param httpPort the port the application serves HTTP on
     * @return the port to publish actors on
     * @throws IllegalArgumentException if the result would not be a usable port
     */
    public static int publicationPortFor(int httpPort) {
        int port = httpPort + PUBLICATION_PORT_OFFSET;
        if (httpPort <= 0 || port > 65535) {
            throw new IllegalArgumentException(
                    "Cannot publish actors for an application on HTTP port " + httpPort
                    + ": " + httpPort + " + " + PUBLICATION_PORT_OFFSET + " is not a usable port");
        }
        return port;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private TransportLayer transport = new HttpTransport();
        private NodeDiscovery discovery;
        private Set<AllowListDispatcher.AllowedAction> allowedActions = Set.of();
        private MessageIdStore messageIds;
        private boolean deduplicationChosen;
        private java.time.Duration callTimeout = java.time.Duration.ofSeconds(30);

        /**
         * Sets the (actor, action) pairs that remote callers may invoke. Defaults to none:
         * nothing can be called from outside until it is listed here.
         */
        public Builder allowedActions(Set<AllowListDispatcher.AllowedAction> allowed) {
            this.allowedActions = Set.copyOf(allowed);
            return this;
        }

        /**
         * Makes the receiver drop the second and later copies of a message with the same
         * {@code messageId}, for HTTP and Kafka alike. The caller owns the store and closes it.
         *
         * <p>One of this and {@link #noDeduplication()} must be called: {@link #build()} refuses
         * to build without a choice. A sender that resends, and Kafka, which can deliver a
         * message twice, make duplicates normal, so a receiver whose actions are not safe to
         * repeat should use a store.</p>
         */
        public Builder deduplicateWith(MessageIdStore store) {
            if (store == null) {
                throw new IllegalArgumentException("store must not be null; call noDeduplication() to opt out");
            }
            if (deduplicationChosen && messageIds == null) {
                throw new IllegalStateException("noDeduplication() was already chosen");
            }
            this.messageIds = store;
            this.deduplicationChosen = true;
            return this;
        }

        /**
         * Chooses to run every copy of a message. <strong>Every duplicate will run:</strong> a
         * message sent twice, a resend after a lost receipt, and a Kafka message delivered twice
         * each run the action again. Choose this only when the actions are safe to repeat, or when
         * the sender never sends the same message twice.
         *
         * <p>One of this and {@link #deduplicateWith(MessageIdStore)} must be called.</p>
         */
        public Builder noDeduplication() {
            if (deduplicationChosen && messageIds != null) {
                throw new IllegalStateException("deduplicateWith(store) was already chosen");
            }
            this.messageIds = null;
            this.deduplicationChosen = true;
            return this;
        }

        /**
         * How long a synchronous call to a remote actor waits for its result when the caller
         * gives no wait of its own. Defaults to 30 seconds.
         */
        public Builder callTimeout(java.time.Duration timeout) {
            if (timeout == null || timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("callTimeout must be positive: " + timeout);
            }
            this.callTimeout = timeout;
            return this;
        }

        /** Sets the transport layer. Defaults to {@link HttpTransport}. */
        public Builder transport(TransportLayer transport) {
            this.transport = transport;
            return this;
        }

        /** Sets how this system finds its own identity and its peers. Defaults to auto-detection. */
        public Builder discovery(NodeDiscovery discovery) {
            this.discovery = discovery;
            return this;
        }

        public DistributedIIActorSystem build() {
            if (!deduplicationChosen) {
                throw new IllegalStateException(
                        "Choose how duplicate messages are handled: deduplicateWith(store) or noDeduplication()");
            }
            if (discovery == null) discovery = NodeDiscoveryFactory.autoDetect();
            return new DistributedIIActorSystem(this);
        }
    }
}
