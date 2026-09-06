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

import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.distributed.discovery.NodeDiscovery;
import com.scivicslab.pojoactor.distributed.discovery.NodeDiscoveryFactory;
import com.scivicslab.pojoactor.distributed.transport.HttpTransport;
import com.scivicslab.pojoactor.distributed.transport.TransportLayer;

import java.util.List;

/**
 * Extends a local {@link ActorSystem} with distributed capabilities.
 *
 * <p>Combines a {@link TransportLayer} (HTTP or Kafka) with a {@link NodeDiscovery}
 * strategy to enable actors to communicate across nodes with the same API as local actors.</p>
 *
 * <h2>K8s / Kafka Example</h2>
 * <pre>{@code
 * NodeDiscovery discovery = NodeDiscoveryFactory.autoDetect();
 * KafkaTransport transport = new KafkaTransport(
 *     new NodeInfo(discovery.getMyNodeId(), discovery.getMyHost(), discovery.getMyPort()),
 *     "kafka:9092"
 * );
 *
 * DistributedActorSystem system = DistributedActorSystem.builder()
 *     .localActorSystem(new ActorSystem("my-app"))
 *     .transport(transport)
 *     .discovery(discovery)
 *     .build();
 *
 * system.startServer("kafka:9092"); // start receiving messages
 *
 * // Get a reference to a remote actor
 * RemoteActorRef remote = system.remoteActorOf(system.getNodes().get(1), "order-saga");
 * ActionResult result = remote.callByActionName("start", orderJson);
 * }</pre>
 *
 * <h2>Slurm / HTTP Example</h2>
 * <pre>{@code
 * DistributedActorSystem system = DistributedActorSystem.builder()
 *     .localActorSystem(new ActorSystem("slurm-worker"))
 *     .transport(new HttpTransport())
 *     .discovery(NodeDiscoveryFactory.autoDetect())
 *     .build();
 * }</pre>
 *
 * @since 3.1.0
 */
public class DistributedActorSystem implements AutoCloseable {

    private final ActorSystem localActorSystem;
    private final TransportLayer transport;
    private final NodeDiscovery discovery;
    private final NodeInfo myNode;
    private KafkaActorServer kafkaServer;
    private HttpActorServer httpServer;

    private DistributedActorSystem(Builder builder) {
        this.localActorSystem = builder.localActorSystem;
        this.transport = builder.transport;
        this.discovery = builder.discovery;
        this.myNode = new NodeInfo(
                discovery.getMyNodeId(),
                discovery.getMyHost(),
                discovery.getMyPort()
        );
    }

    /**
     * Starts a {@link KafkaActorServer} to receive messages from Kafka.
     * Only needed when using {@link com.scivicslab.pojoactor.distributed.transport.KafkaTransport}.
     *
     * @param brokers Kafka bootstrap servers
     */
    public void startServer(String brokers) {
        kafkaServer = new KafkaActorServer(localActorSystem, myNode, brokers);
        kafkaServer.start();
    }

    /**
     * Starts an {@link HttpActorServer} to receive messages via HTTP.
     * Suited for HPC clusters (Slurm, Grid Engine) using {@link com.scivicslab.pojoactor.distributed.transport.HttpTransport}.
     *
     * @param port the port to listen on (should match {@code myNode.getPort()})
     * @throws java.io.IOException if the port cannot be bound
     */
    public void startHttpServer(int port) throws java.io.IOException {
        startHttpServer(null, port);
    }

    /**
     * Starts the embedded HTTP server on one address.
     *
     * <p>Every actor in the local system becomes callable by anything that can reach the
     * address and port, with whatever those actors can do. Pass {@code "127.0.0.1"} when the
     * callers are on this machine, and leave it open only when they are genuinely other hosts.
     *
     * @param bindAddress the address to listen on, e.g. {@code "127.0.0.1"}; {@code null}
     *                    listens on every interface
     * @param port        the port to listen on (should match {@code myNode.getPort()})
     * @throws java.io.IOException if the address and port cannot be bound
     */
    public void startHttpServer(String bindAddress, int port) throws java.io.IOException {
        httpServer = new HttpActorServer(localActorSystem, bindAddress, port);
        httpServer.start();
    }

    /**
     * Returns a {@link RemoteActorRef} for an actor on the given remote node.
     *
     * @param node      the remote node
     * @param actorName the name of the remote actor
     * @return a proxy that delegates calls via the configured transport
     */
    public RemoteActorRef remoteActorOf(NodeInfo node, String actorName) {
        return new RemoteActorRef(actorName, node, transport);
    }

    /** Returns all nodes in the cluster (including this node). */
    public List<NodeInfo> getNodes() {
        return discovery.getAllNodes();
    }

    /** Returns this node's identity. */
    public NodeInfo getMyNode() {
        return myNode;
    }

    /** Returns the underlying local actor system. */
    public ActorSystem getLocalActorSystem() {
        return localActorSystem;
    }

    @Override
    public void close() {
        if (kafkaServer != null) {
            kafkaServer.close();
        }
        if (httpServer != null) {
            httpServer.close();
        }
        transport.close();
        localActorSystem.terminate();
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
     * <p>Deriving rather than configuring matters when something else assigns the HTTP port — a
     * launcher that hands out ports from a range gives a different one on each start, and a
     * configured publication port would have to be corrected every time.
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

    // ---- Builder ----

    public static class Builder {

        private ActorSystem localActorSystem;
        private TransportLayer transport = new HttpTransport();
        private NodeDiscovery discovery;

        /** Sets the local actor system. Required. */
        public Builder localActorSystem(ActorSystem system) {
            this.localActorSystem = system;
            return this;
        }

        /** Sets the transport layer. Defaults to {@link HttpTransport}. */
        public Builder transport(TransportLayer transport) {
            this.transport = transport;
            return this;
        }

        /** Sets the node discovery strategy. Required. */
        public Builder discovery(NodeDiscovery discovery) {
            this.discovery = discovery;
            return this;
        }

        public DistributedActorSystem build() {
            if (localActorSystem == null) throw new IllegalStateException("localActorSystem is required");
            if (discovery == null) discovery = NodeDiscoveryFactory.autoDetect();
            return new DistributedActorSystem(this);
        }
    }
}
