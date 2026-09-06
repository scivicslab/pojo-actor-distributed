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

package com.scivicslab.pojoactor.distributed.transport;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.ActorMessage;
import com.scivicslab.pojoactor.distributed.NodeInfo;

import java.time.Duration;

/**
 * Abstraction over the network transport used for inter-node actor communication.
 *
 * <p>Two implementations are provided:</p>
 * <ul>
 *   <li>{@link HttpTransport} — direct HTTP/gRPC, suited for HPC clusters (Slurm, Grid Engine)</li>
 *   <li>{@link KafkaTransport} — Kafka-based async messaging, suited for K8s microservices</li>
 * </ul>
 */
public interface TransportLayer extends AutoCloseable {

    /**
     * Sends a message to an actor on the target node without waiting for a result (fire-and-forget).
     *
     * @param target  the destination node
     * @param message the actor message to send
     */
    void send(NodeInfo target, ActorMessage message);

    /**
     * Sends a message to an actor on the target node and waits for the result.
     *
     * @param target  the destination node
     * @param message the actor message to send (must have a non-null messageId for correlation)
     * @param timeout maximum time to wait for the response
     * @return the result from the remote actor
     */
    ActionResult sendAndWait(NodeInfo target, ActorMessage message, Duration timeout);

    /** Releases resources held by this transport (producers, consumers, threads). */
    @Override
    void close();
}
