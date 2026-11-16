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
import com.scivicslab.pojoactor.action.CallableByActionName;
import com.scivicslab.pojoactor.distributed.transport.HttpTransport;
import com.scivicslab.pojoactor.distributed.transport.TransportLayer;

import java.time.Duration;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Reference to an actor hosted on a remote node.
 *
 * <p>Implements {@link CallableByActionName} and translates method calls into
 * messages delivered via the configured {@link TransportLayer}.</p>
 *
 * <h2>Usage Example (HTTP transport)</h2>
 * <pre>{@code
 * RemoteActorRef remoteMath = new RemoteActorRef("math", nodeInfo);
 * ActionResult result = remoteMath.callByActionName("add", "5,3");
 * }</pre>
 *
 * <h2>Usage Example (Kafka transport)</h2>
 * <pre>{@code
 * KafkaTransport transport = new KafkaTransport(myNode, "kafka-broker:9092");
 * RemoteActorRef remoteMath = new RemoteActorRef("math", nodeInfo, transport);
 * ActionResult result = remoteMath.callByActionName("add", "5,3");
 * }</pre>
 *
 * @since 1.0.0
 */
public class RemoteActorRef implements CallableByActionName {

    private static final Logger logger = Logger.getLogger(RemoteActorRef.class.getName());
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final String actorName;
    private final NodeInfo nodeInfo;
    private final TransportLayer transport;
    private final Duration defaultTimeout;

    /**
     * Constructs a RemoteActorRef using the default HTTP transport.
     *
     * @param actorName the name of the remote actor
     * @param nodeInfo  information about the node hosting the actor
     */
    public RemoteActorRef(String actorName, NodeInfo nodeInfo) {
        this(actorName, nodeInfo, new HttpTransport());
    }

    /**
     * Constructs a RemoteActorRef with an explicit transport layer.
     *
     * @param actorName the name of the remote actor
     * @param nodeInfo  information about the node hosting the actor
     * @param transport the transport to use for message delivery
     */
    public RemoteActorRef(String actorName, NodeInfo nodeInfo, TransportLayer transport) {
        this(actorName, nodeInfo, transport, DEFAULT_TIMEOUT);
    }

    /**
     * Constructs a RemoteActorRef with an explicit transport and the wait that
     * {@link #callByActionName(String, String)} uses.
     *
     * @param defaultTimeout how long a synchronous call waits for the result
     */
    public RemoteActorRef(String actorName, NodeInfo nodeInfo, TransportLayer transport, Duration defaultTimeout) {
        this.actorName = actorName;
        this.nodeInfo = nodeInfo;
        this.transport = transport;
        this.defaultTimeout = defaultTimeout;
    }

    /**
     * Invokes an action on the remote actor and waits for the result.
     *
     * @param actionName the name of the action to invoke
     * @param args       the arguments for the action
     * @return the result of the action execution
     */
    @Override
    public ActionResult callByActionName(String actionName, String args) {
        return callByActionName(actionName, args, defaultTimeout);
    }

    /**
     * Invokes an action on the remote actor and waits at most {@code timeout} for the result.
     * Use this for short calls; a long piece of work is sent with {@link #send} and answered by
     * a message of its own.
     *
     * @param timeout how long to wait for the result
     */
    public ActionResult callByActionName(String actionName, String args, Duration timeout) {
        ActorMessage message = new ActorMessage(actorName, actionName, args);
        logger.fine(String.format("RemoteActorRef.callByActionName → node=%s actor=%s action=%s",
                nodeInfo.getAddress(), actorName, actionName));
        return transport.sendAndWait(nodeInfo, message, timeout);
    }

    /**
     * Sends an action to the remote actor without waiting for it to run.
     *
     * <p>The receipt says whether the remote side accepted the message into the actor's mailbox.
     * It does not say the action has run or succeeded. When the receipt is not accepted, the
     * caller may send again.</p>
     *
     * @param actionName the name of the action to invoke
     * @param args       the arguments for the action
     * @return the receipt
     */
    public Receipt send(String actionName, String args) {
        return send(actionName, args, UUID.randomUUID().toString());
    }

    /**
     * Sends with a {@code messageId} the caller chose. Sending again with the same id after a
     * receipt that did not come is safe when the receiver drops duplicates: the action runs once.
     *
     * @param messageId the id of this message; use the same one for every try of the same message
     */
    public Receipt send(String actionName, String args, String messageId) {
        ActorMessage message = new ActorMessage(actorName, actionName, args, messageId, null);
        logger.fine(String.format("RemoteActorRef.send → node=%s actor=%s action=%s",
                nodeInfo.getAddress(), actorName, actionName));
        return transport.send(nodeInfo, message);
    }

    /**
     * Sends, and sends again with the same {@code messageId} until a receipt comes or the tries
     * run out. The caller states how many tries and how long to wait between them; there is no
     * default, and the module never resends on its own.
     *
     * @param maxAttempts how many sends at most; at least 1
     * @param delay       how long to wait between two sends; not negative
     * @return the receipt of the last send: accepted, or not accepted when every try failed
     */
    public Receipt sendWithRetry(String actionName, String args, int maxAttempts, Duration delay) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1: " + maxAttempts);
        }
        if (delay == null || delay.isNegative()) {
            throw new IllegalArgumentException("delay must be given and not negative: " + delay);
        }
        String messageId = UUID.randomUUID().toString();
        Receipt receipt = send(actionName, args, messageId);
        for (int attempt = 2; attempt <= maxAttempts && !receipt.accepted(); attempt++) {
            try {
                Thread.sleep(delay.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return receipt;
            }
            logger.fine("RemoteActorRef.send again (" + attempt + "/" + maxAttempts + ") messageId=" + messageId);
            receipt = send(actionName, args, messageId);
        }
        return receipt;
    }

    public String getActorName() {
        return actorName;
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public TransportLayer getTransport() {
        return transport;
    }

    @Override
    public String toString() {
        return "RemoteActorRef{actorName='" + actorName + "', node=" + nodeInfo.getAddress() + '}';
    }
}
