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

import com.scivicslab.pojoactor.core.ActionResult;
import com.scivicslab.pojoactor.core.CallableByActionName;
import com.scivicslab.pojoactor.distributed.transport.HttpTransport;
import com.scivicslab.pojoactor.distributed.transport.TransportLayer;

import java.time.Duration;
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
 * @since 3.0.0
 */
public class RemoteActorRef implements CallableByActionName {

    private static final Logger logger = Logger.getLogger(RemoteActorRef.class.getName());
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final String actorName;
    private final NodeInfo nodeInfo;
    private final TransportLayer transport;

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
        this.actorName = actorName;
        this.nodeInfo = nodeInfo;
        this.transport = transport;
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
        ActorMessage message = new ActorMessage(actorName, actionName, args);
        logger.fine(String.format("RemoteActorRef.callByActionName → node=%s actor=%s action=%s",
                nodeInfo.getAddress(), actorName, actionName));
        return transport.sendAndWait(nodeInfo, message, DEFAULT_TIMEOUT);
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
