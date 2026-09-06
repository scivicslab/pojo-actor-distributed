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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.UUID;

/**
 * Message protocol for distributed actor invocation.
 *
 * <p>This class represents a serializable message that can be sent over HTTP
 * to invoke actions on remote actors. It uses JSON serialization for
 * network transmission.</p>
 *
 * <h2>Message Format</h2>
 * <pre>{@code
 * {
 *   "actorName": "math",
 *   "actionName": "add",
 *   "args": "5,3",
 *   "messageId": "550e8400-e29b-41d4-a716-446655440000",
 *   "replyTo": "node2:8082"
 * }
 * }</pre>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Create message
 * ActorMessage msg = new ActorMessage("math", "add", "5,3");
 *
 * // Serialize to JSON
 * String json = msg.toJson();
 *
 * // Deserialize from JSON
 * ActorMessage received = ActorMessage.fromJson(json);
 * }</pre>
 *
 * @author devteam@scivicslab.com
 * @since 1.0.0
 */
public class ActorMessage {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String actorName;
    private final String actionName;
    private final String args;
    private final String messageId;
    private final String replyTo;

    /**
     * Constructs a new ActorMessage with specified parameters.
     *
     * @param actorName the name of the target actor
     * @param actionName the name of the action to invoke
     * @param args the arguments for the action (string format)
     * @param messageId unique identifier for this message
     * @param replyTo optional reply address (e.g., "node2:8082")
     */
    @JsonCreator
    public ActorMessage(
            @JsonProperty("actorName") String actorName,
            @JsonProperty("actionName") String actionName,
            @JsonProperty("args") String args,
            @JsonProperty("messageId") String messageId,
            @JsonProperty("replyTo") String replyTo) {
        this.actorName = actorName;
        this.actionName = actionName;
        this.args = args;
        this.messageId = messageId != null ? messageId : UUID.randomUUID().toString();
        this.replyTo = replyTo;
    }

    /**
     * Constructs a new ActorMessage without reply address.
     *
     * @param actorName the name of the target actor
     * @param actionName the name of the action to invoke
     * @param args the arguments for the action (string format)
     */
    public ActorMessage(String actorName, String actionName, String args) {
        this(actorName, actionName, args, UUID.randomUUID().toString(), null);
    }

    /**
     * Returns the target actor name.
     *
     * @return the actor name
     */
    public String getActorName() {
        return actorName;
    }

    /**
     * Returns the action name to invoke.
     *
     * @return the action name
     */
    public String getActionName() {
        return actionName;
    }

    /**
     * Returns the action arguments.
     *
     * @return the arguments as a string
     */
    public String getArgs() {
        return args;
    }

    /**
     * Returns the unique message identifier.
     *
     * @return the message ID
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Returns the reply address.
     *
     * @return the reply address, or {@code null} if not specified
     */
    public String getReplyTo() {
        return replyTo;
    }

    /**
     * Serializes this message to JSON string.
     *
     * @return JSON representation of this message
     * @throws RuntimeException if serialization fails
     */
    public String toJson() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize ActorMessage to JSON", e);
        }
    }

    /**
     * Deserializes an ActorMessage from JSON string.
     *
     * @param json the JSON string
     * @return the deserialized ActorMessage
     * @throws RuntimeException if deserialization fails
     */
    public static ActorMessage fromJson(String json) {
        try {
            return objectMapper.readValue(json, ActorMessage.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize ActorMessage from JSON", e);
        }
    }

    @Override
    public String toString() {
        return "ActorMessage{" +
                "actorName='" + actorName + '\'' +
                ", actionName='" + actionName + '\'' +
                ", args='" + args + '\'' +
                ", messageId='" + messageId + '\'' +
                ", replyTo='" + replyTo + '\'' +
                '}';
    }
}
