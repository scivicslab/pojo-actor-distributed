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

import com.scivicslab.pojoactor.distributed.ActorMessage;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ActorMessage JSON serialization.
 */
@Tag("S_load.01")
class ActorMessageTest {

    @Test
    void testBasicSerialization() {
        ActorMessage original = new ActorMessage("math", "add", "5,3");

        String json = original.toJson();
        ActorMessage deserialized = ActorMessage.fromJson(json);

        assertEquals(original.getActorName(), deserialized.getActorName());
        assertEquals(original.getActionName(), deserialized.getActionName());
        assertEquals(original.getArgs(), deserialized.getArgs());
        assertNotNull(deserialized.getMessageId());
    }

    @Test
    void testSerializationWithAllFields() {
        ActorMessage original = new ActorMessage(
                "calculator",
                "multiply",
                "10,20",
                "test-message-id",
                "node2:8082"
        );

        String json = original.toJson();
        ActorMessage deserialized = ActorMessage.fromJson(json);

        assertEquals("calculator", deserialized.getActorName());
        assertEquals("multiply", deserialized.getActionName());
        assertEquals("10,20", deserialized.getArgs());
        assertEquals("test-message-id", deserialized.getMessageId());
        assertEquals("node2:8082", deserialized.getReplyTo());
    }

    @Test
    void testMessageIdGeneration() {
        ActorMessage msg1 = new ActorMessage("actor1", "action1", "args1");
        ActorMessage msg2 = new ActorMessage("actor1", "action1", "args1");

        assertNotNull(msg1.getMessageId());
        assertNotNull(msg2.getMessageId());
        assertNotEquals(msg1.getMessageId(), msg2.getMessageId());
    }

    @Test
    void testJsonFormat() {
        ActorMessage message = new ActorMessage(
                "testActor",
                "testAction",
                "testArgs",
                "test-id-123",
                null
        );

        String json = message.toJson();

        assertTrue(json.contains("\"actorName\":\"testActor\""));
        assertTrue(json.contains("\"actionName\":\"testAction\""));
        assertTrue(json.contains("\"args\":\"testArgs\""));
        assertTrue(json.contains("\"messageId\":\"test-id-123\""));
    }

    @Test
    void testSpecialCharactersInArgs() {
        ActorMessage original = new ActorMessage(
                "actor",
                "action",
                "arg1,arg2,arg3"
        );

        String json = original.toJson();
        ActorMessage deserialized = ActorMessage.fromJson(json);

        assertEquals("arg1,arg2,arg3", deserialized.getArgs());
    }
}
