package com.scivicslab.pojoactor.distributed.transport;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaTransport topic naming conventions.
 * No Kafka broker required.
 */
@Tag("S_load.04")
class KafkaTransportTopicNamingTest {

    @Test
    void inboxTopic_shouldFollowConvention() {
        String topic = KafkaTransport.inboxTopic("node-0");
        assertEquals("pojo-actor.node-0.inbox", topic);
    }

    @Test
    void replyTopic_shouldFollowConvention() {
        String topic = KafkaTransport.replyTopic("node-1");
        assertEquals("pojo-actor.node-1.replies", topic);
    }

    @Test
    void inboxAndReplyTopics_shouldBeDifferent() {
        String inbox = KafkaTransport.inboxTopic("node-0");
        String reply = KafkaTransport.replyTopic("node-0");
        assertNotEquals(inbox, reply);
    }

    @Test
    void topicNames_shouldHandleK8sStyleNodeIds() {
        // K8s StatefulSet pod names like "pojo-actor-0"
        String inbox = KafkaTransport.inboxTopic("pojo-actor-0");
        assertEquals("pojo-actor.pojo-actor-0.inbox", inbox);

        String reply = KafkaTransport.replyTopic("pojo-actor-2");
        assertEquals("pojo-actor.pojo-actor-2.replies", reply);
    }
}
