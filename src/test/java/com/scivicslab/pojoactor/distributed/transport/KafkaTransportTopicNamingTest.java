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
    void topicNames_shouldHandleK8sStyleNodeIds() {
        // K8s StatefulSet pod names like "pojo-actor-0"
        String inbox = KafkaTransport.inboxTopic("pojo-actor-0");
        assertEquals("pojo-actor.pojo-actor-0.inbox", inbox);
    }
}
