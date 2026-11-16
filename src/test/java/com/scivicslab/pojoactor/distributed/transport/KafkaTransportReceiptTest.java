package com.scivicslab.pojoactor.distributed.transport;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.ActorMessage;
import com.scivicslab.pojoactor.distributed.NodeInfo;
import com.scivicslab.pojoactor.distributed.Receipt;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/** KafkaTransport without a broker: the receipt is the broker's acknowledgement. */
class KafkaTransportReceiptTest {

    private final NodeInfo me = new NodeInfo("me", "127.0.0.1", 8080);
    private final NodeInfo target = new NodeInfo("node-b", "127.0.0.1", 8081);

    @Test
    void producerWaitsForEveryReplicaAndIsIdempotent() {
        Properties p = KafkaTransport.producerProperties("kafka:9092");
        assertEquals("all", p.get(ProducerConfig.ACKS_CONFIG).toString());
        assertEquals("true", p.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG).toString());
    }

    @Test
    void send_whenTheBrokerAcknowledges_isAcceptedAndWritesToTheInbox() {
        MockProducer<String, String> producer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        KafkaTransport transport = new KafkaTransport(me, producer);
        ActorMessage msg = new ActorMessage("worker", "run", "x");

        Receipt receipt = transport.send(target, msg);

        assertTrue(receipt.accepted());
        assertEquals(msg.getMessageId(), receipt.messageId());
        assertEquals(1, producer.history().size());
        assertEquals("pojo-actor.node-b.inbox", producer.history().get(0).topic());
        assertEquals(msg.getMessageId(), producer.history().get(0).key());
    }

    @Test
    void send_whenTheBrokerFails_isNotAcceptedAndDoesNotThrow() {
        MockProducer<String, String> producer =
                new MockProducer<>(false, new StringSerializer(), new StringSerializer());
        KafkaTransport transport = new KafkaTransport(me, producer);
        Thread failer = Thread.ofVirtual().start(() -> {
            try { Thread.sleep(100); } catch (InterruptedException e) { }
            producer.errorNext(new RuntimeException("broker down"));
        });

        Receipt receipt = assertDoesNotThrow(() -> transport.send(target, new ActorMessage("w", "a", "")));

        assertFalse(receipt.accepted());
        assertTrue(receipt.detail().contains("broker down"), receipt.detail());
    }

    /** Replies are separate sends now; there is no reply topic to wait on. */
    @Test
    void sendAndWait_isNotOfferedOverKafka() {
        MockProducer<String, String> producer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        KafkaTransport transport = new KafkaTransport(me, producer);

        ActionResult result = transport.sendAndWait(target, new ActorMessage("w", "a", ""), Duration.ofSeconds(1));

        assertFalse(result.isSuccess());
        assertTrue(result.getResult().contains("not supported"), result.getResult());
        assertTrue(producer.history().isEmpty(), "nothing is sent");
    }
}
