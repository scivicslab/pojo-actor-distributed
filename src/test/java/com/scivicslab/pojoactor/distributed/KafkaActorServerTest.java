package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Kafka receiver, tested against {@link MockConsumer}: no broker is needed. What matters is
 * the order (hand to the dispatcher, then record the position) and what happens to a message
 * that cannot be handled.
 */
class KafkaActorServerTest {

    private static final String TOPIC = KafkaTransport.inboxTopic("me");
    private final TopicPartition tp = new TopicPartition(TOPIC, 0);
    private final NodeInfo me = new NodeInfo("me", "127.0.0.1", 8080);

    private final List<String> events = new ArrayList<>();
    private MockConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                events.add("commit:" + offsets.get(tp).offset());
                super.commitSync(offsets);
            }
        };
        consumer.assign(List.of(tp));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
    }

    private void addRecord(long offset, String json) {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, "k" + offset, json));
    }

    private static String message(String actor, String action) {
        return new ActorMessage(actor, action, "args").toJson();
    }

    private Dispatcher accepting() {
        return m -> {
            events.add("dispatch:" + m.getActorName() + "/" + m.getActionName());
            return new Dispatcher.Dispatch(true,
                    CompletableFuture.completedFuture(new ActionResult(true, "ok")));
        };
    }

    @Test
    void positionIsRecordedAfterTheCallIsHandedToTheDispatcher() {
        addRecord(0, message("worker", "run"));
        KafkaActorServer server = new KafkaActorServer(accepting(), me, consumer);

        server.pollOnce(Duration.ZERO);

        assertEquals(List.of("dispatch:worker/run", "commit:1"), events);
    }

    @Test
    void consumerDoesNotCommitByItself() {
        Properties p = KafkaActorServer.consumerProperties("kafka:9092", "me");
        assertEquals("false", p.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).toString());
        assertEquals("earliest", p.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals("pojo-actor-server-me", p.get(ConsumerConfig.GROUP_ID_CONFIG));
    }

    /** A message the dispatcher refuses will never become acceptable: it must not block the inbox. */
    @Test
    void refusedMessageIsSkippedAndItsPositionRecorded() {
        addRecord(0, message("worker", "forbidden"));
        addRecord(1, message("worker", "run"));
        Dispatcher refuseFirst = m -> {
            events.add("dispatch:" + m.getActorName() + "/" + m.getActionName());
            return m.getActionName().equals("forbidden") ? Dispatcher.Dispatch.refused("Not allowed")
                    : new Dispatcher.Dispatch(true,
                            CompletableFuture.completedFuture(new ActionResult(true, "ok")));
        };
        KafkaActorServer server = new KafkaActorServer(refuseFirst, me, consumer);

        server.pollOnce(Duration.ZERO);

        assertEquals(List.of("dispatch:worker/forbidden", "dispatch:worker/run", "commit:2"), events);
    }

    @Test
    void unparseableMessageIsSkippedAndItsPositionRecorded() {
        addRecord(0, "{not json");
        addRecord(1, message("worker", "run"));
        KafkaActorServer server = new KafkaActorServer(accepting(), me, consumer);

        server.pollOnce(Duration.ZERO);

        assertEquals(List.of("dispatch:worker/run", "commit:2"), events);
    }

    /**
     * If handing a message over fails, its position must not be recorded and it must be read
     * again, otherwise a message that was never accepted is lost.
     */
    @Test
    void failureWhileHandingOver_isNotRecordedAndIsReadAgain() {
        addRecord(0, message("worker", "first"));
        addRecord(1, message("worker", "boom"));
        addRecord(2, message("worker", "third"));
        boolean[] fail = {true};
        Dispatcher flaky = m -> {
            events.add("dispatch:" + m.getActionName());
            if (m.getActionName().equals("boom") && fail[0]) throw new IllegalStateException("mailbox down");
            return new Dispatcher.Dispatch(true,
                    CompletableFuture.completedFuture(new ActionResult(true, "ok")));
        };
        KafkaActorServer server = new KafkaActorServer(flaky, me, consumer);

        server.pollOnce(Duration.ZERO);

        // "first" was handed over and recorded; "boom" failed; "third" was not touched.
        assertEquals(List.of("dispatch:first", "dispatch:boom", "commit:1"), events);
        assertEquals(1L, consumer.position(tp), "the consumer goes back to the failed message");

        // MockConsumer does not replay records after a seek, so hand the broker's copies over again.
        events.clear();
        fail[0] = false;
        addRecord(1, message("worker", "boom"));
        addRecord(2, message("worker", "third"));
        server.pollOnce(Duration.ZERO);

        assertEquals(List.of("dispatch:boom", "dispatch:third", "commit:3"), events);
    }

    @Test
    void noRecords_noCommit() {
        KafkaActorServer server = new KafkaActorServer(accepting(), me, consumer);
        server.pollOnce(Duration.ZERO);
        assertTrue(events.isEmpty());
    }

    /** The same message read twice (redelivery after a crash) reaches the actor once. */
    @Test
    void aMessageReadTwice_reachesTheDispatcherChainOnce_andBothPositionsAreRecorded() {
        ActorMessage m = new ActorMessage("worker", "run", "args");
        addRecord(0, m.toJson());
        addRecord(1, m.toJson());
        Dispatcher dedup = new DedupDispatcher(accepting(),
                new InMemoryMessageIdStore(100, Duration.ofHours(1)));
        KafkaActorServer server = new KafkaActorServer(dedup, me, consumer);

        server.pollOnce(Duration.ZERO);

        assertEquals(List.of("dispatch:worker/run", "commit:2"), events);
    }
}
