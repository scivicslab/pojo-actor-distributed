package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Over Kafka, while the message-id store cannot record, the receiver stops reading its inbox
 * (pauses) instead of reading the same message again and running it again. Tested against
 * {@link MockConsumer}: no broker is used.
 */
@Tag("DistributedActorSystemRedesign_261115_oo01")
class KafkaActorServerStoreUnavailableTest {

    private static final String TOPIC = KafkaTransport.inboxTopic("me");
    private final TopicPartition tp = new TopicPartition(TOPIC, 0);
    private final NodeInfo me = new NodeInfo("me", "127.0.0.1", 8080);
    private final List<String> events = new ArrayList<>();

    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
        @Override
        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            events.add("commit:" + offsets.get(tp).offset());
            super.commitSync(offsets);
        }
    };

    private void addRecord(long offset, String json) {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, "k" + offset, json));
    }

    @Test
    void aStoreThatCannotRecord_pausesTheInbox_untilItRecordsAgain() {
        consumer.assign(List.of(tp));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
        BreakableMessageIdStore store = new BreakableMessageIdStore();
        MessageIdStoreTest.FakeClock clock = new MessageIdStoreTest.FakeClock();
        Dispatcher running = m -> {
            events.add("run:" + m.getActionName());
            return new Dispatcher.Dispatch(true, CompletableFuture.completedFuture(new ActionResult(true, "ok")));
        };
        KafkaActorServer server = new KafkaActorServer(new DedupDispatcher(running, store, clock), me, consumer);
        String a = new ActorMessage("worker", "a", "").toJson();
        String b = new ActorMessage("worker", "b", "").toJson();

        // a runs, but its id cannot be recorded; b is not taken.
        store.broken = true;
        addRecord(0, a);
        addRecord(1, b);
        server.pollOnce(Duration.ZERO);

        assertEquals(List.of("run:a", "commit:1"), events, "a was handed over; nothing from b on is recorded");
        assertEquals(1L, consumer.position(tp), "the consumer goes back to b");
        assertEquals(Set.of(tp), consumer.paused());

        // Paused: the broker still holds b, and it is not read. MockConsumer does not replay
        // records after a seek, so hand the broker's copy over again.
        events.clear();
        addRecord(1, b);
        server.pollOnce(Duration.ZERO);
        clock.advance(Duration.ofSeconds(1));
        server.pollOnce(Duration.ZERO);             // the store is tried once more, and still fails
        server.pollOnce(Duration.ZERO);             // too soon to try again
        assertEquals(List.of(), events, "nothing runs and nothing is recorded while paused");
        assertEquals(2, store.addAttempts.get(), "the store is tried once per backoff step, not on every poll");
        assertEquals(Set.of(tp), consumer.paused());

        // The store works again: a's id is recorded, the inbox is resumed, b runs and is recorded.
        store.broken = false;
        clock.advance(Duration.ofSeconds(2));
        server.pollOnce(Duration.ZERO);

        assertEquals(Set.of(), consumer.paused());
        assertEquals(List.of("run:b", "commit:2"), events);
        assertTrue(store.contains(ActorMessage.fromJson(a).getMessageId()));
        assertTrue(store.contains(ActorMessage.fromJson(b).getMessageId()));
    }

    @Test
    void aStoreThatCannotBeRead_pausesTheInbox_untilItCanBeReadAgain() {
        consumer.assign(List.of(tp));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
        BreakableMessageIdStore store = new BreakableMessageIdStore();
        MessageIdStoreTest.FakeClock clock = new MessageIdStoreTest.FakeClock();
        Dispatcher running = m -> {
            events.add("run:" + m.getActionName());
            return new Dispatcher.Dispatch(true, CompletableFuture.completedFuture(new ActionResult(true, "ok")));
        };
        KafkaActorServer server = new KafkaActorServer(new DedupDispatcher(running, store, clock), me, consumer);
        String a = new ActorMessage("worker", "a", "").toJson();

        // a cannot be checked against the store: it is not run and its position is not recorded.
        store.containsBroken = true;
        addRecord(0, a);
        server.pollOnce(Duration.ZERO);

        assertEquals(List.of(), events);
        assertEquals(0L, consumer.position(tp), "the consumer goes back to a");
        assertEquals(Set.of(tp), consumer.paused());

        // Paused: the broker's copy of a is held, not read.
        addRecord(0, a);
        server.pollOnce(Duration.ZERO);
        assertEquals(List.of(), events, "nothing runs while paused");
        assertEquals(Set.of(tp), consumer.paused());

        // The store can be read again: the inbox is resumed, a runs once and is recorded.
        store.containsBroken = false;
        clock.advance(Duration.ofSeconds(1));
        server.pollOnce(Duration.ZERO);

        assertEquals(Set.of(), consumer.paused());
        assertEquals(List.of("run:a", "commit:1"), events);
    }
}
