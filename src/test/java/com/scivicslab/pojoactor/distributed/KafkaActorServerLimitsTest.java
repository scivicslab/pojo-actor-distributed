package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;

/**
 * A record in the inbox is not read past a size limit, the same limit as an HTTP body (chat-86, HIGH 5).
 *
 * <p>Before the limit a record of any size was parsed and, when it did not parse, written to the log in
 * full: a 4 MiB record made a 4 MiB log line.</p>
 */
@Tag("HttpActorServerLimits_261123_oo01")
@DisplayName("KafkaActorServer — a record over the size limit is skipped, not parsed or logged")
@Timeout(60)
class KafkaActorServerLimitsTest {

    private static class Capture extends Handler {
        final List<String> lines = new CopyOnWriteArrayList<>();
        @Override public void publish(LogRecord r) { lines.add(r.getMessage()); }
        @Override public void flush() { }
        @Override public void close() { }
    }

    @Test
    @DisplayName("an oversized record is skipped with a short log line, and the next record is still taken")
    void oversizedRecord_isSkipped_andTheNextOneRuns() {
        Capture capture = new Capture();
        Logger log = Logger.getLogger(KafkaActorServer.class.getName());
        log.addHandler(capture);
        try {
            NodeInfo me = new NodeInfo("n1", "127.0.0.1", 1);
            String topic = KafkaTransport.inboxTopic("n1");
            TopicPartition partition = new TopicPartition(topic, 0);
            MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
            consumer.assign(List.of(partition));
            consumer.updateBeginningOffsets(Map.of(partition, 0L));

            String oversized = "{\"actorName\":\"worker\",\"actionName\":\"run\",\"args\":\"token=sk-live-SECRET"
                    + " ".repeat(2 * 1024 * 1024) + "\"}";
            consumer.addRecord(new ConsumerRecord<>(topic, 0, 0, "k0", oversized));
            consumer.addRecord(new ConsumerRecord<>(topic, 0, 1, "k1",
                    new ActorMessage("worker", "run", "small", "m-1", null).toJson()));

            List<String> ran = new CopyOnWriteArrayList<>();
            Dispatcher dispatcher = message -> {
                ran.add(message.getArgs());
                return new Dispatcher.Dispatch(true, CompletableFuture.completedFuture(new ActionResult(true, "ok")));
            };
            new KafkaActorServer(dispatcher, me, consumer).pollOnce(Duration.ZERO);

            assertEquals(List.of("small"), ran, "only the record within the limit is handed over");
            int longest = capture.lines.stream().mapToInt(String::length).max().orElse(0);
            assertTrue(longest < 1000, "the log must not carry the record, longest line was " + longest + " chars");
            assertTrue(capture.lines.stream().noneMatch(l -> l.contains("sk-live-SECRET")));
        } finally {
            log.removeHandler(capture);
        }
    }
}
