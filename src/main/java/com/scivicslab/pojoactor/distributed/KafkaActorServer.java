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

import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server-side component that reads actor messages from the node's Kafka inbox and hands them to
 * a {@link Dispatcher}: the same allow-list and the same mailbox as {@link HttpActorServer}.
 *
 * <p>Consumes from {@code pojo-actor.{myNodeId}.inbox}. The position in the inbox is recorded
 * by hand, only after the dispatcher has taken the message (into the actor's mailbox, or
 * refused it). A receiver that dies in between reads the message again after it restarts, so a
 * message is never lost, and the same message can arrive twice.</p>
 *
 * <p>A message that cannot be parsed, or that the dispatcher refuses, is logged and skipped:
 * reading it again would not change the answer, and it would block every message behind it.
 * A message the dispatcher fails to take (an exception) is not recorded and is read again.</p>
 *
 * <p>No answer is written back. An action that has an answer to give sends it as a new message
 * to the sender's inbox.</p>
 *
 * <h2>Lifecycle</h2>
 * <pre>{@code
 * KafkaActorServer server = new KafkaActorServer(dispatcher, myNode, "kafka:9092");
 * server.start();   // begins consuming in a virtual thread
 * // ...
 * server.close();   // stops the consumer loop
 * }</pre>
 *
 * @since 1.0.0
 */
public class KafkaActorServer implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(KafkaActorServer.class.getName());

    private final Dispatcher dispatcher;
    private final NodeInfo myNode;
    private final Consumer<String, String> consumer;
    private volatile boolean running = false;
    private Thread serverThread;

    /**
     * Creates a KafkaActorServer.
     *
     * @param dispatcher decides what may be called and runs it through the actor's mailbox
     * @param myNode     this node's identity (determines which inbox topic to consume)
     * @param brokers    Kafka bootstrap servers (e.g. {@code "kafka:9092"})
     */
    public KafkaActorServer(Dispatcher dispatcher, NodeInfo myNode, String brokers) {
        this(dispatcher, myNode,
                new KafkaConsumer<>(consumerProperties(brokers, myNode.getNodeId())));
    }

    // For tests: a consumer that needs no broker.
    KafkaActorServer(Dispatcher dispatcher, NodeInfo myNode, Consumer<String, String> consumer) {
        this.dispatcher = dispatcher;
        this.myNode = myNode;
        this.consumer = consumer;
    }

    /**
     * Starts the inbox consumer loop in a virtual thread.
     * Safe to call only once.
     */
    public synchronized void start() {
        if (running) return;
        running = true;
        serverThread = Thread.ofVirtual()
                .name("kafka-actor-server-" + myNode.getNodeId())
                .start(this::consumeLoop);
        logger.info("KafkaActorServer started on node " + myNode.getNodeId()
                + ", inbox=" + KafkaTransport.inboxTopic(myNode.getNodeId()));
    }

    @Override
    public synchronized void close() {
        running = false;
        // The consumer is used by the loop thread only; wake it, let it leave, then close.
        consumer.wakeup();
        if (serverThread != null) {
            try {
                serverThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        consumer.close(Duration.ofSeconds(5));
        logger.info("KafkaActorServer stopped on node " + myNode.getNodeId());
    }

    private void consumeLoop() {
        consumer.subscribe(Collections.singletonList(KafkaTransport.inboxTopic(myNode.getNodeId())));
        while (running) {
            try {
                pollOnce(Duration.ofMillis(200));
            } catch (WakeupException e) {
                // close() asked the loop to leave
            } catch (Exception e) {
                if (running) {
                    logger.log(Level.WARNING, "Error in KafkaActorServer consume loop", e);
                }
            }
        }
    }

    /**
     * Reads one batch and hands each message to the dispatcher in order. The position is
     * recorded after the messages that were handed over; it stops at the first message the
     * dispatcher failed to take, and that message is read again by the next poll.
     */
    void pollOnce(Duration timeout) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        if (records.isEmpty()) return;

        Map<TopicPartition, OffsetAndMetadata> done = new HashMap<>();
        boolean failed = false;
        for (TopicPartition partition : records.partitions()) {
            if (failed) {
                // Not touched: go back to the first message of this partition.
                consumer.seek(partition, records.records(partition).get(0).offset());
                continue;
            }
            for (ConsumerRecord<String, String> record : records.records(partition)) {
                try {
                    handOver(record.value());
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Could not hand over message at " + partition
                            + " offset " + record.offset() + "; it will be read again", e);
                    consumer.seek(partition, record.offset());
                    failed = true;
                    break;
                }
                done.put(partition, new OffsetAndMetadata(record.offset() + 1));
            }
        }
        if (!done.isEmpty()) {
            consumer.commitSync(done);
        }
    }

    // Returns normally when the message needs no second try (taken, refused, or unparseable).
    private void handOver(String json) {
        ActorMessage message;
        try {
            message = ActorMessage.fromJson(json);
        } catch (RuntimeException e) {
            logger.warning("Skipping unparseable message: " + json);
            return;
        }
        Dispatcher.Dispatch dispatch = dispatcher.dispatch(message);
        if (!dispatch.accepted()) {
            logger.warning("Skipping refused message " + message.getMessageId() + ": "
                    + dispatch.result().join().getResult());
        }
    }

    static Properties consumerProperties(String brokers, String nodeId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pojo-actor-server-" + nodeId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // The position is recorded by hand, after the dispatcher has taken the message.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }
}
