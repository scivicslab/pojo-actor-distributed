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
import java.util.Set;
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
 * A message the dispatcher fails to take (an exception or an Error) is not recorded and is read
 * again. The consume loop keeps running through such failures and through a failed subscribe;
 * only a {@link VirtualMachineError} other than {@link StackOverflowError} ends it.</p>
 *
 * <p>When the dispatcher answers {@link Dispatcher.Dispatch.Outcome#UNAVAILABLE} (the message-id
 * store cannot record), the message is not taken: the consumer goes back to it, records no
 * position from it on, and pauses every assigned partition. It keeps polling while paused (the
 * polls return nothing) and asks {@link Dispatcher#recover()} at the start of each turn; the
 * dispatcher decides how often it really tries (see {@link DedupDispatcher}) and logs each failed
 * try. When it recovers, the partitions are resumed and the message is read again.</p>
 *
 * <p>No answer is written back. An action that has an answer to give sends it as a new message
 * to the sender's inbox.</p>
 *
 * <h2>Tests</h2>
 * <p>The unit tests use Kafka's {@code MockConsumer}. The integration tests ({@code *IT.java},
 * run by {@code tasks/kafka-it.sh} against a single Kafka broker on k8s, one partition per inbox)
 * check against a real broker: a stopped receiver resumes from its committed position, the inbox
 * pauses while the message-id store fails and resumes by itself, the receiver goes on after the
 * broker restarts, and a redelivered message is dropped by a {@link FileMessageIdStore} that
 * survived the restart. Not tested: inboxes with more than one partition (and so the order of
 * messages across partitions), a cluster of several brokers, and TLS or authentication.</p>
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
    private volatile int maxMessageChars = (int) HttpActorServer.DEFAULT_MAX_BODY_BYTES;
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
        this(dispatcher, myNode, brokers, null);
    }

    /**
     * Creates a KafkaActorServer with the program's own Kafka client settings.
     *
     * <p>The settings reach the Kafka consumer as they are: {@code security.protocol},
     * {@code ssl.*}, {@code sasl.*} and the like. A message read from the inbox has no caller
     * identity ({@link CallerIdentity#NONE}); who may write to the inbox is decided by the broker's
     * access control, not by this server.</p>
     *
     * @param dispatcher       decides what may be called and runs it through the actor's mailbox
     * @param myNode           this node's identity (determines which inbox topic to consume)
     * @param brokers          Kafka bootstrap servers
     * @param clientProperties the program's Kafka consumer settings; may be null. The settings this
     *                         server depends on (group, starting position, manual commit, deserialisers)
     *                         cannot be changed: setting one to another value throws
     *                         {@link IllegalArgumentException}
     * @since 5.0.0
     */
    public KafkaActorServer(Dispatcher dispatcher, NodeInfo myNode, String brokers, Properties clientProperties) {
        this(dispatcher, myNode,
                new KafkaConsumer<>(consumerProperties(brokers, myNode.getNodeId(), clientProperties)));
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
    /**
     * Sets the longest message, in characters, that is read from the inbox; a longer one is skipped and
     * logged by its length only. The default is {@link HttpActorServer#DEFAULT_MAX_BODY_BYTES}.
     *
     * @param maxMessageChars the limit; must be positive
     */
    public void setMaxMessageChars(int maxMessageChars) {
        if (maxMessageChars <= 0) {
            throw new IllegalArgumentException("maxMessageChars must be positive: " + maxMessageChars);
        }
        this.maxMessageChars = maxMessageChars;
    }

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

    // While running is true, senders get receipts, so the loop must not end by itself: anything
    // thrown is reported and the next turn tries again (a failed subscribe is repeated; a message
    // whose position was not recorded is read again). Only a VirtualMachineError other than
    // StackOverflowError ends the loop: it is reported, then left to the JVM.
    private void consumeLoop() {
        boolean subscribed = false;
        while (running) {
            try {
                if (!subscribed) {
                    consumer.subscribe(Collections.singletonList(
                            KafkaTransport.inboxTopic(myNode.getNodeId())));
                    subscribed = true;
                }
                pollOnce(Duration.ofMillis(200));
            } catch (WakeupException e) {
                // close() asked the loop to leave
            } catch (VirtualMachineError e) {
                if (!(e instanceof StackOverflowError)) {
                    logger.log(Level.SEVERE, "KafkaActorServer consume loop stops on node "
                            + myNode.getNodeId(), e);
                    throw e;
                }
                if (!reportAndPause(e)) return;
            } catch (Throwable e) {
                if (!reportAndPause(e)) return;
            }
        }
    }

    // Returns false when the loop thread was interrupted while pausing.
    private boolean reportAndPause(Throwable e) {
        if (!running) return true;
        logger.log(Level.SEVERE, "Error in KafkaActorServer consume loop on node "
                + myNode.getNodeId() + "; trying again", e);
        try {
            // Without a pause, a failure that repeats (a broker that is down) fills the log.
            Thread.sleep(200);
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Reads one batch and hands each message to the dispatcher in order. The position is
     * recorded after the messages that were handed over; it stops at the first message the
     * dispatcher failed to take, and that message is read again by the next poll.
     */
    void pollOnce(Duration timeout) {
        Set<TopicPartition> paused = consumer.paused();
        if (!paused.isEmpty() && dispatcher.recover()) {
            consumer.resume(paused);
            logger.info("KafkaActorServer on node " + myNode.getNodeId() + " reads its inbox again");
        }
        // A paused consumer still polls: the poll returns nothing from a paused partition, and
        // it keeps this consumer in its group.
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        if (records.isEmpty()) return;

        Map<TopicPartition, OffsetAndMetadata> done = new HashMap<>();
        boolean failed = false;
        boolean unavailable = false;
        Error error = null;
        for (TopicPartition partition : records.partitions()) {
            if (failed) {
                // Not touched: go back to the first message of this partition.
                consumer.seek(partition, records.records(partition).get(0).offset());
                continue;
            }
            for (ConsumerRecord<String, String> record : records.records(partition)) {
                try {
                    if (!handOver(record.value())) {
                        // Not taken, and must not be taken by a later poll before the
                        // dispatcher can take calls again: go back to it and stop reading.
                        consumer.seek(partition, record.offset());
                        unavailable = true;
                        failed = true;
                        break;
                    }
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Could not hand over message at " + partition
                            + " offset " + record.offset() + "; it will be read again", e);
                    consumer.seek(partition, record.offset());
                    failed = true;
                    break;
                } catch (Error e) {
                    // Go back and record what was done, as for an exception, then let the
                    // consume loop decide what an Error means.
                    consumer.seek(partition, record.offset());
                    error = e;
                    failed = true;
                    break;
                }
                done.put(partition, new OffsetAndMetadata(record.offset() + 1));
            }
        }
        if (!done.isEmpty()) {
            consumer.commitSync(done);
        }
        if (unavailable) {
            consumer.pause(consumer.assignment());
            logger.severe("KafkaActorServer on node " + myNode.getNodeId() + " stops reading its inbox:"
                    + " the dispatcher cannot take calls for the moment; it reads again when the"
                    + " dispatcher recovers");
        }
        if (error != null) {
            throw error;
        }
    }

    // Returns true when the message needs no second try (taken, refused, or unparseable), false
    // when the dispatcher cannot take calls for the moment. Throws when handing over failed.
    private boolean handOver(String json) {
        if (json != null && json.length() > maxMessageChars) {
            // Neither parsed nor logged: a record this size is not a message a node sends.
            logger.warning("Skipping a message of " + json.length() + " characters, over the limit of "
                    + maxMessageChars);
            return true;
        }
        ActorMessage message;
        try {
            message = ActorMessage.fromJson(json);
        } catch (RuntimeException e) {
            logger.warning("Skipping unparseable message: " + json);
            return true;
        }
        // The JSON text "null" parses without error to no message; it is no more usable.
        if (message == null) {
            logger.warning("Skipping unparseable message: " + json);
            return true;
        }
        Dispatcher.Dispatch dispatch = dispatcher.dispatch(message);
        if (dispatch.unavailable()) {
            return false;
        }
        if (!dispatch.accepted()) {
            logger.warning("Skipping refused message " + message.getMessageId() + ": "
                    + dispatch.result().join().getResult());
        }
        return true;
    }

    static Properties consumerProperties(String brokers, String nodeId) {
        return consumerProperties(brokers, nodeId, null);
    }

    static Properties consumerProperties(String brokers, String nodeId, Properties fromProgram) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pojo-actor-server-" + nodeId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // The position is recorded by hand, after the dispatcher has taken the message.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return com.scivicslab.pojoactor.distributed.transport.KafkaClientSettings.merge(props, fromProgram);
    }
}
