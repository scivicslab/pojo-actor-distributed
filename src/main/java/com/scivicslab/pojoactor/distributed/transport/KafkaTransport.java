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

package com.scivicslab.pojoactor.distributed.transport;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.ActorMessage;
import com.scivicslab.pojoactor.distributed.NodeInfo;
import com.scivicslab.pojoactor.distributed.Receipt;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Kafka-based transport for inter-node actor communication.
 *
 * <p>A message is written to the inbox topic of the receiving node and stays there until the
 * receiver reads it, so it survives a receiver that is down. Delivery is at least once: the same
 * message can arrive twice, and the receiver has to drop the second copy by {@code messageId}.</p>
 *
 * <h2>Topic naming</h2>
 * <pre>
 * pojo-actor.{nodeId}.inbox   — all messages addressed to a node
 * </pre>
 *
 * <h2>Receipt</h2>
 * <p>{@link #send} returns when the broker has acknowledged the write ({@code acks=all},
 * idempotent producer). That means the message is stored in the inbox, not that the remote actor
 * has taken it into its mailbox: the receiver does that when it reads the inbox.</p>
 *
 * <p>A receipt that is not accepted does not mean the message was not stored. When the broker does
 * not answer within the 30 s that {@link #send} waits, the receipt is not accepted, but the producer
 * keeps the message and goes on trying to deliver it. Against a real broker that was restarted,
 * such a message was delivered after the broker came back. A sender that sends again after a
 * receipt that was not accepted must use the same {@code messageId}, and the receiver must drop
 * duplicates.</p>
 *
 * <h2>Tests</h2>
 * <p>The unit tests use Kafka's {@code MockProducer}. The integration tests ({@code *IT.java}, run
 * by {@code tasks/kafka-it.sh}) send through a real broker on k8s; see {@code KafkaActorServer}
 * for what they check and what they do not.</p>
 *
 * <h2>No synchronous call</h2>
 * <p>{@link #sendAndWait} is not offered. A long request is sent with {@link #send}; when the
 * work is done, the receiving actor sends the answer back as another message to the sender's
 * inbox.</p>
 *
 * @since 1.0.0
 */
public class KafkaTransport implements TransportLayer {

    private static final Logger logger = Logger.getLogger(KafkaTransport.class.getName());

    static final String TOPIC_INBOX_PREFIX = "pojo-actor.";
    static final String TOPIC_INBOX_SUFFIX = ".inbox";

    private final NodeInfo myNode;
    private final Producer<String, String> producer;

    /**
     * Creates a KafkaTransport for the given local node.
     *
     * @param myNode  this node's identity
     * @param brokers Kafka bootstrap servers (e.g. {@code "kafka:9092"})
     */
    public KafkaTransport(NodeInfo myNode, String brokers) {
        this(myNode, brokers, null);
    }

    /**
     * Creates a KafkaTransport with the program's own Kafka client settings.
     *
     * <p>The settings reach the Kafka producer as they are: {@code security.protocol},
     * {@code ssl.*}, {@code sasl.*} and the like. The library does not secure the connection to the
     * brokers by itself; the program and the platform decide that and pass the settings here.</p>
     *
     * @param myNode           this node's identity
     * @param brokers          Kafka bootstrap servers
     * @param clientProperties the program's Kafka producer settings; may be null. The settings that the
     *                         receipt depends on (acknowledgement by every in-sync replica, idempotence,
     *                         serialisers) cannot be changed: setting one to another value throws
     *                         {@link IllegalArgumentException}
     * @since 5.0.0
     */
    public KafkaTransport(NodeInfo myNode, String brokers, java.util.Properties clientProperties) {
        this(myNode, new KafkaProducer<>(producerProperties(brokers, clientProperties)));
    }

    // For tests: a producer that needs no broker.
    KafkaTransport(NodeInfo myNode, Producer<String, String> producer) {
        this.myNode = myNode;
        this.producer = producer;
    }

    /**
     * Writes the message to the target's inbox and returns the broker's acknowledgement.
     * A failure gives a receipt that is not accepted; nothing is thrown.
     */
    @Override
    public Receipt send(NodeInfo target, ActorMessage message) {
        String topic = inboxTopic(target.getNodeId());
        try {
            producer.send(new ProducerRecord<>(topic, message.getMessageId(), message.toJson()))
                    .get(30, TimeUnit.SECONDS);
            return Receipt.accepted(message.getMessageId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Receipt.refused(message.getMessageId(), "Interrupted");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to send message to topic " + topic, e);
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            return Receipt.refused(message.getMessageId(), "Kafka error: " + cause.getMessage());
        }
    }

    /** Not offered over Kafka: send the request with {@link #send} and receive the answer as a message. */
    @Override
    public ActionResult sendAndWait(NodeInfo target, ActorMessage message, Duration timeout) {
        return new ActionResult(false,
                "sendAndWait is not supported over Kafka; use send and receive the answer as a message");
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(5));
    }

    // ---- Static helpers ----

    public static String inboxTopic(String nodeId) {
        return TOPIC_INBOX_PREFIX + nodeId + TOPIC_INBOX_SUFFIX;
    }

    static Properties producerProperties(String brokers) {
        return producerProperties(brokers, null);
    }

    static Properties producerProperties(String brokers, Properties fromProgram) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // A write is acknowledged only when every in-sync replica has it, and a retry does not
        // write the message twice.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return KafkaClientSettings.merge(props, fromProgram);
    }
}
