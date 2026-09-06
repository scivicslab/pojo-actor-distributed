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

import com.scivicslab.pojoactor.core.ActionResult;
import com.scivicslab.pojoactor.distributed.ActorMessage;
import com.scivicslab.pojoactor.distributed.NodeInfo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Kafka-based transport for inter-node actor communication.
 *
 * <p>Suited for K8s microservice environments where:</p>
 * <ul>
 *   <li>Messages must survive node restarts (Kafka persistence)</li>
 *   <li>Services scale dynamically</li>
 *   <li>At-least-once delivery is required</li>
 * </ul>
 *
 * <h2>Topic Naming Convention</h2>
 * <pre>
 * pojo-actor.{nodeId}.inbox   — all messages addressed to a node
 * pojo-actor.{nodeId}.replies — all reply messages from a node
 * </pre>
 *
 * <h2>tell (fire-and-forget)</h2>
 * <pre>
 * Sender: produce to pojo-actor.{targetNodeId}.inbox (replyTo = null)
 * </pre>
 *
 * <h2>ask (request-response)</h2>
 * <pre>
 * Sender: produce to pojo-actor.{targetNodeId}.inbox
 *         with messageId=UUID, replyTo={myNodeId}
 *         then block on CompletableFuture until reply arrives
 * Reply poller: poll pojo-actor.{myNodeId}.replies
 *               complete the matching Future by messageId
 * </pre>
 *
 * @since 3.1.0
 */
public class KafkaTransport implements TransportLayer {

    private static final Logger logger = Logger.getLogger(KafkaTransport.class.getName());

    static final String TOPIC_INBOX_PREFIX = "pojo-actor.";
    static final String TOPIC_INBOX_SUFFIX = ".inbox";
    static final String TOPIC_REPLIES_SUFFIX = ".replies";

    private final NodeInfo myNode;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> replyConsumer;
    private final ConcurrentHashMap<String, CompletableFuture<ActionResult>> pending = new ConcurrentHashMap<>();
    private final Thread replyPoller;
    private volatile boolean running = true;

    /**
     * Creates a KafkaTransport for the given local node.
     *
     * @param myNode  this node's identity (used as the reply topic name)
     * @param brokers Kafka bootstrap servers (e.g. {@code "kafka:9092"})
     */
    public KafkaTransport(NodeInfo myNode, String brokers) {
        this.myNode = myNode;
        this.producer = createProducer(brokers);
        this.replyConsumer = createReplyConsumer(brokers, myNode.getNodeId());
        this.replyPoller = Thread.ofVirtual()
                .name("kafka-reply-poller-" + myNode.getNodeId())
                .start(this::pollReplies);
    }

    @Override
    public void send(NodeInfo target, ActorMessage message) {
        String topic = inboxTopic(target.getNodeId());
        String json = message.toJson();
        producer.send(new ProducerRecord<>(topic, message.getMessageId(), json), (meta, ex) -> {
            if (ex != null) {
                logger.log(Level.WARNING, "Failed to send message to topic " + topic, ex);
            }
        });
    }

    @Override
    public ActionResult sendAndWait(NodeInfo target, ActorMessage message, Duration timeout) {
        // Ensure message has a replyTo so the server knows where to send the result
        ActorMessage enriched = new ActorMessage(
                message.getActorName(),
                message.getActionName(),
                message.getArgs(),
                message.getMessageId(),
                myNode.getNodeId()
        );

        CompletableFuture<ActionResult> future = new CompletableFuture<>();
        pending.put(enriched.getMessageId(), future);

        send(target, enriched);

        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            pending.remove(enriched.getMessageId());
            return new ActionResult(false, "Timeout waiting for reply from " + target.getNodeId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pending.remove(enriched.getMessageId());
            return new ActionResult(false, "Interrupted waiting for reply");
        } catch (Exception e) {
            pending.remove(enriched.getMessageId());
            return new ActionResult(false, "Error waiting for reply: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        running = false;
        replyPoller.interrupt();
        producer.close(Duration.ofSeconds(5));
        replyConsumer.close(Duration.ofSeconds(5));
        // Complete any pending futures with a failure so callers don't hang
        pending.forEach((id, future) ->
                future.complete(new ActionResult(false, "KafkaTransport closed")));
        pending.clear();
    }

    /** Background loop: polls the reply topic and completes matching futures. */
    private void pollReplies() {
        String replyTopic = replyTopic(myNode.getNodeId());
        replyConsumer.subscribe(Collections.singletonList(replyTopic));
        logger.info("KafkaTransport reply poller started, listening on " + replyTopic);

        while (running) {
            try {
                ConsumerRecords<String, String> records = replyConsumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    ActorMessage reply = ActorMessage.fromJson(record.value());
                    if (reply == null) continue;
                    CompletableFuture<ActionResult> future = pending.remove(reply.getMessageId());
                    if (future != null) {
                        boolean success = !"false".equalsIgnoreCase(reply.getArgs());
                        future.complete(new ActionResult(success, reply.getArgs()));
                    } else {
                        logger.warning("Received reply for unknown messageId: " + reply.getMessageId());
                    }
                }
            } catch (Exception e) {
                if (running) {
                    logger.log(Level.WARNING, "Error polling replies", e);
                }
            }
        }
    }

    // ---- Static helpers ----

    public static String inboxTopic(String nodeId) {
        return TOPIC_INBOX_PREFIX + nodeId + TOPIC_INBOX_SUFFIX;
    }

    public static String replyTopic(String nodeId) {
        return TOPIC_INBOX_PREFIX + nodeId + TOPIC_REPLIES_SUFFIX;
    }

    // ---- Kafka factory methods ----

    private static KafkaProducer<String, String> createProducer(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, String> createReplyConsumer(String brokers, String nodeId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pojo-actor-replies-" + nodeId + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(props);
    }
}
