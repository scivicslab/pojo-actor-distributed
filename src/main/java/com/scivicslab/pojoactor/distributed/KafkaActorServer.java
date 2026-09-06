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

import com.scivicslab.pojoactor.core.ActionResult;
import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.core.CallableByActionName;
import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server-side component that receives actor messages from Kafka and dispatches
 * them to the local {@link ActorSystem}.
 *
 * <p>Consumes from {@code pojo-actor.{myNodeId}.inbox}, dispatches each message
 * to the named local actor, and — when the message has a {@code replyTo} — publishes
 * the result to {@code pojo-actor.{replyTo}.replies}.</p>
 *
 * <h2>Lifecycle</h2>
 * <pre>{@code
 * KafkaActorServer server = new KafkaActorServer(actorSystem, myNode, "kafka:9092");
 * server.start();   // begins consuming in a virtual thread
 * // ...
 * server.close();   // stops the consumer loop
 * }</pre>
 *
 * @since 3.1.0
 */
public class KafkaActorServer implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(KafkaActorServer.class.getName());

    private final ActorSystem actorSystem;
    private final NodeInfo myNode;
    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> replyProducer;
    private volatile boolean running = false;
    private Thread serverThread;

    /**
     * Creates a KafkaActorServer.
     *
     * @param actorSystem the local actor system to dispatch messages to
     * @param myNode      this node's identity (determines which inbox topic to consume)
     * @param brokers     Kafka bootstrap servers (e.g. {@code "kafka:9092"})
     */
    public KafkaActorServer(ActorSystem actorSystem, NodeInfo myNode, String brokers) {
        this.actorSystem = actorSystem;
        this.myNode = myNode;
        this.consumer = createConsumer(brokers, myNode.getNodeId());
        this.replyProducer = createProducer(brokers);
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
        if (serverThread != null) {
            serverThread.interrupt();
        }
        consumer.close(Duration.ofSeconds(5));
        replyProducer.close(Duration.ofSeconds(5));
        logger.info("KafkaActorServer stopped on node " + myNode.getNodeId());
    }

    private void consumeLoop() {
        String inboxTopic = KafkaTransport.inboxTopic(myNode.getNodeId());
        consumer.subscribe(Collections.singletonList(inboxTopic));

        while (running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    dispatch(record.value());
                }
            } catch (Exception e) {
                if (running) {
                    logger.log(Level.WARNING, "Error in KafkaActorServer consume loop", e);
                }
            }
        }
    }

    private void dispatch(String json) {
        ActorMessage message = ActorMessage.fromJson(json);
        if (message == null) {
            logger.warning("Received unparseable message: " + json);
            return;
        }

        String actorName = message.getActorName();
        CallableByActionName actor = resolveActor(actorName);
        if (actor == null) {
            logger.warning("No actor found for name: " + actorName);
            sendReply(message, new ActionResult(false, "Actor not found: " + actorName));
            return;
        }

        try {
            ActionResult result = actor.callByActionName(message.getActionName(), message.getArgs());
            sendReply(message, result);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Actor dispatch failed for " + actorName, e);
            sendReply(message, new ActionResult(false, "Dispatch error: " + e.getMessage()));
        }
    }

    private CallableByActionName resolveActor(String actorName) {
        try {
            var ref = actorSystem.getActor(actorName);
            if (ref == null) return null;
            // ActorRef wraps the POJO; access via callByActionName if supported
            Object obj = ref.ask(a -> a).join();
            if (obj instanceof CallableByActionName callable) {
                return callable;
            }
            // Fall back to action-name dispatch via ActorRef's annotation processor
            return (action, args) -> ref.ask(a -> {
                if (a instanceof CallableByActionName c) return c.callByActionName(action, args);
                return new ActionResult(false, "Actor does not implement CallableByActionName");
            }).join();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to resolve actor: " + actorName, e);
            return null;
        }
    }

    private void sendReply(ActorMessage request, ActionResult result) {
        String replyTo = request.getReplyTo();
        if (replyTo == null || replyTo.isBlank()) return;

        String replyTopic = KafkaTransport.replyTopic(replyTo);
        // Reuse messageId for correlation; args field carries the result payload
        ActorMessage reply = new ActorMessage(
                request.getActorName(),
                request.getActionName(),
                result.isSuccess() ? result.getResult() : "false:" + result.getResult(),
                request.getMessageId(),
                null
        );
        replyProducer.send(new ProducerRecord<>(replyTopic, request.getMessageId(), reply.toJson()),
                (meta, ex) -> {
                    if (ex != null) {
                        logger.log(Level.WARNING, "Failed to send reply to " + replyTopic, ex);
                    }
                });
    }

    // ---- Kafka factory methods ----

    private static KafkaConsumer<String, String> createConsumer(String brokers, String nodeId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pojo-actor-server-" + nodeId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<String, String> createProducer(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(props);
    }
}
