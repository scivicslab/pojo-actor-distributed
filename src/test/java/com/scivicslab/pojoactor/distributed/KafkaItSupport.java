package com.scivicslab.pojoactor.distributed;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;

import com.scivicslab.pojoactor.distributed.discovery.StaticNodeDiscovery;
import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;

/**
 * What the Kafka integration tests share: the broker's address, fresh node names, the inbox topic
 * (one partition), the receiver's committed position, and bounded waits.
 *
 * <p>The broker's address comes from the system property {@code kafka.it.bootstrap}, which also
 * turns on the Maven profile {@code kafka-it} (see {@code tasks/kafka-it.sh}).</p>
 */
final class KafkaItSupport implements AutoCloseable {

    static final String TALLY = "tally";
    static final AllowListDispatcher.AllowedAction RECORD = new AllowListDispatcher.AllowedAction(TALLY, "record");

    final String bootstrap;
    final Admin admin;

    KafkaItSupport() {
        bootstrap = System.getProperty("kafka.it.bootstrap");
        if (bootstrap == null || bootstrap.isBlank()) {
            throw new IllegalStateException("kafka.it.bootstrap is not set; run tasks/kafka-it.sh");
        }
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        admin = Admin.create(p);
    }

    /** A node name no other test has used, so that its inbox and its group start empty. */
    static String newNodeId(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    static NodeInfo node(String nodeId) {
        return new NodeInfo(nodeId, "127.0.0.1", 0);
    }

    static String group(String nodeId) {
        return "pojo-actor-server-" + nodeId;
    }

    static TopicPartition inboxPartition(String nodeId) {
        return new TopicPartition(KafkaTransport.inboxTopic(nodeId), 0);
    }

    /** Creates the node's inbox with one partition. */
    void createInbox(String nodeId) throws Exception {
        try {
            admin.createTopics(List.of(new NewTopic(KafkaTransport.inboxTopic(nodeId), 1, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) throw e;
        }
    }

    /**
     * Builds a node that reads its inbox and has a {@link RunTally} named {@value #TALLY} whose
     * {@code record} may be called. {@code store} null means no deduplication.
     */
    DistributedIIActorSystem startReceiver(String nodeId, RunTally tally, MessageIdStore store) {
        DistributedIIActorSystem.Builder b = DistributedIIActorSystem.builder()
                .discovery(new StaticNodeDiscovery(node(nodeId), List.of()))
                .transport(new KafkaTransport(node(nodeId), bootstrap))
                .allowedActions(Set.of(RECORD));
        b = store == null ? b.noDeduplication() : b.deduplicateWith(store);
        DistributedIIActorSystem system = b.build();
        system.actorOf(TALLY, tally);
        system.startServer(bootstrap);
        return system;
    }

    /** The receiver group's committed position in the node's inbox; -1 when nothing is committed. */
    long committed(String nodeId) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(group(nodeId))
                .partitionsToOffsetAndMetadata().get();
        OffsetAndMetadata o = offsets.get(inboxPartition(nodeId));
        return o == null ? -1 : o.offset();
    }

    /**
     * Sets the receiver group's position, as if the receiver had died before recording it. The
     * group must have no member; a receiver that has just been closed may take a moment to leave.
     */
    void resetPosition(String nodeId, long offset, Duration within) throws Exception {
        long end = System.nanoTime() + within.toNanos();
        while (true) {
            try {
                admin.alterConsumerGroupOffsets(group(nodeId),
                        Map.of(inboxPartition(nodeId), new OffsetAndMetadata(offset))).all().get();
                return;
            } catch (ExecutionException e) {
                if (System.nanoTime() > end) throw e;
                Thread.sleep(500);
            }
        }
    }

    /** Waits until the condition holds; fails with the message when it does not within the time. */
    static void await(Duration within, BooleanSupplier condition, String what) throws InterruptedException {
        long end = System.nanoTime() + within.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > end) {
                throw new AssertionError("Not within " + within.toSeconds() + " s: " + what);
            }
            Thread.sleep(100);
        }
    }

    /** As {@link #await}, for a check that talks to the broker. */
    interface Check {
        boolean holds() throws Exception;
    }

    static void awaitChecked(Duration within, Check condition, String what) throws Exception {
        long end = System.nanoTime() + within.toNanos();
        while (!condition.holds()) {
            if (System.nanoTime() > end) {
                throw new AssertionError("Not within " + within.toSeconds() + " s: " + what);
            }
            Thread.sleep(200);
        }
    }

    /** Prints an observation into the test's report, for the parts the assertions do not decide. */
    static void observe(String text) {
        System.out.println("[observed] " + text);
    }

    @Override
    public void close() {
        admin.close(Duration.ofSeconds(5));
    }
}
