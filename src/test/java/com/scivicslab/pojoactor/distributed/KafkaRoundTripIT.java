package com.scivicslab.pojoactor.distributed;

import static com.scivicslab.pojoactor.distributed.KafkaItSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.discovery.StaticNodeDiscovery;
import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;

/**
 * Case A, against a real broker: a send gets the broker's receipt and the remote action runs once;
 * an answer comes back as a send of its own to the sender's inbox; the synchronous call is refused.
 */
@Tag("DistributedActorSystemRedesign_261115_oo01")
@DisplayName("Kafka, real broker: send, receipt, one run, answer as a message")
@Timeout(120)
class KafkaRoundTripIT {

    private KafkaItSupport kafka;

    @BeforeEach
    void setUp() {
        kafka = new KafkaItSupport();
    }

    @AfterEach
    void tearDown() {
        kafka.close();
    }

    @Test
    void send_toARunningReceiver_isAccepted_andTheActionRunsOnce() throws Exception {
        String b = newNodeId("a-recv");
        kafka.createInbox(b);
        RunTally tally = new RunTally();
        try (DistributedIIActorSystem receiver = kafka.startReceiver(b, tally, new InMemoryMessageIdStore(1000, Duration.ofHours(1)));
             KafkaTransport sender = new KafkaTransport(node(newNodeId("a-send")), kafka.bootstrap)) {
            RemoteActorRef remote = new RemoteActorRef(TALLY, node(b), sender);

            Receipt receipt = remote.send("record", "a1");

            assertTrue(receipt.accepted(), "receipt: " + receipt);
            await(Duration.ofSeconds(60), () -> tally.runs("a1") == 1, "a1 runs");
            awaitChecked(Duration.ofSeconds(30), () -> kafka.committed(b) == 1, "the position after a1 is committed");
            Thread.sleep(3000);
            assertEquals(1, tally.runs("a1"), "a1 ran once");
        }
    }

    @Test
    void answer_comesBackAsASendToTheSendersInbox() throws Exception {
        String a = newNodeId("a-asker");
        String b = newNodeId("a-squarer");
        kafka.createInbox(a);
        kafka.createInbox(b);
        RunTally answers = new RunTally();
        try (DistributedIIActorSystem asker = kafka.startReceiver(a, answers, new InMemoryMessageIdStore(1000, Duration.ofHours(1)));
             DistributedIIActorSystem worker = DistributedIIActorSystem.builder()
                     .discovery(new StaticNodeDiscovery(node(b), List.of()))
                     .transport(new KafkaTransport(node(b), kafka.bootstrap))
                     .allowedActions(Set.of(new AllowListDispatcher.AllowedAction("squarer", "square")))
                     .deduplicateWith(new InMemoryMessageIdStore(1000, Duration.ofHours(1)))
                     .build();
             KafkaTransport askerOut = new KafkaTransport(node(a), kafka.bootstrap)) {
            KafkaTransport workerOut = new KafkaTransport(node(b), kafka.bootstrap);
            worker.actorOf("squarer", new Squarer(new RemoteActorRef(TALLY, node(a), workerOut), "record"));
            worker.startServer(kafka.bootstrap);

            Receipt receipt = new RemoteActorRef("squarer", node(b), askerOut).send("square", "q7,7");

            assertTrue(receipt.accepted(), "receipt: " + receipt);
            await(Duration.ofSeconds(60), () -> answers.runs("q7,49") == 1, "the answer q7,49 arrives at the asker");
            Thread.sleep(3000);
            assertEquals(java.util.Map.of("q7,49", 1), answers.snapshot(), "one answer, once");
            workerOut.close();
        }
    }

    @Test
    void sendAndWait_overKafka_isRefusedWithItsReason() throws Exception {
        try (KafkaTransport sender = new KafkaTransport(node(newNodeId("a-send")), kafka.bootstrap)) {
            RemoteActorRef remote = new RemoteActorRef(TALLY, node(newNodeId("a-nobody")), sender);

            ActionResult viaRef = remote.callByActionName("record", "x", Duration.ofSeconds(5));
            ActionResult viaTransport = sender.sendAndWait(node("a-nobody"),
                    new ActorMessage(TALLY, "record", "x", UUID.randomUUID().toString(), null), Duration.ofSeconds(5));

            assertFalse(viaRef.isSuccess());
            assertTrue(viaRef.getResult().contains("sendAndWait is not supported over Kafka"), viaRef.getResult());
            assertFalse(viaTransport.isSuccess());
            assertEquals(viaRef.getResult(), viaTransport.getResult());
        }
    }
}
