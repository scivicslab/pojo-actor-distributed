package com.scivicslab.pojoactor.distributed;

import static com.scivicslab.pojoactor.distributed.KafkaItSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.UUID;

import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;

/**
 * Case D, against a real broker: the broker pod is deleted while a receiver runs. The StatefulSet
 * creates a new pod on the same volume. What a send during the outage gets back, whether that
 * message is delivered anyway, and whether the receiver goes on by itself are printed as
 * observations; the assertions hold the parts that must be true.
 *
 * <p>Needs the Kubernetes API (the build pod's ServiceAccount): run through {@code tasks/kafka-it.sh}.</p>
 */
@Tag("DistributedActorSystemRedesign_261115_oo01")
@DisplayName("Kafka, real broker: the broker restarts while a receiver runs")
@Timeout(600)
class KafkaBrokerRestartIT {

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
    void brokerRestart_receiverGoesOn_andAMessageSentDuringTheOutageRunsOnce() throws Exception {
        BrokerPod broker = new BrokerPod(System.getProperty("kafka.it.brokerPod", "kafka-0"));
        String b = newNodeId("d-recv");
        kafka.createInbox(b);
        RunTally tally = new RunTally();
        String id2 = UUID.randomUUID().toString();
        try (DistributedIIActorSystem receiver = kafka.startReceiver(b, tally, new InMemoryMessageIdStore(1000, Duration.ofHours(1)));
             KafkaTransport sender = new KafkaTransport(node(newNodeId("d-send")), kafka.bootstrap)) {
            RemoteActorRef remote = new RemoteActorRef(TALLY, node(b), sender);
            assertTrue(remote.send("record", "d1").accepted());
            await(Duration.ofSeconds(60), () -> tally.runs("d1") == 1, "d1 runs before the restart");
            awaitChecked(Duration.ofSeconds(30), () -> kafka.committed(b) == 1, "position 1 is committed");

            // The outage: delete the broker pod and wait until the old pod is gone.
            String oldUid = BrokerPod.uid(broker.get());
            long deleted = System.nanoTime();
            broker.delete();
            awaitChecked(Duration.ofSeconds(120), () -> !oldUid.equals(BrokerPod.uid(broker.get())),
                    "the old broker pod is gone");
            JSONObject atSend = broker.get();
            observe("case D: old pod gone after " + ms(deleted) + " ms; new pod ready at send: " + BrokerPod.ready(atSend));

            long sent = System.nanoTime();
            Receipt during = remote.send("record", "d2", id2);
            observe("case D: send during the outage returned after " + ms(sent) + " ms: " + during);

            awaitChecked(Duration.ofSeconds(300), () -> BrokerPod.ready(broker.get()), "the new broker pod is ready");
            observe("case D: new broker pod ready " + ms(deleted) + " ms after the delete");

            // Without a resend: does d2 arrive anyway (the producer may still hold it and retry)?
            long waitFrom = System.nanoTime();
            try {
                await(Duration.ofSeconds(150), () -> tally.runs("d2") >= 1, "d2 without a resend");
                observe("case D: d2 ran WITHOUT a resend, " + ms(waitFrom) + " ms after the broker was ready"
                        + " (receipt during the outage: accepted=" + during.accepted() + ")");
            } catch (AssertionError e) {
                observe("case D: d2 did not run without a resend within 150 s (receipt accepted=" + during.accepted() + ")");
            }

            // The caller's rule: no accepted receipt, send again with the same id.
            if (!during.accepted()) {
                Receipt again = during;
                for (int i = 0; i < 3 && !again.accepted(); i++) {
                    again = remote.send("record", "d2", id2);
                }
                observe("case D: resend of d2 with the same id: " + again);
            }
            assertTrue(remote.send("record", "d3").accepted(), "a send after the restart is accepted");
            await(Duration.ofSeconds(120), () -> tally.runs("d3") == 1, "the receiver reads its inbox again after the restart (d3)");
            await(Duration.ofSeconds(60), () -> tally.runs("d2") >= 1, "d2 runs");
            Thread.sleep(3000);
            observe("case D: runs " + tally.snapshot() + ", committed " + kafka.committed(b));
            assertEquals(1, tally.runs("d1"), "d1 does not run again after the restart");
            assertEquals(1, tally.runs("d2"), "d2 runs once, however many copies were sent");
            assertEquals(1, tally.runs("d3"));
        }
    }

    private static long ms(long since) {
        return Duration.ofNanos(System.nanoTime() - since).toMillis();
    }
}
