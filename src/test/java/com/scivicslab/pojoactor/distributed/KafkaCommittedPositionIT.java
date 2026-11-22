package com.scivicslab.pojoactor.distributed;

import static com.scivicslab.pojoactor.distributed.KafkaItSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;

/**
 * Case B, against a real broker: a receiver that is stopped and started again (same node, same
 * group) goes on from its committed position. The messages sent while it was down run once, and
 * the ones it had already handled do not run again. No deduplication, so every redelivery would
 * show as a second run.
 */
@Tag("DistributedActorSystemRedesign_261115_oo01")
@DisplayName("Kafka, real broker: a restarted receiver resumes from its committed position")
@Timeout(180)
class KafkaCommittedPositionIT {

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
    void restartedReceiver_runsWhatCameWhileItWasDown_once_andNothingAgain() throws Exception {
        String b = newNodeId("b-recv");
        kafka.createInbox(b);
        try (KafkaTransport sender = new KafkaTransport(node(newNodeId("b-send")), kafka.bootstrap)) {
            RemoteActorRef remote = new RemoteActorRef(TALLY, node(b), sender);

            RunTally before = new RunTally();
            try (DistributedIIActorSystem receiver = kafka.startReceiver(b, before, null)) {
                for (int i = 1; i <= 5; i++) {
                    assertTrue(remote.send("record", "b" + i).accepted());
                }
                await(Duration.ofSeconds(60), () -> before.snapshot().size() == 5, "b1..b5 run");
                awaitChecked(Duration.ofSeconds(30), () -> kafka.committed(b) == 5, "position 5 is committed");
            }

            for (int i = 6; i <= 10; i++) {
                assertTrue(remote.send("record", "b" + i).accepted(), "the broker takes b" + i + " while the receiver is down");
            }
            assertEquals(5, kafka.committed(b), "nothing is committed while the receiver is down");

            RunTally after = new RunTally();
            try (DistributedIIActorSystem receiver = kafka.startReceiver(b, after, null)) {
                await(Duration.ofSeconds(60), () -> after.snapshot().size() >= 5, "b6..b10 run after the restart");
                awaitChecked(Duration.ofSeconds(30), () -> kafka.committed(b) == 10, "position 10 is committed");
                Thread.sleep(3000);
            }

            Map<String, Integer> expectedBefore = new TreeMap<>();
            Map<String, Integer> expectedAfter = new TreeMap<>();
            for (int i = 1; i <= 5; i++) expectedBefore.put("b" + i, 1);
            for (int i = 6; i <= 10; i++) expectedAfter.put("b" + i, 1);
            observe("case B runs before the stop " + before.snapshot() + ", after the restart " + after.snapshot());
            assertEquals(expectedBefore, before.snapshot());
            assertEquals(expectedAfter, after.snapshot(), "only b6..b10, once each, after the restart");
        }
    }
}
