package com.scivicslab.pojoactor.distributed;

import static com.scivicslab.pojoactor.distributed.KafkaItSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;

/**
 * Case E, against a real broker: a message that ran but whose position was not recorded (the
 * receiver died in between, done here by setting the group's position back) is delivered again
 * after the restart. With a {@link FileMessageIdStore} on the same file, the second copy is
 * dropped; without deduplication the same redelivery runs the action a second time.
 */
@Tag("DistributedActorSystemRedesign_261115_oo01")
@DisplayName("Kafka, real broker: a redelivered message runs once with FileMessageIdStore")
@Timeout(240)
class KafkaRedeliveryIT {

    private KafkaItSupport kafka;

    @TempDir
    Path dir;

    @BeforeEach
    void setUp() {
        kafka = new KafkaItSupport();
    }

    @AfterEach
    void tearDown() {
        kafka.close();
    }

    @Test
    void redeliveredMessage_runsOnce_withTheStoreAcrossTheRestart() throws Exception {
        String b = newNodeId("e-recv");
        kafka.createInbox(b);
        Path ids = dir.resolve("message-ids");
        String id1 = UUID.randomUUID().toString();
        try (KafkaTransport sender = new KafkaTransport(node(newNodeId("e-send")), kafka.bootstrap)) {
            RemoteActorRef remote = new RemoteActorRef(TALLY, node(b), sender);

            RunTally first = new RunTally();
            try (FileMessageIdStore store = new FileMessageIdStore(ids, 1000, Duration.ofHours(1));
                 DistributedIIActorSystem receiver = kafka.startReceiver(b, first, store)) {
                assertTrue(remote.send("record", "e1", id1).accepted());
                await(Duration.ofSeconds(60), () -> first.runs("e1") == 1, "e1 runs");
                awaitChecked(Duration.ofSeconds(30), () -> kafka.committed(b) == 1, "position 1 is committed");
            }
            assertTrue(Files.readString(ids).contains(java.net.URLEncoder.encode(id1, java.nio.charset.StandardCharsets.UTF_8)),
                    "e1's id is in the file");

            // The receiver "died" before recording its position: set it back to e1.
            kafka.resetPosition(b, 0, Duration.ofSeconds(60));
            assertEquals(0, kafka.committed(b));

            RunTally second = new RunTally();
            try (FileMessageIdStore store = new FileMessageIdStore(ids, 1000, Duration.ofHours(1));
                 DistributedIIActorSystem receiver = kafka.startReceiver(b, second, store)) {
                assertTrue(remote.send("record", "e2").accepted());
                // One partition: e2 comes after e1, so once e2 has run, e1 has been read again.
                await(Duration.ofSeconds(60), () -> second.runs("e2") == 1, "e2 runs after the restart");
                awaitChecked(Duration.ofSeconds(30), () -> kafka.committed(b) == 2, "position 2 is committed");
                Thread.sleep(2000);
            }
            observe("case E with FileMessageIdStore: runs after the restart " + second.snapshot());
            assertEquals(0, second.runs("e1"), "the redelivered e1 is dropped: its id survived the restart in the file");

            // Control: the same redelivery without deduplication runs e1 again.
            kafka.resetPosition(b, 0, Duration.ofSeconds(60));
            RunTally third = new RunTally();
            try (DistributedIIActorSystem receiver = kafka.startReceiver(b, third, null)) {
                await(Duration.ofSeconds(60), () -> third.runs("e2") == 1, "e1 and e2 are read again");
                Thread.sleep(2000);
            }
            observe("case E control without deduplication: runs " + third.snapshot());
            assertEquals(1, third.runs("e1"), "without deduplication the redelivered e1 runs again");
        }
    }
}
