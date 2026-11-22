package com.scivicslab.pojoactor.distributed;

import static com.scivicslab.pojoactor.distributed.KafkaItSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;

/**
 * Case C (audit item 18), against a real broker: while the message-id store cannot record, or
 * cannot be read, the receiver stops reading its inbox, tries the store again with the backoff
 * (1 s, then doubling), and goes on by itself when the store works again. The waiting message runs
 * once.
 */
@Tag("DistributedActorSystemRedesign_261115_oo01")
@DisplayName("Kafka, real broker: the receiver pauses while the message-id store fails")
@Timeout(180)
class KafkaStoreUnavailableIT {

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
    void storeThatCannotRecord_pausesTheInbox_andTheReceiverResumesByItself() throws Exception {
        String b = newNodeId("c-recv");
        kafka.createInbox(b);
        BreakableMessageIdStore store = new BreakableMessageIdStore();
        RunTally tally = new RunTally();
        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();
        try (DistributedIIActorSystem receiver = kafka.startReceiver(b, tally, store);
             KafkaTransport sender = new KafkaTransport(node(newNodeId("c-send")), kafka.bootstrap)) {
            RemoteActorRef remote = new RemoteActorRef(TALLY, node(b), sender);

            store.broken = true;
            assertTrue(remote.send("record", "c1", id1).accepted());
            await(Duration.ofSeconds(60), () -> tally.runs("c1") == 1, "c1 runs although its id cannot be recorded");
            await(Duration.ofSeconds(10), () -> store.addAttempts.get() >= 1, "the store was asked to record c1");
            assertTrue(remote.send("record", "c2", id2).accepted());

            int attemptsAtPause = store.addAttempts.get();
            Thread.sleep(6000);
            int triesWhilePaused = store.addAttempts.get() - attemptsAtPause;
            observe("case C: store tried " + triesWhilePaused + " time(s) in 6 s while paused; committed=" + kafka.committed(b));
            assertEquals(0, tally.runs("c2"), "c2 does not run while the store cannot record");
            assertTrue(triesWhilePaused >= 1 && triesWhilePaused <= 4,
                    "the store is tried with the backoff (1 s, 2 s, 4 s), not on every poll: " + triesWhilePaused);
            assertEquals(1, kafka.committed(b), "nothing from c2 on is committed while paused");

            long repaired = System.nanoTime();
            store.broken = false;
            await(Duration.ofSeconds(40), () -> tally.runs("c2") == 1, "c2 runs after the store records again");
            observe("case C: c2 ran " + Duration.ofNanos(System.nanoTime() - repaired).toMillis() + " ms after the store was repaired");
            awaitChecked(Duration.ofSeconds(30), () -> kafka.committed(b) == 2, "position 2 is committed");
            Thread.sleep(3000);
            assertEquals(1, tally.runs("c1"), "c1 ran once");
            assertEquals(1, tally.runs("c2"), "c2 ran once");
            assertTrue(store.contains(id1), "c1's id was recorded after the repair");
            assertTrue(store.contains(id2));
        }
    }

    @Test
    void storeThatCannotBeRead_pausesTheInbox_andTheReceiverResumesByItself() throws Exception {
        String b = newNodeId("c-read");
        kafka.createInbox(b);
        BreakableMessageIdStore store = new BreakableMessageIdStore();
        RunTally tally = new RunTally();
        try (DistributedIIActorSystem receiver = kafka.startReceiver(b, tally, store);
             KafkaTransport sender = new KafkaTransport(node(newNodeId("c-send")), kafka.bootstrap)) {
            RemoteActorRef remote = new RemoteActorRef(TALLY, node(b), sender);

            store.containsBroken = true;
            assertTrue(remote.send("record", "c3").accepted());
            await(Duration.ofSeconds(60), () -> store.containsAttempts.get() >= 1, "the receiver reads c3 and asks the store");
            int attemptsAtPause = store.containsAttempts.get();
            Thread.sleep(6000);
            int triesWhilePaused = store.containsAttempts.get() - attemptsAtPause;
            observe("case C (read): store tried " + triesWhilePaused + " time(s) in 6 s while paused");
            assertEquals(0, tally.runs("c3"), "c3 does not run while the store cannot be read");
            assertTrue(triesWhilePaused >= 1 && triesWhilePaused <= 4, "backoff, not every poll: " + triesWhilePaused);
            assertTrue(kafka.committed(b) <= 0, "c3's position is not committed");

            store.containsBroken = false;
            await(Duration.ofSeconds(40), () -> tally.runs("c3") == 1, "c3 runs after the store can be read again");
            awaitChecked(Duration.ofSeconds(30), () -> kafka.committed(b) == 1, "position 1 is committed");
            Thread.sleep(3000);
            assertEquals(1, tally.runs("c3"), "c3 ran once");
        }
    }
}
