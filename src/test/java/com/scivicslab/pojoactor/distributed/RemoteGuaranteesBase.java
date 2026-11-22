package com.scivicslab.pojoactor.distributed;

import static com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.QUIET_MILLIS;
import static com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.WAIT_SECONDS;
import static com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.await;
import static com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.describe;
import static com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.kind;
import static com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.onCommonPool;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.Delivery;
import com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.Fixture;
import com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.Sent;
import com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.Target;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The guarantees of an actor that is called from outside the process, each run for every delivery
 * a subclass names and for both kinds of called actor. A call from outside is put in the actor's
 * mailbox by a {@link Dispatcher}; from there the actor's receive loop (a virtual thread) takes it
 * like any other message, so that
 * <ol>
 *   <li>a message sent later does not start before an earlier one has ended;</li>
 *   <li>at most one thread is inside the object at any time, calls from outside mixed with plain
 *       {@code tell}s;</li>
 *   <li>a field with no lock loses no write, in the same mixture;</li>
 *   <li>the called action runs on the receive loop's virtual thread: not on the thread that
 *       brought the call (an HTTP server's virtual thread, the thread that polls Kafka), and not
 *       on a real thread of Java's common pool.</li>
 * </ol>
 * "Sent" means that the send call has returned: for a dispatcher call, when {@code dispatch} has
 * returned; for {@code send} over HTTP, when the receipt has come; for {@code callByActionName}
 * over HTTP, when the result has come; for Kafka, when the record has been read and handed over.
 * Every wait is bounded: a step that hangs fails the test with a message, not the build.
 *
 * <p>The class is not itself a test ({@code Base}, not {@code Test}), so surefire runs only its
 * subclasses.</p>
 */
abstract class RemoteGuaranteesBase {

    /** The deliveries this subclass is about. */
    abstract List<Delivery> deliveries();

    // ------------------------------------------------------------------ 1. order

    @Test
    @DisplayName("1 order: a plain message sent after a held-up call from outside does not start until that call has ended")
    void aLaterMessageWaitsForTheEarlierCall() {
        List<String> failures = new ArrayList<>();
        for (Delivery d : deliveries()) {
            for (Target target : Target.values()) {
                try (Fixture f = new Fixture(target)) {
                    Sent heavy = d.deliver(f, "heavy", null);
                    await(f.p.heavyStarted, "the held-up call to start");
                    CompletableFuture<Void> light = f.plainTell("light", null);
                    Thread.sleep(QUIET_MILLIS);
                    long lightStartedWhileHeld = f.p.lightStartStamp;
                    f.p.heavyRelease.countDown();
                    await(heavy.done(), "the held-up call");
                    await(light, "the later message");
                    if (lightStartedWhileHeld != 0) {
                        failures.add(d.name() + " -> " + target + ": the later message started while the call from outside was still held up");
                    } else if (f.p.lightStartStamp <= f.p.heavyEndStamp) {
                        failures.add(d.name() + " -> " + target + ": the later message started (stamp " + f.p.lightStartStamp
                                + ") before the call from outside ended (stamp " + f.p.heavyEndStamp + ")");
                    }
                } catch (Throwable t) {
                    failures.add(describe(d, target, t));
                }
            }
        }
        assertEquals(List.of(), failures, "deliveries where order was not kept: " + failures.size());
    }

    @Test
    @DisplayName("1 order: 300 messages, alternately a call from outside and a plain tell, start in the order sent")
    void messagesStartInTheOrderTheyWereSent() {
        int count = 300;
        List<String> failures = new ArrayList<>();
        for (Delivery d : deliveries()) {
            for (Target target : Target.values()) {
                try (Fixture f = new Fixture(target)) {
                    List<CompletableFuture<?>> all = new ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        if (i % 2 == 0) {
                            Sent sent = d.deliver(f, "record", String.valueOf(i));
                            sent.awaitCall(); // the next message is sent only after this send call has returned
                            all.add(sent.done());
                        } else {
                            all.add(f.plainTell("record", String.valueOf(i)));
                        }
                    }
                    for (CompletableFuture<?> each : all) {
                        await(each, "a message");
                    }
                    List<Integer> started = new ArrayList<>(f.p.startOrder);
                    List<Integer> sent = new ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        sent.add(i);
                    }
                    if (!sent.equals(started)) {
                        int at = 0;
                        while (at < started.size() && at < count && started.get(at) == at) {
                            at++;
                        }
                        failures.add(d.name() + " -> " + target + ": message " + (at < started.size() ? started.get(at) : -1)
                                + " started at position " + at + ", where message " + at + " was due");
                    }
                } catch (Throwable t) {
                    failures.add(describe(d, target, t));
                }
            }
        }
        assertEquals(List.of(), failures, "deliveries where the start order differs from the send order: " + failures.size());
    }

    // ------------------------------------------------------------------ 2. at most one inside

    @Test
    @DisplayName("2 exclusion: calls from outside and plain tells, sent at once, never have two threads inside the object")
    void atMostOneThreadIsInsideTheObject() {
        List<String> failures = new ArrayList<>();
        for (Delivery d : deliveries()) {
            for (Target target : Target.values()) {
                try (Fixture f = new Fixture(target)) {
                    sendMixed(f, d, "touch");
                    int most = f.p.maxInside.get();
                    if (most > 1) {
                        failures.add(d.name() + " -> " + target + ": " + most + " threads were inside the object at the same time");
                    }
                } catch (Throwable t) {
                    failures.add(describe(d, target, t));
                }
            }
        }
        assertEquals(List.of(), failures, "deliveries where two threads were inside at once: " + failures.size());
    }

    // ------------------------------------------------------------------ 3. no lost write

    @Test
    @DisplayName("3 writes: increments of an unguarded field, calls from outside mixed with plain tells, lose none")
    void noWriteIsLost() {
        List<String> failures = new ArrayList<>();
        for (Delivery d : deliveries()) {
            for (Target target : Target.values()) {
                try (Fixture f = new Fixture(target)) {
                    int total = sendMixed(f, d, "increment");
                    int value = f.p.counter;
                    if (value != total) {
                        failures.add(d.name() + " -> " + target + ": the field ends at " + value + " after " + total + " increments, "
                                + (total - value) + " lost");
                    }
                } catch (Throwable t) {
                    failures.add(describe(d, target, t));
                }
            }
        }
        assertEquals(List.of(), failures, "deliveries that lost writes: " + failures.size());
    }

    /** The most sends of one delivery on their way at once in the mixed tests. */
    private static final int IN_FLIGHT = 32;

    /**
     * Sends half of {@link Delivery#mixedMessages()} by {@code d} from this thread and the other half
     * by plain {@code tell} from a virtual thread at the same time, waits for all of them, and
     * returns how many were sent in all.
     */
    private static int sendMixed(Fixture f, Delivery d, String action) throws Exception {
        int half = d.mixedMessages() / 2;
        List<CompletableFuture<?>> plain = new ArrayList<>();
        Thread sender = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < half; i++) {
                plain.add(f.plainTell(action, null));
            }
        });
        // At most IN_FLIGHT sends of this delivery are on their way at once; this test is about the
        // guarantees, not about how many calls one receiver takes at once.
        java.util.ArrayDeque<CompletableFuture<?>> inFlight = new java.util.ArrayDeque<>();
        for (int i = 0; i < half; i++) {
            inFlight.add(d.deliver(f, action, null).done());
            if (inFlight.size() >= IN_FLIGHT) {
                await(inFlight.poll(), "a message sent as " + d.name());
            }
        }
        while (!inFlight.isEmpty()) {
            await(inFlight.poll(), "a message sent as " + d.name());
        }
        sender.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        for (CompletableFuture<?> each : plain) {
            await(each, "a plain tell");
        }
        return half * 2;
    }

    // ------------------------------------------------------------------ 4. thread kinds

    @Test
    @DisplayName("4 threads: the action of a call from outside runs on the receive loop's virtual thread")
    void theActionRunsOnTheReceiveLoop() {
        List<String> failures = new ArrayList<>();
        for (Delivery d : deliveries()) {
            for (Target target : Target.values()) {
                try (Fixture f = new Fixture(target)) {
                    List<String> problems = new ArrayList<>();
                    Thread loop = f.actor.ask(x -> Thread.currentThread()).get(WAIT_SECONDS, TimeUnit.SECONDS);
                    if (!loop.isVirtual()) {
                        problems.add("the receive loop runs on a " + kind(loop) + ", not a virtual thread");
                    }
                    Sent heavy = d.deliver(f, "heavy", null);
                    await(f.p.heavyStarted, "the heavy call to start");
                    f.p.heavyRelease.countDown();
                    await(heavy.done(), "the heavy call");
                    await(d.deliver(f, "light", null).done(), "the light call");

                    if (f.p.lightThread != loop) {
                        problems.add("light call ran on " + kind(f.p.lightThread) + ", not on the receive loop's " + kind(loop));
                    }
                    if (f.p.heavyThread != loop) {
                        problems.add("heavy call ran on " + kind(f.p.heavyThread) + ", not on the receive loop's " + kind(loop));
                    }
                    if (onCommonPool(f.p.heavyThread) || onCommonPool(f.p.lightThread)) {
                        problems.add("work ran on a real thread of Java's common pool");
                    }
                    if (!problems.isEmpty()) {
                        failures.add(d.name() + " -> " + target + ": " + String.join("; ", problems));
                    }
                } catch (Throwable t) {
                    failures.add(describe(d, target, t));
                }
            }
        }
        assertEquals(List.of(), failures, "deliveries that ran work on the wrong thread: " + failures.size());
    }
}
