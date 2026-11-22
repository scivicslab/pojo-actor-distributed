package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.action.CallableByActionName;
import com.scivicslab.pojoactor.core.ActorRef;
import com.scivicslab.pojoactor.core.ActorSystem;
import com.scivicslab.pojoactor.distributed.transport.HttpTransport;
import com.scivicslab.pojoactor.distributed.transport.KafkaTransport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What the remote-call guarantee tests share: the POJO under test, a fixture that makes an actor
 * system and the ways a call can arrive from outside the process, and the ways of waiting and
 * reporting.
 *
 * <p>A call that arrives from outside (HTTP or Kafka) is given to a {@link Dispatcher}, which puts
 * it in the target actor's mailbox. From there it is a message like any other: the actor's receive
 * loop, a virtual thread, takes one message at a time, in the order the messages entered the
 * mailbox. So a call from outside must not run on the thread that brought it (an HTTP server's
 * virtual thread, the Kafka poll thread), must not start before an earlier message has ended, and
 * must never be inside the object together with another message.</p>
 *
 * <p>This code uses no real-thread pool: the senders that run at the same time are virtual
 * threads, and no commonPool, executor-less {@code *Async} or parallel stream is used.</p>
 */
final class RemoteGuaranteeSupport {

    private RemoteGuaranteeSupport() {
    }

    /** The longest any step waits; a step that waits longer fails instead of hanging the test. */
    static final long WAIT_SECONDS = 10;

    /** How long a message that must not start is given to start anyway. */
    static final long QUIET_MILLIS = 300;

    /** The name of the actor every delivery calls. */
    static final String ACTOR = "p";

    /** The actions the actor may be called with from outside. */
    static final Set<String> ACTIONS = Set.of("heavy", "light", "touch", "increment", "record");

    /**
     * The POJO that is the actor's object. Its fields are read and written without any lock: the
     * guarantee under test is that the actor is the lock. It is reached by action name, as an actor
     * called from outside is. The argument of every action is {@code tag} or {@code tag|number};
     * the tag names the call, so that a test can wait for exactly that call to end.
     */
    static final class P implements CallableByActionName {
        final AtomicInteger inside = new AtomicInteger();
        final AtomicInteger maxInside = new AtomicInteger();
        final AtomicLong stamps = new AtomicLong();
        volatile long heavyEndStamp;
        volatile long lightStartStamp;
        volatile Thread heavyThread;
        volatile Thread lightThread;
        final CountDownLatch heavyStarted = new CountDownLatch(1);
        final CountDownLatch heavyRelease = new CountDownLatch(1);
        /** Read and written with no lock and no volatile: a lost update shows as a short count. */
        int counter;
        final Queue<Integer> startOrder = new ConcurrentLinkedQueue<>();
        private final Map<String, CompletableFuture<Void>> ended = new ConcurrentHashMap<>();

        /** Completes when the call with this tag has ended; may be asked for before the call arrives. */
        CompletableFuture<Void> endOf(String tag) {
            return ended.computeIfAbsent(tag, t -> new CompletableFuture<>());
        }

        @Override
        public ActionResult callByActionName(String actionName, String args) {
            String[] parts = args.split("\\|", 2);
            int now = inside.incrementAndGet();
            maxInside.accumulateAndGet(now, Math::max);
            try {
                switch (actionName) {
                    case "heavy" -> heavy();
                    case "light" -> light();
                    case "touch" -> pause();
                    case "increment" -> increment();
                    case "record" -> startOrder.add(Integer.parseInt(parts[1]));
                    default -> {
                        return new ActionResult(false, "Unknown action: " + actionName);
                    }
                }
                return new ActionResult(true, "ok");
            } finally {
                inside.decrementAndGet();
                endOf(parts[0]).complete(null);
            }
        }

        /** Widens the window inside a method so that two threads inside it overlap with near certainty. */
        private static void pause() {
            for (int i = 0; i < 300; i++) {
                Thread.onSpinWait();
            }
        }

        /** A held-up message: it starts, then waits until the test releases it, then ends. */
        private void heavy() {
            heavyThread = Thread.currentThread();
            heavyStarted.countDown();
            try {
                heavyRelease.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            heavyEndStamp = stamps.incrementAndGet();
        }

        /** A quick message that records when it started and on which thread. */
        private void light() {
            lightThread = Thread.currentThread();
            lightStartStamp = stamps.incrementAndGet();
        }

        /** Adds one to the unguarded counter, with a pause between the read and the write. */
        private void increment() {
            int read = counter;
            pause();
            counter = read + 1;
        }
    }

    /** An actor reference that is itself called by action name, as Turing-workflow's IIActorRef is. */
    static final class CallableRef extends ActorRef<P> implements CallableByActionName {
        CallableRef(String name, P object, ActorSystem system) {
            super(name, object, system);
        }

        @Override
        public ActionResult callByActionName(String actionName, String args) {
            return object.callByActionName(actionName, args);
        }
    }

    /** What the called actor is: a plain reference that holds the POJO, or a reference that is called itself. */
    enum Target {
        POJO_BEHIND_A_PLAIN_REFERENCE,
        REFERENCE_CALLED_BY_ACTION_NAME
    }

    /** An actor system with one actor, and the ways a call from outside can arrive at it. */
    static final class Fixture implements AutoCloseable {
        final ActorSystem system = new ActorSystem("remote-guarantees");
        final P p = new P();
        final ActorRef<P> actor;
        /** The allow-list alone, as the Dispatcher of a receiver that does not de-duplicate. */
        final Dispatcher allowList;
        /** What {@code DistributedIIActorSystem} builds by default: the allow-list behind the de-duplication. */
        final Dispatcher dispatcher;
        private final AtomicInteger tags = new AtomicInteger();
        private HttpActorServer httpServer;
        private RemoteActorRef remote;
        private KafkaActorServer kafkaServer;
        private MockConsumer<String, String> kafkaConsumer;
        private long kafkaOffset;
        private final Object kafkaLock = new Object();

        Fixture(Target target) {
            if (target == Target.POJO_BEHIND_A_PLAIN_REFERENCE) {
                actor = system.actorOf(ACTOR, p);
            } else {
                CallableRef ref = new CallableRef(ACTOR, p, system);
                system.addActor(ref);
                actor = ref;
            }
            Set<AllowListDispatcher.AllowedAction> allowed = new java.util.HashSet<>();
            for (String action : ACTIONS) {
                allowed.add(new AllowListDispatcher.AllowedAction(ACTOR, action));
            }
            allowList = new AllowListDispatcher(system, allowed);
            dispatcher = new DedupDispatcher(allowList, new InMemoryMessageIdStore(1_000_000, Duration.ofMinutes(10)));
        }

        /** A tag that no other call of this fixture has. */
        String nextTag() {
            return "m" + tags.incrementAndGet();
        }

        /** A plain {@code tell} that calls the same action by name, so that it is counted like a call from outside. */
        CompletableFuture<Void> plainTell(String action, String number) {
            String tag = nextTag();
            String args = number == null ? tag : tag + "|" + number;
            return actor.tell(x -> x.callByActionName(action, args));
        }

        /** Starts an HTTP receiver on a free loopback port, once, and returns a client of it. */
        synchronized RemoteActorRef http() throws Exception {
            if (remote == null) {
                int port;
                try (ServerSocket free = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
                    port = free.getLocalPort();
                }
                httpServer = new HttpActorServer(dispatcher, "127.0.0.1", port);
                httpServer.start();
                remote = new RemoteActorRef(ACTOR, new NodeInfo("node", "127.0.0.1", port), new HttpTransport(),
                        Duration.ofSeconds(WAIT_SECONDS * 3));
            }
            return remote;
        }

        /** Hands a call to the Kafka receiver, as if a record had arrived in its inbox, and polls once. */
        void kafka(ActorMessage message) {
            synchronized (kafkaLock) {
                if (kafkaServer == null) {
                    NodeInfo me = new NodeInfo("me", "127.0.0.1", 8080);
                    TopicPartition tp = new TopicPartition(KafkaTransport.inboxTopic("me"), 0);
                    kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
                    kafkaConsumer.assign(java.util.List.of(tp));
                    kafkaConsumer.updateBeginningOffsets(Map.of(tp, 0L));
                    kafkaServer = new KafkaActorServer(dispatcher, me, kafkaConsumer);
                }
                kafkaConsumer.addRecord(new ConsumerRecord<>(KafkaTransport.inboxTopic("me"), 0, kafkaOffset++,
                        "k", message.toJson()));
                kafkaServer.pollOnce(Duration.ZERO);
            }
        }

        @Override
        public void close() {
            p.heavyRelease.countDown(); // a held-up message must not outlive the test
            if (httpServer != null) {
                httpServer.close();
            }
            if (kafkaServer != null) {
                kafkaServer.close();
            }
            system.terminate();
        }
    }

    /** What a delivery returns: when the send call came back, and when the called action has ended. */
    record Sent(CountDownLatch called, CompletableFuture<?> done) {
        void awaitCall() throws InterruptedException {
            if (!called.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new AssertionError("the send call did not return within " + WAIT_SECONDS + " s");
            }
        }
    }

    /** One way a call arrives from outside. */
    interface Delivery {
        String name();

        /** How many messages the mixed tests send in all: half by this delivery, half by plain {@code tell}. */
        default int mixedMessages() {
            return 10_000;
        }

        /**
         * Sends the call. A delivery that blocks (an HTTP call) does so on a virtual thread of its
         * own and returns at once; {@link Sent#awaitCall()} waits for the send call to return.
         */
        Sent deliver(Fixture f, String action, String number) throws Exception;
    }

    private static String argsOf(String tag, String number) {
        return number == null ? tag : tag + "|" + number;
    }

    /** A call handed to a dispatcher in this thread: when {@code dispatch} returns, the call is in the mailbox. */
    private static Delivery dispatching(String name, java.util.function.Function<Fixture, Dispatcher> which) {
        return new Delivery() {
            public String name() {
                return name;
            }

            public Sent deliver(Fixture f, String action, String number) {
                String tag = f.nextTag();
                Dispatcher.Dispatch d = which.apply(f).dispatch(new ActorMessage(ACTOR, action, argsOf(tag, number), "id-" + tag, null));
                CompletableFuture<Void> done = f.p.endOf(tag);
                if (!d.accepted()) {
                    done.completeExceptionally(new AssertionError("the dispatcher did not accept the call: " + d.result().join().getResult()));
                }
                return new Sent(new CountDownLatch(0), done);
            }
        };
    }

    static final Delivery ALLOW_LIST_DISPATCH = dispatching("Dispatcher.dispatch (allow-list)", f -> f.allowList);

    static final Delivery DEDUP_DISPATCH = dispatching("Dispatcher.dispatch (allow-list behind de-duplication)", f -> f.dispatcher);

    /** A blocking call on a virtual thread of its own: {@code called} counts down when the call has returned. */
    private interface Blocking {
        void call(Fixture f, String action, String args, String id) throws Exception;
    }

    private static Delivery overHttp(String name, Blocking call) {
        return new Delivery() {
            public String name() {
                return name;
            }

            @Override
            public int mixedMessages() {
                return 2_000;
            }

            public Sent deliver(Fixture f, String action, String number) throws Exception {
                f.http();
                String tag = f.nextTag();
                CountDownLatch called = new CountDownLatch(1);
                CompletableFuture<Void> done = f.p.endOf(tag);
                Thread.ofVirtual().start(() -> {
                    try {
                        call.call(f, action, argsOf(tag, number), "id-" + tag);
                    } catch (Throwable t) {
                        done.completeExceptionally(t);
                    } finally {
                        called.countDown();
                    }
                });
                return new Sent(called, done);
            }
        };
    }

    /** {@code RemoteActorRef.send}: the answer is the receipt, that the call is in the mailbox, not that it has run. */
    static final Delivery HTTP_SEND = overHttp("RemoteActorRef.send over HTTP (receipt when accepted)", (f, action, args, id) -> {
        Receipt receipt = f.http().send(action, args, id);
        if (!receipt.accepted()) {
            throw new AssertionError("no receipt: " + receipt.detail());
        }
    });

    /** {@code RemoteActorRef.callByActionName}: the answer is the result, after the action has run. */
    static final Delivery HTTP_INVOKE = overHttp("RemoteActorRef.callByActionName over HTTP (result after the run)", (f, action, args, id) -> {
        ActionResult result = f.http().callByActionName(action, args);
        if (!result.isSuccess()) {
            throw new AssertionError("the call failed: " + result.getResult());
        }
    });

    /** A record in the Kafka receiver's inbox, read by one poll: the receiver hands it to the dispatcher in this thread. */
    static final Delivery KAFKA = new Delivery() {
        public String name() {
            return "Kafka inbox record read by KafkaActorServer";
        }

        public Sent deliver(Fixture f, String action, String number) {
            String tag = f.nextTag();
            f.kafka(new ActorMessage(ACTOR, action, argsOf(tag, number), "id-" + tag, null));
            return new Sent(new CountDownLatch(0), f.p.endOf(tag));
        }
    };

    // ------------------------------------------------------------------ waiting and reporting

    static void await(CompletableFuture<?> future, String what) throws InterruptedException, ExecutionException {
        try {
            future.get(WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError(what + " did not finish within " + WAIT_SECONDS + " s");
        }
    }

    static void await(CountDownLatch latch, String what) throws InterruptedException {
        if (!latch.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
            throw new AssertionError(what + " did not happen within " + WAIT_SECONDS + " s");
        }
    }

    /** Whether {@code t} is a real thread of Java's common pool. */
    static boolean onCommonPool(Thread t) {
        return t instanceof ForkJoinWorkerThread w && w.getPool() == ForkJoinPool.commonPool();
    }

    /** A thread, with whether it is a real (platform) thread or a virtual thread. */
    static String kind(Thread t) {
        return t == null ? "no thread" : (t.isVirtual() ? "virtual thread " : "real thread ") + t.getName();
    }

    /** Where in the guarantee tests a failure happened, as File.java:line. */
    static String where(Throwable t) {
        for (StackTraceElement e : t.getStackTrace()) {
            if (e.getClassName().startsWith("com.scivicslab.pojoactor.distributed.Remote")) {
                return e.getFileName() + ":" + e.getLineNumber();
            }
        }
        return "?";
    }

    /** One line for a failure of one delivery to one kind of target, naming both and the line that failed. */
    static String describe(Delivery d, Target target, Throwable t) {
        Throwable cause = t instanceof ExecutionException && t.getCause() != null ? t.getCause() : t;
        return d.name() + " -> " + target + " [" + where(cause) + "]: " + cause;
    }
}
