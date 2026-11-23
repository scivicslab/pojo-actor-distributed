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

import com.scivicslab.pojoactor.action.ActionResult;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link Dispatcher} that lets each message through once.
 *
 * <p>What is remembered is the id together with a digest of the actor, the action and the arguments, not
 * the id alone. A copy is a message with the same id and the same content, which is what a sender that
 * resent after a lost receipt sends. Another sender who reaches the receiver first with the same id and
 * different content does not make the real message a duplicate: it is a different message and it runs.
 * Whoever knows the whole message can still stop it by sending it first; that does not change what runs,
 * only who sent it, and a receiver with no authentication cannot tell the two apart.</p>
 *
 * <p>The allow-list is asked first ({@link Dispatcher#permits}). A message that is not allowed is refused
 * whether or not its id was seen, so the answer does not tell a caller which ids exist.</p>
 *
 * <p>While the store has no room ({@link MessageIdStore#hasRoom}), a message that is not a copy is not
 * taken: the call gets {@link Dispatch.Outcome#UNAVAILABLE} with the reason {@value #FULL_REASON} and may
 * be sent again once ids have expired. A store that forgot a live id to make room would let a copy of an
 * old message run again, and anyone who can send many messages could cause that. Size the store for the
 * messages of one window. Copies are still recognised while the store is full.</p>
 *
 * <p>The first copy goes to the wrapped dispatcher. If that accepts it, the id is remembered, and
 * later copies are dropped without reaching the actor. A dropped copy still gets an accepted
 * receipt, because the first copy was accepted: a sender that resent after a lost receipt can
 * stop. A message that was refused, or that the wrapped dispatcher failed to take, is not
 * remembered, so a resend is tried again.</p>
 *
 * <p>The id is remembered after the wrapped dispatcher has accepted the message. A receiver that
 * dies between the two may run the message a second time after it restarts: delivery is at
 * least once, and this only removes the duplicates that a running receiver can see.</p>
 *
 * <h2>When the store fails</h2>
 * <p>If {@link MessageIdStore#add} throws, the message was already accepted and will run, so it
 * keeps its accepted outcome; the failure is logged as SEVERE. Its id is kept here, in memory, as
 * pending, and the dispatcher becomes unavailable: a resend of that message could not be told
 * apart from a new one, and every copy would run the action again.</p>
 *
 * <p>If {@link MessageIdStore#contains} throws, the dispatcher cannot tell a copy from a new
 * message either. Nothing was accepted, so nothing is pending: the call gets
 * {@link Dispatch.Outcome#UNAVAILABLE} and the dispatcher becomes unavailable until the store can
 * be read again. The id of that call is kept only to ask the store about it on the next try.</p>
 *
 * <p>While unavailable, {@link #dispatch} puts nothing in a mailbox. Each call (and each
 * {@link #recover}) first tries the store again, but no sooner than the backoff allows: 1 s after
 * the failure, then 2, 4, 8 and 16 s, then every 30 s. A try reads the store if a read failed and
 * records every pending id. When it succeeds the dispatcher is available again and the call goes
 * on as usual; otherwise the call gets {@link Dispatch.Outcome#UNAVAILABLE} with the reason
 * {@value #UNAVAILABLE_REASON}. Pending ids live only in memory: a receiver that is stopped while
 * some are pending may run those messages again after it restarts, as for any message accepted
 * just before a crash.</p>
 */
public class DedupDispatcher implements Dispatcher {

    private static final Logger logger = Logger.getLogger(DedupDispatcher.class.getName());

    /** The reason given with {@link Dispatch.Outcome#UNAVAILABLE}. */
    public static final String UNAVAILABLE_REASON = "message id store unavailable";

    /** The reason given with {@link Dispatch.Outcome#UNAVAILABLE} while the store has no room. */
    public static final String FULL_REASON = "message id store full";

    /** How long after the first failure the store is tried again. */
    static final Duration FIRST_RETRY = Duration.ofSeconds(1);

    /** The longest wait between two tries; each failed try doubles the wait up to this. */
    static final Duration MAX_RETRY = Duration.ofSeconds(30);

    private final Dispatcher inner;
    private final MessageIdStore seen;
    private final Clock clock;

    // Accepted, but not yet recorded in the store, in the order they were accepted.
    private final Set<String> pending = new LinkedHashSet<>();
    // The id whose check against the store failed; null when the store could be read.
    private String unreadId;
    // Whether the last new message was refused for want of room; keeps the log to one line per fill.
    private boolean full;
    private Duration wait = FIRST_RETRY;
    private long nextTryMillis;

    public DedupDispatcher(Dispatcher inner, MessageIdStore seen) {
        this(inner, seen, Clock.systemUTC());
    }

    // For tests: a clock the test moves by hand.
    DedupDispatcher(Dispatcher inner, MessageIdStore seen, Clock clock) {
        this.inner = inner;
        this.seen = seen;
        this.clock = clock;
    }

    // Serialised so that two copies arriving together cannot both pass the check. The wrapped
    // dispatcher only puts the call in a mailbox, so the lock is held briefly.
    @Override
    public Dispatch dispatch(ActorMessage message) {
        return dispatch(message, CallerIdentity.NONE);
    }

    @Override
    public synchronized Dispatch dispatch(ActorMessage message, CallerIdentity caller) {
        if (!recover()) {
            return Dispatch.unavailable(UNAVAILABLE_REASON);
        }
        if (!inner.permits(message, caller)) {
            return inner.dispatch(message, caller);
        }
        String id = keyOf(message);
        String call = message.getMessageId() + " (" + message.getActorName() + "/" + message.getActionName() + ")";
        boolean duplicate;
        try {
            duplicate = seen.contains(id);
        } catch (RuntimeException e) {
            unreadId = id;
            startBackoff();
            logger.log(Level.SEVERE, "Could not read the message id store for message " + call
                    + "; not taken, and taking no calls until the store can be read, next try in "
                    + wait.toSeconds() + " s", e);
            return Dispatch.unavailable(UNAVAILABLE_REASON);
        }
        if (duplicate) {
            logger.info("Dropping duplicate message " + call);
            return new Dispatch(true, CompletableFuture.completedFuture(
                    new ActionResult(false, "Duplicate message " + message.getMessageId() + " ignored")));
        }
        if (!seen.hasRoom()) {
            if (!full) {
                full = true;
                logger.severe("The message id store is full of ids that have not expired; taking no new messages"
                        + " until some expire. Message " + call + " was not taken.");
            }
            return Dispatch.unavailable(FULL_REASON);
        }
        full = false;
        Dispatch dispatch = inner.dispatch(message, caller);
        if (dispatch.accepted()) {
            try {
                seen.add(id);
            } catch (RuntimeException e) {
                pending.add(id);
                startBackoff();
                logger.log(Level.SEVERE, "Could not record message id " + call + ", accepted and will run;"
                        + " taking no more calls until the message id store records again, next try in "
                        + wait.toSeconds() + " s", e);
            }
        }
        return dispatch;
    }

    /**
     * Tries the store again if the dispatcher is unavailable and the backoff allows a try now:
     * reads it if a read failed, and records the pending ids.
     *
     * @return true when the dispatcher is available (calls are taken); false while the store
     *         still fails or it is too soon to try it again
     */
    @Override
    public boolean permits(ActorMessage message, CallerIdentity caller) {
        return inner.permits(message, caller);
    }

    @Override
    public boolean permits(ActorMessage message) {
        return inner.permits(message);
    }

    @Override
    public synchronized boolean recover() {
        if (unreadId == null && pending.isEmpty()) {
            return true;
        }
        long now = clock.millis();
        if (now < nextTryMillis) {
            return false;
        }
        try {
            if (unreadId != null) {
                seen.contains(unreadId);
                unreadId = null;
            }
            Iterator<String> it = pending.iterator();
            while (it.hasNext()) {
                seen.add(it.next());
                it.remove();
            }
        } catch (RuntimeException e) {
            wait = min(wait.multipliedBy(2), MAX_RETRY);
            nextTryMillis = now + wait.toMillis();
            logger.log(Level.SEVERE, "The message id store still fails (" + (unreadId != null ? "cannot be read, " : "")
                    + pending.size() + " id(s) pending); taking no calls, next try in " + wait.toSeconds() + " s", e);
            return false;
        }
        wait = FIRST_RETRY;
        logger.info("The message id store works again; taking calls");
        return true;
    }

    /**
     * The key under which a message is remembered: its id and a digest of its actor, action and arguments.
     * Each part is length-prefixed, so no two different messages give the same text.
     */
    static String keyOf(ActorMessage message) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            for (String part : new String[] {message.getActorName(), message.getActionName(), message.getArgs()}) {
                byte[] bytes = (part == null ? "\u0000null" : part).getBytes(StandardCharsets.UTF_8);
                sha.update((bytes.length + ":").getBytes(StandardCharsets.US_ASCII));
                sha.update(bytes);
            }
            return message.getMessageId() + "#" + java.util.HexFormat.of().formatHex(sha.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required of every Java platform", e);
        }
    }

    private void startBackoff() {
        wait = FIRST_RETRY;
        nextTryMillis = clock.millis() + wait.toMillis();
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
