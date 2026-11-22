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

import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link Dispatcher} that lets each {@code messageId} through once.
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
    public synchronized Dispatch dispatch(ActorMessage message) {
        if (!recover()) {
            return Dispatch.unavailable(UNAVAILABLE_REASON);
        }
        String id = message.getMessageId();
        String call = id + " (" + message.getActorName() + "/" + message.getActionName() + ")";
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
                    new ActionResult(false, "Duplicate message " + id + " ignored")));
        }
        Dispatch dispatch = inner.dispatch(message);
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

    private void startBackoff() {
        wait = FIRST_RETRY;
        nextTryMillis = clock.millis() + wait.toMillis();
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
