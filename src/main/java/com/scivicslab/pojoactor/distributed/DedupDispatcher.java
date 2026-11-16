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

import java.util.concurrent.CompletableFuture;
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
 */
public class DedupDispatcher implements Dispatcher {

    private static final Logger logger = Logger.getLogger(DedupDispatcher.class.getName());

    private final Dispatcher inner;
    private final MessageIdStore seen;

    public DedupDispatcher(Dispatcher inner, MessageIdStore seen) {
        this.inner = inner;
        this.seen = seen;
    }

    // Serialised so that two copies arriving together cannot both pass the check. The wrapped
    // dispatcher only puts the call in a mailbox, so the lock is held briefly.
    @Override
    public synchronized Dispatch dispatch(ActorMessage message) {
        String id = message.getMessageId();
        if (seen.contains(id)) {
            logger.info("Dropping duplicate message " + id + " for " + message.getActorName()
                    + "/" + message.getActionName());
            return new Dispatch(true, CompletableFuture.completedFuture(
                    new ActionResult(false, "Duplicate message " + id + " ignored")));
        }
        Dispatch dispatch = inner.dispatch(message);
        if (dispatch.accepted()) {
            seen.add(id);
        }
        return dispatch;
    }
}
