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

/**
 * Receives a call for a local actor from outside the process (HTTP or Kafka) and decides
 * whether it may run. Both receivers call the same dispatcher.
 *
 * <p>An accepted call runs through the target actor's mailbox, on the actor's own thread,
 * one message at a time. Callers never run an action on their own thread.</p>
 */
public interface Dispatcher {

    /**
     * Checks the call and, if it is allowed, puts it in the target actor's mailbox.
     * Returns without waiting for the action to run.
     *
     * @param message the call: target actor, action, arguments and the {@code messageId} that
     *                lets a receiver drop a second copy
     * @return the outcome: {@link Dispatch#accepted()} is the receipt
     */
    Dispatch dispatch(ActorMessage message);

    /** Dispatches a call that has no id of its own to remember: one is made up. */
    default Dispatch dispatch(String actorName, String actionName, String args) {
        return dispatch(new ActorMessage(actorName, actionName, args));
    }

    /**
     * The outcome of {@link #dispatch}.
     *
     * @param accepted true when the call is in the actor's mailbox; false when it was refused
     * @param result   completes with the action's result once it has run; for a refused call it
     *                 is already complete with a failure
     */
    record Dispatch(boolean accepted, CompletableFuture<ActionResult> result) {

        /** A refused call. */
        public static Dispatch refused(String reason) {
            return new Dispatch(false, CompletableFuture.completedFuture(new ActionResult(false, reason)));
        }
    }
}
