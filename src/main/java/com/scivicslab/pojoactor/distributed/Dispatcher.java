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
     * Tries to become able to take calls again after {@link #dispatch} answered
     * {@link Dispatch.Outcome#UNAVAILABLE}. A receiver that has stopped reading (the Kafka
     * receiver pauses its inbox) calls this on each turn and starts reading again when it is
     * true. It may return false without trying, when it tried too recently.
     *
     * @return true when calls can be taken; a dispatcher that is never unavailable is always true
     */
    default boolean recover() {
        return true;
    }

    /**
     * The outcome of {@link #dispatch}.
     *
     * @param outcome what happened to the call
     * @param result  completes with the action's result once it has run; for a call that was not
     *                accepted it is already complete with a failure that says why
     */
    record Dispatch(Outcome outcome, CompletableFuture<ActionResult> result) {

        /** What happened to a call. */
        public enum Outcome {
            /** The call is in the actor's mailbox: the receipt. */
            ACCEPTED,
            /** The call will never run as sent (not allowed, no such actor, actor not running). */
            REFUSED,
            /**
             * The call was not taken now, but the same call may be taken later: the receiver
             * cannot work safely for the moment (the message-id store cannot record). Nothing
             * was put in a mailbox; send it again later with the same {@code messageId}.
             */
            UNAVAILABLE
        }

        /** An accepted call when {@code accepted} is true, a refused one when it is false. */
        public Dispatch(boolean accepted, CompletableFuture<ActionResult> result) {
            this(accepted ? Outcome.ACCEPTED : Outcome.REFUSED, result);
        }

        /** @return true when the call is in the actor's mailbox: the receipt */
        public boolean accepted() {
            return outcome == Outcome.ACCEPTED;
        }

        /** @return true when the call was not taken now but may be taken later */
        public boolean unavailable() {
            return outcome == Outcome.UNAVAILABLE;
        }

        /** A refused call. */
        public static Dispatch refused(String reason) {
            return new Dispatch(Outcome.REFUSED, CompletableFuture.completedFuture(new ActionResult(false, reason)));
        }

        /** A call not taken now that may be taken later. */
        public static Dispatch unavailable(String reason) {
            return new Dispatch(Outcome.UNAVAILABLE,
                    CompletableFuture.completedFuture(new ActionResult(false, reason)));
        }
    }
}
