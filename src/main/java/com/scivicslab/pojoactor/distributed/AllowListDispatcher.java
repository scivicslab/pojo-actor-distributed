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
import com.scivicslab.pojoactor.action.CallableByActionName;
import com.scivicslab.pojoactor.core.ActorRef;
import com.scivicslab.pojoactor.core.ActorSystem;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link Dispatcher} that lets through only the (actor, action) pairs on its list.
 * An empty list lets nothing through.
 *
 * <p>A call for an actor that does not exist is refused with the same sentence as a call that
 * is not on the list, so a caller cannot find out which actors exist.</p>
 *
 * <p>An allowed call for an actor whose message loop is no longer running is refused with
 * "Not accepted: actor not running": a receipt for it would promise a run that never comes.
 * The check is made when the call arrives; an actor that dies after it does not run the call.</p>
 */
public class AllowListDispatcher implements Dispatcher {

    private static final Logger logger = Logger.getLogger(AllowListDispatcher.class.getName());

    private final ActorSystem actorSystem;
    private final CallPolicy policy;

    /**
     * @param actorSystem the local actor system that holds the actors
     * @param allowed     the (actor, action) pairs that may be called from outside, by any caller
     */
    public AllowListDispatcher(ActorSystem actorSystem, Set<AllowedAction> allowed) {
        this(actorSystem, CallPolicy.anyCaller(allowed));
    }

    /**
     * @param actorSystem the local actor system that holds the actors
     * @param policy      decides, for each call, whether its caller may call that action of that actor;
     *                    {@link CallPolicy#perCaller(java.util.Map)} lets each identity call only its own list
     * @since 5.0.0
     */
    public AllowListDispatcher(ActorSystem actorSystem, CallPolicy policy) {
        this.actorSystem = actorSystem;
        this.policy = java.util.Objects.requireNonNull(policy, "policy");
    }

    /** Asks as for a call whose sender is not known: {@link CallerIdentity#NONE}. */
    @Override
    public boolean permits(ActorMessage message) {
        return permits(message, CallerIdentity.NONE);
    }

    @Override
    public boolean permits(ActorMessage message, CallerIdentity caller) {
        return policy.allows(caller, message.getActorName(), message.getActionName());
    }

    /** Checks the call as one whose sender is not known: {@link CallerIdentity#NONE}. */
    @Override
    public Dispatch dispatch(ActorMessage message) {
        return dispatch(message, CallerIdentity.NONE);
    }

    @Override
    public Dispatch dispatch(ActorMessage message, CallerIdentity caller) {
        String actorName = message.getActorName();
        String actionName = message.getActionName();
        String args = message.getArgs();
        ActorRef<?> ref = policy.allows(caller, actorName, actionName)
                ? actorSystem.getActor(actorName) : null;
        if (ref == null) {
            logger.warning("Refused call from outside: " + actorName + "/" + actionName + " from " + caller);
            return Dispatch.refused("Not allowed: " + actorName + "/" + actionName);
        }
        // An actor whose message loop has died (an Error in an action ends it) stays registered,
        // but nobody reads its mailbox: a call put there would never run.
        if (!ref.isAlive()) {
            logger.warning("Refused call for an actor that is not running: " + actorName + "/" + actionName);
            return Dispatch.refused("Not accepted: actor not running");
        }
        try {
            return new Dispatch(true, runInMailbox(ref, actionName, args));
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "Could not enqueue " + actorName + "/" + actionName, e);
            return Dispatch.refused("Not accepted: " + actorName + "/" + actionName);
        }
    }

    // The reference itself may know the action names (Turing-workflow's IIActorRef does);
    // otherwise the POJO it holds may. Either way the call is a message to the actor,
    // so it runs on the actor's thread, after the messages already in the mailbox.
    private static <T> java.util.concurrent.CompletableFuture<ActionResult> runInMailbox(
            ActorRef<T> ref, String actionName, String args) {
        return ref.ask(pojo -> {
            CallableByActionName target = ref instanceof CallableByActionName c ? c
                    : pojo instanceof CallableByActionName p ? p : null;
            if (target == null) {
                return new ActionResult(false, "Actor does not implement CallableByActionName");
            }
            try {
                return target.callByActionName(actionName, args);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Action failed: " + actionName, e);
                return new ActionResult(false, "Action error: " + e.getMessage());
            }
        });
    }

    /** One (actor name, action name) pair that may be called from outside. */
    public record AllowedAction(String actorName, String actionName) {}
}
