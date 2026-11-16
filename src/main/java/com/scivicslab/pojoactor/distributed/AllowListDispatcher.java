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
 */
public class AllowListDispatcher implements Dispatcher {

    private static final Logger logger = Logger.getLogger(AllowListDispatcher.class.getName());

    private final ActorSystem actorSystem;
    private final Set<AllowedAction> allowed;

    /**
     * @param actorSystem the local actor system that holds the actors
     * @param allowed     the (actor, action) pairs that may be called from outside
     */
    public AllowListDispatcher(ActorSystem actorSystem, Set<AllowedAction> allowed) {
        this.actorSystem = actorSystem;
        this.allowed = Set.copyOf(allowed);
    }

    @Override
    public Dispatch dispatch(ActorMessage message) {
        String actorName = message.getActorName();
        String actionName = message.getActionName();
        String args = message.getArgs();
        ActorRef<?> ref = allowed.contains(new AllowedAction(actorName, actionName))
                ? actorSystem.getActor(actorName) : null;
        if (ref == null) {
            logger.warning("Refused call from outside: " + actorName + "/" + actionName);
            return Dispatch.refused("Not allowed: " + actorName + "/" + actionName);
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
