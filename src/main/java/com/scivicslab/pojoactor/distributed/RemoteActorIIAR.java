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

import java.util.logging.Level;
import java.util.logging.Logger;

import com.scivicslab.pojoactor.core.ActionResult;
import com.scivicslab.pojoactor.distributed.RemoteActorRef;
import com.scivicslab.turingworkflow.workflow.IIActorRef;
import com.scivicslab.turingworkflow.workflow.IIActorSystem;

/**
 * Stands in, inside this actor system, for an actor that lives in another process, so that a
 * workflow can name it in {@code actor:} like any other.
 *
 * <p>An {@link Interpreter} resolves an action's actor to an {@link IIActorRef} and calls
 * {@code callByActionName} on it. A {@link RemoteActorRef} knows how to reach another process but
 * is not an {@code ActorRef}, so it cannot be what the interpreter resolves to. This class is the
 * one layer between them.
 *
 * <p>The wrapped type is {@link Void}: the object itself is in the other process, so there is
 * nothing here for {@code ask} or {@code tell} to reach. Only {@code callByActionName} works.
 *
 * <p>See {@code RemoteChildActor_260906_oo01}.
 */
public class RemoteActorIIAR extends IIActorRef<Void> {

    private static final Logger LOGGER = Logger.getLogger(RemoteActorIIAR.class.getName());

    private final RemoteActorRef remote;

    /**
     * @param actorName the name this actor answers to in <em>this</em> system, which need not be
     *                  the name it has in its own — the caller decides how to qualify it
     * @param remote    the reference that reaches the actor in the other process
     * @param system    the actor system this proxy belongs to
     */
    public RemoteActorIIAR(String actorName, RemoteActorRef remote, IIActorSystem system) {
        super(actorName, null, system);
        this.remote = remote;
    }

    /** @return the reference this proxy forwards to */
    public RemoteActorRef getRemote() {
        return remote;
    }

    /**
     * Forwards the call and returns whatever the other process answered.
     *
     * <p>Overriding rather than declaring {@code @Action} methods, against the advice on
     * {@link IIActorRef#callByActionName}, because <strong>which actions exist is the other
     * process's business</strong>. Any list held here would be a copy that goes stale the moment
     * the child gains an action, and an unknown name has to reach the child to be refused by it.
     *
     * <p>A child that is not running fails this one action rather than ending the run: the
     * interpreter treats a failed {@link ActionResult} as a transition that did not fire, whereas
     * an exception would stop the workflow.
     */
    @Override
    public ActionResult callByActionName(String actionName, String args) {
        try {
            return remote.callByActionName(actionName, args);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Could not reach " + remote.getNodeInfo().getAddress()
                    + " for actor " + remote.getActorName() + " action " + actionName, e);
            return new ActionResult(false, "Could not reach " + remote.getActorName()
                    + " on " + remote.getNodeInfo().getAddress() + ": " + e.getMessage());
        }
    }
}
