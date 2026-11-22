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

import com.scivicslab.pojoactor.distributed.AllowListDispatcher.AllowedAction;

import java.util.Map;
import java.util.Set;

/**
 * Decides whether a caller may call an action of an actor. An {@link AllowListDispatcher} asks it
 * once for every call that arrives from outside the process.
 *
 * <p>Two policies cover the usual cases: {@link #anyCaller(Set)} lets every caller call the listed
 * actions, whoever it is; {@link #perCaller(Map, Set)} lets each identity call only the actions
 * listed for it. A policy lets nothing through that it does not list.</p>
 *
 * @since 5.0.0
 */
@FunctionalInterface
public interface CallPolicy {

    /**
     * @param caller     who sent the call; {@link CallerIdentity#NONE} when the receiver cannot tell
     * @param actorName  the actor the call is for
     * @param actionName the action the call is for
     * @return true when this caller may call this action of this actor
     */
    boolean allows(CallerIdentity caller, String actorName, String actionName);

    /** Every caller may call the listed (actor, action) pairs, and nothing else. */
    static CallPolicy anyCaller(Set<AllowedAction> allowed) {
        Set<AllowedAction> copy = Set.copyOf(allowed);
        return (caller, actorName, actionName) -> copy.contains(new AllowedAction(actorName, actionName));
    }

    /**
     * Each identity may call only the (actor, action) pairs listed for it; a caller with no identity
     * may call nothing.
     *
     * @param byIdentity the pairs each identity may call; an identity that is not a key may call nothing
     */
    static CallPolicy perCaller(Map<CallerIdentity, Set<AllowedAction>> byIdentity) {
        return perCaller(byIdentity, Set.of());
    }

    /**
     * Each identity may call only the (actor, action) pairs listed for it. A caller with no
     * identity, which includes every message that arrives by Kafka, may call only
     * {@code forUnidentified}.
     *
     * @param byIdentity    the pairs each identity may call; an identity that is not a key may call nothing
     * @param forUnidentified the pairs a caller with {@link CallerIdentity#NONE} may call
     */
    static CallPolicy perCaller(Map<CallerIdentity, Set<AllowedAction>> byIdentity,
                                Set<AllowedAction> forUnidentified) {
        Map<CallerIdentity, Set<AllowedAction>> copy = Map.copyOf(byIdentity);
        Set<AllowedAction> unidentified = Set.copyOf(forUnidentified);
        return (caller, actorName, actionName) -> {
            AllowedAction call = new AllowedAction(actorName, actionName);
            if (!caller.identified()) {
                return unidentified.contains(call);
            }
            Set<AllowedAction> allowed = copy.get(caller);
            return allowed != null && allowed.contains(call);
        };
    }
}
