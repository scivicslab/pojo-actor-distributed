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

/**
 * The receiver's memory of message ids it has already accepted, so that the second copy of a
 * message can be dropped. Replaceable: a chat application can keep the ids in its own database.
 *
 * <p>The memory is bounded by a count and an age, both given by whoever creates the store;
 * an id that has fallen out of the window is forgotten, and a copy that arrives later runs
 * again.</p>
 */
public interface MessageIdStore {

    /** @return true if this id was added and has not been forgotten */
    boolean contains(String messageId);

    /**
     * Whether an id can be added without a remembered, unexpired id being forgotten to make room.
     *
     * <p>{@link DedupDispatcher} refuses a new message while this is false: a store that forgot a live id
     * would let a copy of that message run again, and a sender who sends many new ids could make it so.
     * The default is true, for a store that never forgets a live id.</p>
     */
    default boolean hasRoom() {
        return true;
    }

    /** Remembers the id. Must be durable when this returns if the store claims to survive a restart. */
    void add(String messageId);
}
