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

import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link MessageIdStore} in memory, bounded by a count and an age. It does not survive a restart.
 */
public class InMemoryMessageIdStore implements MessageIdStore {

    private final int maxEntries;
    private final long maxAgeMillis;
    private final Clock clock;
    // Oldest first: ids are added in time order, so expired ones are all at the head.
    private final LinkedHashMap<String, Long> seen = new LinkedHashMap<>();

    /**
     * @param maxEntries how many ids to remember at most; must be positive
     * @param maxAge     how long to remember an id; must be positive
     */
    public InMemoryMessageIdStore(int maxEntries, Duration maxAge) {
        this(maxEntries, maxAge, Clock.systemUTC());
    }

    InMemoryMessageIdStore(int maxEntries, Duration maxAge, Clock clock) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive: " + maxEntries);
        }
        if (maxAge == null || maxAge.isZero() || maxAge.isNegative()) {
            throw new IllegalArgumentException("maxAge must be positive: " + maxAge);
        }
        this.maxEntries = maxEntries;
        this.maxAgeMillis = maxAge.toMillis();
        this.clock = clock;
    }

    @Override
    public synchronized boolean contains(String messageId) {
        expire();
        return seen.containsKey(messageId);
    }

    @Override
    public synchronized boolean hasRoom() {
        expire();
        return seen.size() < maxEntries;
    }

    @Override
    public synchronized void add(String messageId) {
        addAt(messageId, clock.millis());
    }

    // For loading a persisted entry with the time it was first seen.
    synchronized void addAt(String messageId, long millis) {
        seen.remove(messageId);
        seen.put(messageId, millis);
        expire();
    }

    synchronized Map<String, Long> snapshot() {
        expire();
        return new LinkedHashMap<>(seen);
    }

    private void expire() {
        long oldestAllowed = clock.millis() - maxAgeMillis;
        Iterator<Map.Entry<String, Long>> it = seen.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> e = it.next();
            if (e.getValue() < oldestAllowed || seen.size() > maxEntries) {
                it.remove();
            } else {
                break;
            }
        }
    }
}
