package com.scivicslab.pojoactor.distributed;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A message-id store whose {@link #add} or {@link #contains} the test can make fail, as a full or
 * broken disk or an unreachable database would, and make work again.
 */
class BreakableMessageIdStore implements MessageIdStore {

    private final InMemoryMessageIdStore memory = new InMemoryMessageIdStore(100, Duration.ofHours(1));
    /** When true, {@link #add} throws; {@link #contains} keeps reading what is remembered. */
    volatile boolean broken;
    /** When true, {@link #contains} throws. */
    volatile boolean containsBroken;
    final AtomicInteger addAttempts = new AtomicInteger();
    final AtomicInteger containsAttempts = new AtomicInteger();

    @Override
    public boolean contains(String messageId) {
        containsAttempts.incrementAndGet();
        if (containsBroken) {
            throw new UncheckedIOException(new IOException("test: the store cannot be read"));
        }
        return memory.contains(messageId);
    }

    @Override
    public void add(String messageId) {
        addAttempts.incrementAndGet();
        if (broken) {
            throw new UncheckedIOException(new IOException("test: the disk is full"));
        }
        memory.add(messageId);
    }
}
