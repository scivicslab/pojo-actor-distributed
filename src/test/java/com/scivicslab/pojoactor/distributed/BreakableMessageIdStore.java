package com.scivicslab.pojoactor.distributed;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A message-id store whose {@link #add} the test can make fail, as a full or broken disk would,
 * and make work again. {@link #contains} keeps working: it reads what is already remembered.
 */
class BreakableMessageIdStore implements MessageIdStore {

    private final InMemoryMessageIdStore memory = new InMemoryMessageIdStore(100, Duration.ofHours(1));
    volatile boolean broken;
    final AtomicInteger addAttempts = new AtomicInteger();

    @Override
    public boolean contains(String messageId) {
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
