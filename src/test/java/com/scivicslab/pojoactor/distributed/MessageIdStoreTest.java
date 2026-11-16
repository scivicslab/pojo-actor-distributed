package com.scivicslab.pojoactor.distributed;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** The memory of seen message ids: bounded by count and by age, and the file one survives a restart. */
class MessageIdStoreTest {

    /** A clock the test moves by hand. */
    static class FakeClock extends Clock {
        private Instant now = Instant.parse("2026-11-16T00:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    @Test
    void remembersWhatWasAdded() {
        MessageIdStore store = new InMemoryMessageIdStore(10, Duration.ofHours(1));
        assertFalse(store.contains("a"));
        store.add("a");
        assertTrue(store.contains("a"));
        assertFalse(store.contains("b"));
    }

    @Test
    void whenFull_forgetsTheOldestFirst() {
        MessageIdStore store = new InMemoryMessageIdStore(2, Duration.ofHours(1));
        store.add("a");
        store.add("b");
        store.add("c");
        assertFalse(store.contains("a"));
        assertTrue(store.contains("b"));
        assertTrue(store.contains("c"));
    }

    @Test
    void forgetsWhatIsOlderThanTheWindow() {
        FakeClock clock = new FakeClock();
        MessageIdStore store = new InMemoryMessageIdStore(10, Duration.ofMinutes(10), clock);
        store.add("old");
        clock.advance(Duration.ofMinutes(6));
        store.add("newer");
        clock.advance(Duration.ofMinutes(6));
        assertFalse(store.contains("old"), "12 minutes old");
        assertTrue(store.contains("newer"), "6 minutes old");
    }

    @Test
    void bothWindowsMustBeGiven_noDefault() {
        assertThrows(IllegalArgumentException.class, () -> new InMemoryMessageIdStore(0, Duration.ofHours(1)));
        assertThrows(IllegalArgumentException.class, () -> new InMemoryMessageIdStore(10, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new InMemoryMessageIdStore(10, null));
    }

    // ---- the file store ----

    @Test
    void fileStore_survivesAReopen(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("ids.log");
        try (FileMessageIdStore store = new FileMessageIdStore(file, 10, Duration.ofHours(1))) {
            store.add("a");
            store.add("b");
        }
        try (FileMessageIdStore reopened = new FileMessageIdStore(file, 10, Duration.ofHours(1))) {
            assertTrue(reopened.contains("a"));
            assertTrue(reopened.contains("b"));
            assertFalse(reopened.contains("c"));
        }
    }

    @Test
    void fileStore_reopenedAfterTheWindow_forgetsTheOldOnes(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("ids.log");
        FakeClock clock = new FakeClock();
        try (FileMessageIdStore store = new FileMessageIdStore(file, 10, Duration.ofMinutes(10), clock)) {
            store.add("old");
            clock.advance(Duration.ofMinutes(8));
            store.add("newer");
        }
        clock.advance(Duration.ofMinutes(5));
        try (FileMessageIdStore reopened = new FileMessageIdStore(file, 10, Duration.ofMinutes(10), clock)) {
            assertFalse(reopened.contains("old"));
            assertTrue(reopened.contains("newer"));
        }
    }

    @Test
    void fileStore_reopenedWithASmallerCount_keepsTheNewest(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("ids.log");
        try (FileMessageIdStore store = new FileMessageIdStore(file, 10, Duration.ofHours(1))) {
            for (String id : List.of("a", "b", "c", "d")) store.add(id);
        }
        try (FileMessageIdStore reopened = new FileMessageIdStore(file, 2, Duration.ofHours(1))) {
            assertFalse(reopened.contains("a"));
            assertFalse(reopened.contains("b"));
            assertTrue(reopened.contains("c"));
            assertTrue(reopened.contains("d"));
        }
    }

    /** A torn last line (a crash during a write) must not stop the receiver from starting. */
    @Test
    void fileStore_ignoresALineItCannotRead(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("ids.log");
        try (FileMessageIdStore store = new FileMessageIdStore(file, 10, Duration.ofHours(1))) {
            store.add("a");
        }
        Files.writeString(file, Files.readString(file) + "garbage-without-a-timestamp\n12");
        try (FileMessageIdStore reopened = new FileMessageIdStore(file, 10, Duration.ofHours(1))) {
            assertTrue(reopened.contains("a"));
        }
    }

    /** The file does not grow without bound while the process runs. */
    @Test
    void fileStore_staysBounded(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("ids.log");
        try (FileMessageIdStore store = new FileMessageIdStore(file, 5, Duration.ofHours(1))) {
            for (int i = 0; i < 500; i++) store.add("id-" + i);
            assertTrue(store.contains("id-499"));
            assertFalse(store.contains("id-100"));
        }
        long lines = Files.readAllLines(file).size();
        assertTrue(lines <= 20, "file has " + lines + " lines for a window of 5");
    }
}
