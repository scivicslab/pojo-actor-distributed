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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A {@link MessageIdStore} that survives a restart: every id is appended to a file and forced to
 * disk before {@link #add} returns. On start the file is read back, what has aged out or does not
 * fit the count is dropped, and the file is rewritten.
 *
 * <p>A line is {@code epochMillis TAB url-encoded-id}. A line that cannot be read, such as a
 * torn last line after a crash, is skipped.</p>
 */
public class FileMessageIdStore implements MessageIdStore, AutoCloseable {

    private static final Logger logger = Logger.getLogger(FileMessageIdStore.class.getName());

    private final Path file;
    private final int maxEntries;
    private final Clock clock;
    private final InMemoryMessageIdStore memory;
    private FileChannel out;
    private int linesInFile;

    /**
     * @param file       where the ids are kept; created if missing
     * @param maxEntries how many ids to remember at most; must be positive
     * @param maxAge     how long to remember an id; must be positive
     */
    public FileMessageIdStore(Path file, int maxEntries, Duration maxAge) {
        this(file, maxEntries, maxAge, Clock.systemUTC());
    }

    FileMessageIdStore(Path file, int maxEntries, Duration maxAge, Clock clock) {
        this.file = file;
        this.maxEntries = maxEntries;
        this.clock = clock;
        this.memory = new InMemoryMessageIdStore(maxEntries, maxAge, clock);
        try {
            load();
            compact();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open message id file " + file, e);
        }
    }

    @Override
    public boolean contains(String messageId) {
        return memory.contains(messageId);
    }

    @Override
    public synchronized void add(String messageId) {
        long now = clock.millis();
        try {
            append(now, messageId);
            memory.addAt(messageId, now);
            if (linesInFile > 2 * maxEntries + 10) {
                compact();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write message id file " + file, e);
        }
    }

    @Override
    public synchronized void close() {
        try {
            if (out != null) out.close();
        } catch (IOException e) {
            logger.warning("Could not close " + file + ": " + e.getMessage());
        }
    }

    private void load() throws IOException {
        if (!Files.exists(file)) return;
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (String line : lines) {
            int tab = line.indexOf('\t');
            if (tab <= 0 || tab == line.length() - 1) continue;
            try {
                long millis = Long.parseLong(line.substring(0, tab));
                memory.addAt(URLDecoder.decode(line.substring(tab + 1), StandardCharsets.UTF_8), millis);
            } catch (IllegalArgumentException e) {
                // a line that cannot be read is skipped
            }
        }
    }

    // Rewrites the file with only what is still remembered, then reopens it for appending.
    private void compact() throws IOException {
        if (out != null) out.close();
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        StringBuilder sb = new StringBuilder();
        int n = 0;
        for (Map.Entry<String, Long> e : memory.snapshot().entrySet()) {
            sb.append(e.getValue()).append('\t').append(encode(e.getKey())).append('\n');
            n++;
        }
        Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        linesInFile = n;
        out = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    private void append(long millis, String id) throws IOException {
        byte[] line = (millis + "\t" + encode(id) + "\n").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.wrap(line);
        while (buf.hasRemaining()) out.write(buf);
        out.force(false);
        linesInFile++;
    }

    private static String encode(String id) {
        return URLEncoder.encode(id, StandardCharsets.UTF_8);
    }
}
