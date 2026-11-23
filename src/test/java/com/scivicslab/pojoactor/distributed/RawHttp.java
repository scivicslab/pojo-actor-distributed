package com.scivicslab.pojoactor.distributed;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Writes an HTTP request by hand, so that a test controls every header and the way the body is framed
 * (the JDK's HttpClient sets some of them itself and refuses others), and reads the whole answer.
 */
final class RawHttp {

    private RawHttp() {
    }

    /** What the server answered: the status line, and everything after the blank line. */
    record Answer(String status, String body) {
        int code() {
            return Integer.parseInt(status.split(" ")[1]);
        }
    }

    /** Sends {@code head} (request line and headers, ending in a blank line) then {@code body}; reads to the end. */
    static Answer send(int port, String head, byte[] body, int readTimeoutMillis) throws IOException {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(readTimeoutMillis);
            OutputStream out = socket.getOutputStream();
            out.write(head.getBytes(StandardCharsets.ISO_8859_1));
            if (body != null) out.write(body);
            out.flush();
            return read(socket.getInputStream());
        }
    }

    /**
     * Reads one answer: the status line and headers, then as many body bytes as Content-Length says, or to
     * the end of the stream when it says nothing. A server may keep the connection open after answering
     * (it is still draining a request body it refused), so the end of the stream is not waited for.
     */
    static Answer read(InputStream in) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) >= 0) {
            head.write(b);
            byte[] so = head.toByteArray();
            int n = so.length;
            if (n >= 4 && so[n - 4] == '\r' && so[n - 3] == '\n' && so[n - 2] == '\r' && so[n - 1] == '\n') break;
        }
        String header = head.toString(StandardCharsets.ISO_8859_1);
        int lineEnd = header.indexOf("\r\n");
        String status = lineEnd < 0 ? header : header.substring(0, lineEnd);
        long length = -1;
        for (String line : header.split("\r\n")) {
            if (line.toLowerCase(java.util.Locale.ROOT).startsWith("content-length:")) {
                length = Long.parseLong(line.substring(line.indexOf(':') + 1).trim());
            }
        }
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        long remaining = length;
        while (remaining != 0) {
            int n = in.read(chunk, 0, remaining < 0 ? chunk.length : (int) Math.min(chunk.length, remaining));
            if (n < 0) break;
            body.write(chunk, 0, n);
            if (remaining > 0) remaining -= n;
        }
        return new Answer(status, body.toString(StandardCharsets.UTF_8));
    }

    /** A POST of {@code json} to {@code path} with the given Content-Type, Host and (when not null) Origin. */
    static Answer post(int port, String path, String host, String contentType, String origin, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        StringBuilder head = new StringBuilder("POST " + path + " HTTP/1.1\r\nHost: " + host + "\r\n");
        if (contentType != null) head.append("Content-Type: ").append(contentType).append("\r\n");
        if (origin != null) head.append("Origin: ").append(origin).append("\r\n");
        head.append("Content-Length: ").append(body.length).append("\r\nConnection: close\r\n\r\n");
        return send(port, head.toString(), body, 20000);
    }

    static String message(String actor, String action, String args, String id) {
        return new ActorMessage(actor, action, args, id, null).toJson();
    }
}
