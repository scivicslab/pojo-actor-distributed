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

package com.scivicslab.pojoactor.distributed.transport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scivicslab.pojoactor.action.ActionResult;
import com.scivicslab.pojoactor.distributed.ActorMessage;
import com.scivicslab.pojoactor.distributed.NodeInfo;
import com.scivicslab.pojoactor.distributed.Receipt;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HTTP-based transport for inter-node actor communication.
 *
 * <p>Suited for HPC clusters (Slurm, Grid Engine) where nodes share the same network
 * and direct synchronous communication is acceptable.</p>
 *
 * <p>Sends HTTP POST requests to {@code http://{host}:{port}/actor/{actorName}/invoke}
 * and waits synchronously for the JSON response.</p>
 */
public class HttpTransport implements TransportLayer {

    private static final Logger logger = Logger.getLogger(HttpTransport.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final HttpClient httpClient;

    public HttpTransport() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Sends a message to {@code /actor/{name}/send} and returns the receipt: accepted when the
     * remote mailbox took it (2xx). Does not wait for the action to run. A refusal, an error
     * answer or an unreachable node gives a receipt that is not accepted; nothing is thrown.
     */
    @Override
    public Receipt send(NodeInfo target, ActorMessage message) {
        try {
            HttpRequest request = buildRequest(target, message, Duration.ofSeconds(30), "send");
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return Receipt.accepted(message.getMessageId());
            }
            return Receipt.refused(message.getMessageId(), "HTTP " + status + ": " + response.body());
        } catch (IOException e) {
            logger.log(Level.WARNING, "Failed to send message to " + target.getUrl(), e);
            return Receipt.refused(message.getMessageId(), "Network error: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Receipt.refused(message.getMessageId(), "Interrupted");
        }
    }

    /**
     * Sends a message and waits synchronously for the result.
     */
    @Override
    public ActionResult sendAndWait(NodeInfo target, ActorMessage message, Duration timeout) {
        try {
            HttpRequest request = buildRequest(target, message, timeout, "invoke");
            logger.fine(String.format("HTTP sendAndWait → %s actor=%s action=%s",
                    target.getUrl(), message.getActorName(), message.getActionName()));
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return parseResponse(response);
        } catch (IOException | InterruptedException e) {
            logger.log(Level.SEVERE, "HTTP transport error", e);
            return new ActionResult(false, "Network error: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        // HttpClient manages its own connection pool; nothing explicit to close
    }

    private HttpRequest buildRequest(NodeInfo target, ActorMessage message, Duration timeout, String verb) {
        String url = String.format("%s/actor/%s/%s", target.getUrl(), message.getActorName(), verb);
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofString(message.toJson()))
                .build();
    }

    private ActionResult parseResponse(HttpResponse<String> response) {
        try {
            JsonNode json = objectMapper.readTree(response.body());
            boolean success = json.get("success").asBoolean();
            String result = json.get("result").asText();
            return new ActionResult(success, result);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to parse HTTP response", e);
            return new ActionResult(false, "Failed to parse response: " + e.getMessage());
        }
    }
}
