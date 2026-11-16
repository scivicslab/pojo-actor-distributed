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
 * The answer to a {@code send}: whether the message was accepted, not whether it was run.
 *
 * <p>Over HTTP, accepted means the remote actor's mailbox took the message (a 2xx answer). Over
 * Kafka, accepted means the broker stored it in the inbox topic; the remote mailbox takes it
 * later, when the receiver reads it. A message that is not accepted may be sent again with the
 * same {@code messageId}.</p>
 *
 * @param accepted  true when the message was accepted
 * @param messageId the id of the message this receipt answers
 * @param detail    why it was not accepted; empty when it was
 */
public record Receipt(boolean accepted, String messageId, String detail) {

    public static Receipt accepted(String messageId) {
        return new Receipt(true, messageId, "");
    }

    public static Receipt refused(String messageId, String detail) {
        return new Receipt(false, messageId, detail);
    }
}
