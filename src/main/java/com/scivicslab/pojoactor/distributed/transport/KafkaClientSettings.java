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

import java.util.Map;
import java.util.Properties;

/**
 * Combines the Kafka client settings the library needs with the settings of the program that uses it.
 *
 * <p>The program passes its own settings, such as {@code security.protocol}, {@code ssl.truststore.location},
 * {@code ssl.keystore.location} or {@code sasl.*}, and they reach the Kafka client as they are. The library
 * keeps a few settings for itself, because what it promises depends on them: the serialisers, the
 * acknowledgement and idempotence of the producer, the consumer group, the starting position and the
 * manual commit of the consumer. A program that sets one of those to another value gets an
 * {@link IllegalArgumentException} naming the setting, not a client that quietly works differently.</p>
 *
 * <p>How the broker is secured (TLS, mutual TLS, SASL, access control lists) is decided by the program
 * and the platform; this class only carries the settings.</p>
 *
 * @since 5.0.0
 */
public final class KafkaClientSettings {

    private KafkaClientSettings() {
    }

    /**
     * @param required the settings the library depends on
     * @param fromProgram the settings of the program, or null for none
     * @return the program's settings plus the required ones
     * @throws IllegalArgumentException if the program sets a required setting to another value
     */
    public static Properties merge(Properties required, Properties fromProgram) {
        Properties merged = new Properties();
        if (fromProgram != null) {
            for (Map.Entry<Object, Object> entry : fromProgram.entrySet()) {
                merged.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            for (String key : required.stringPropertyNames()) {
                Object own = merged.get(key);
                if (own != null && !String.valueOf(own).equals(required.getProperty(key))) {
                    throw new IllegalArgumentException("The Kafka setting '" + key + "' is fixed by the library at '"
                            + required.getProperty(key) + "' and cannot be set to '" + own + "'");
                }
            }
        }
        for (String key : required.stringPropertyNames()) {
            merged.put(key, required.getProperty(key));
        }
        return merged;
    }
}
