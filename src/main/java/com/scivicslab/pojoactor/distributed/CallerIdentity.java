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

import java.util.Objects;

/**
 * Who sent a call, as far as the receiver knows.
 *
 * <p>The identity is read from the certificate the caller presented in a mutual TLS handshake, after
 * the {@code SSLContext} that the program passed has accepted that certificate. It is a SPIFFE ID
 * when the certificate carries one (a URI in the subject alternative name that starts with
 * {@code spiffe://}), otherwise the common name (CN) of the certificate's subject, otherwise
 * {@link #NONE}. A call that arrives with no certificate to read, such as a call over plain HTTP on
 * the loopback address or a message from a Kafka inbox, has the identity {@link #NONE}.</p>
 *
 * <p>The kind is part of the identity: a certificate whose CN is the text of a SPIFFE ID is a
 * different caller from the certificate that holds that SPIFFE ID.</p>
 *
 * @param kind  where the value was read from
 * @param value the SPIFFE ID or the common name; empty only for {@link #NONE}
 * @since 5.0.0
 */
public record CallerIdentity(Kind kind, String value) {

    /** Where the value of an identity was read from. */
    public enum Kind {
        /** A {@code spiffe://} URI in the certificate's subject alternative name. */
        SPIFFE_ID,
        /** The CN of the certificate's subject, when the certificate has no SPIFFE ID. */
        COMMON_NAME,
        /** No identity: there was no certificate, or it carried neither a SPIFFE ID nor a CN. */
        NONE
    }

    /** The identity of a call that carries none. */
    public static final CallerIdentity NONE = new CallerIdentity(Kind.NONE, "");

    public CallerIdentity {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(value, "value");
        if ((kind == Kind.NONE) != value.isEmpty()) {
            throw new IllegalArgumentException("an identity has a value exactly when its kind is not NONE: "
                    + kind + " / '" + value + "'");
        }
    }

    /** The identity of the holder of this SPIFFE ID, such as {@code spiffe://example.org/ns/app/sa/worker}. */
    public static CallerIdentity spiffeId(String spiffeId) {
        return new CallerIdentity(Kind.SPIFFE_ID, spiffeId);
    }

    /** The identity of the holder of a certificate whose subject has this common name and no SPIFFE ID. */
    public static CallerIdentity commonName(String commonName) {
        return new CallerIdentity(Kind.COMMON_NAME, commonName);
    }

    /** Whether the receiver knows who sent the call. */
    public boolean identified() {
        return kind != Kind.NONE;
    }

    @Override
    public String toString() {
        return identified() ? kind + ":" + value : "NONE";
    }
}
