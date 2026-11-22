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

import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

/**
 * Reads the identity of the peer of a TLS connection from its certificate.
 *
 * <p>The certificate is taken as already accepted: the {@code SSLContext} that the program passed
 * to the server has checked the chain against its trust anchors before any call is read. This class
 * only reads who the certificate says its holder is. The SPIFFE ID comes first (a URI in the
 * subject alternative name that starts with {@code spiffe://}); the common name of the subject
 * comes next; a certificate with neither gives {@link CallerIdentity#NONE}.</p>
 *
 * @since 5.0.0
 */
public final class PeerIdentity {

    private static final int SAN_URI = 6;
    private static final String SPIFFE_SCHEME = "spiffe://";

    private PeerIdentity() {
    }

    /**
     * The identity of the holder of {@code certificate}.
     *
     * @param certificate the certificate the peer presented
     * @return its SPIFFE ID, else its common name, else {@link CallerIdentity#NONE}
     */
    public static CallerIdentity of(X509Certificate certificate) {
        String spiffeId = spiffeId(certificate);
        if (spiffeId != null) {
            return CallerIdentity.spiffeId(spiffeId);
        }
        String commonName = commonName(certificate);
        if (commonName != null) {
            return CallerIdentity.commonName(commonName);
        }
        return CallerIdentity.NONE;
    }

    /**
     * The identity of the peer of {@code session}, read from the first certificate it presented.
     *
     * @param session the TLS session of the connection
     * @return the peer's identity; {@link CallerIdentity#NONE} when it presented no certificate
     */
    public static CallerIdentity of(SSLSession session) {
        try {
            Certificate[] chain = session.getPeerCertificates();
            if (chain.length > 0 && chain[0] instanceof X509Certificate certificate) {
                return of(certificate);
            }
        } catch (SSLPeerUnverifiedException e) {
            // the peer presented no certificate
        }
        return CallerIdentity.NONE;
    }

    private static String spiffeId(X509Certificate certificate) {
        Collection<List<?>> names;
        try {
            names = certificate.getSubjectAlternativeNames();
        } catch (CertificateParsingException e) {
            return null;
        }
        if (names == null) {
            return null;
        }
        for (List<?> name : names) {
            if (name.size() == 2 && name.get(0) instanceof Integer type && type == SAN_URI
                    && name.get(1) instanceof String uri && uri.startsWith(SPIFFE_SCHEME)
                    && uri.length() > SPIFFE_SCHEME.length()) {
                return uri;
            }
        }
        return null;
    }

    private static String commonName(X509Certificate certificate) {
        try {
            LdapName subject = new LdapName(certificate.getSubjectX500Principal().getName(X500Principal.RFC2253));
            // getRdns() lists the rightmost RDN first; the most specific CN is the leftmost
            for (int i = subject.size() - 1; i >= 0; i--) {
                Rdn rdn = subject.getRdn(i);
                if ("CN".equalsIgnoreCase(rdn.getType()) && rdn.getValue() != null
                        && !rdn.getValue().toString().isEmpty()) {
                    return rdn.getValue().toString();
                }
            }
        } catch (InvalidNameException e) {
            // a subject that cannot be read has no common name
        }
        return null;
    }
}
