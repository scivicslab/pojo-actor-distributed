package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scivicslab.pojoactor.distributed.TestCertificates.Issued;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Reading who a certificate says its holder is: the SPIFFE ID first, then the common name. */
@Tag("MutualTlsForActorCalls_261123_oo01")
@DisplayName("PeerIdentity — the SPIFFE ID, else the common name")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PeerIdentityTest {

    private static Issued authority;

    @BeforeAll
    static void makeAuthority() throws Exception {
        authority = TestCertificates.newAuthority("test-authority");
    }

    private static CallerIdentity identityOf(String subject, List<String> uris) throws Exception {
        return PeerIdentity.of(TestCertificates.issue(authority, subject, uris, List.of(), List.of()).certificate());
    }

    @Test
    @DisplayName("a SPIFFE ID in the subject alternative name is the identity, even when the subject has a common name")
    void spiffeIdComesFirst() throws Exception {
        CallerIdentity identity = identityOf("CN=worker", List.of("spiffe://example.org/ns/app/sa/worker"));

        assertEquals(CallerIdentity.spiffeId("spiffe://example.org/ns/app/sa/worker"), identity);
        assertEquals(CallerIdentity.Kind.SPIFFE_ID, identity.kind());
    }

    @Test
    @DisplayName("without a SPIFFE ID the common name is the identity")
    void commonNameWhenThereIsNoSpiffeId() throws Exception {
        assertEquals(CallerIdentity.commonName("worker-7"), identityOf("CN=worker-7,O=Example,C=JP", List.of()));
    }

    @Test
    @DisplayName("the most specific common name is taken when the subject has several")
    void theMostSpecificCommonName() throws Exception {
        // BouncyCastle reads this string in the order of the name: the last CN is the most specific one
        assertEquals(CallerIdentity.commonName("inner"), identityOf("CN=outer,OU=part,CN=inner", List.of()));
    }

    @Test
    @DisplayName("a URI that is not a SPIFFE ID is ignored, and the common name is used")
    void aUriThatIsNotASpiffeIdIsIgnored() throws Exception {
        assertEquals(CallerIdentity.commonName("worker"),
                identityOf("CN=worker", List.of("https://example.org/not-spiffe")));
    }

    @Test
    @DisplayName("the SPIFFE ID is found among other names")
    void theSpiffeIdAmongOtherNames() throws Exception {
        assertEquals(CallerIdentity.spiffeId("spiffe://example.org/b"),
                identityOf("CN=x", List.of("https://example.org/a", "spiffe://example.org/b")));
    }

    @Test
    @DisplayName("a certificate with neither a SPIFFE ID nor a common name has no identity")
    void noIdentity() throws Exception {
        CallerIdentity identity = identityOf("O=Example", List.of());

        assertEquals(CallerIdentity.NONE, identity);
        assertFalse(identity.identified());
    }

    @Test
    @DisplayName("an identity is its kind and its value together, and NONE has no value")
    void anIdentityIsItsKindAndValue() {
        assertFalse(CallerIdentity.commonName("spiffe://x/y").equals(CallerIdentity.spiffeId("spiffe://x/y")));
        assertTrue(CallerIdentity.NONE.value().isEmpty());
        assertThrows(IllegalArgumentException.class, () -> new CallerIdentity(CallerIdentity.Kind.NONE, "x"));
        assertThrows(IllegalArgumentException.class, () -> new CallerIdentity(CallerIdentity.Kind.SPIFFE_ID, ""));
    }
}
