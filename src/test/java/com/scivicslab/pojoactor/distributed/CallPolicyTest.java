package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scivicslab.pojoactor.distributed.AllowListDispatcher.AllowedAction;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Which identity may call which action of which actor. */
@Tag("MutualTlsForActorCalls_261123_oo01")
@DisplayName("CallPolicy — who may call what")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class CallPolicyTest {

    private static final CallerIdentity ALICE = CallerIdentity.spiffeId("spiffe://example.org/alice");
    private static final CallerIdentity BOB = CallerIdentity.commonName("bob");

    @Test
    @DisplayName("anyCaller lets every caller, identified or not, call the listed pairs and nothing else")
    void anyCaller() {
        CallPolicy policy = CallPolicy.anyCaller(Set.of(new AllowedAction("calc", "add")));

        assertTrue(policy.allows(ALICE, "calc", "add"));
        assertTrue(policy.allows(CallerIdentity.NONE, "calc", "add"));
        assertFalse(policy.allows(ALICE, "calc", "echo"));
        assertFalse(policy.allows(ALICE, "other", "add"));
    }

    @Test
    @DisplayName("perCaller lets each identity call only what is listed for it")
    void perCaller() {
        CallPolicy policy = CallPolicy.perCaller(Map.of(
                ALICE, Set.of(new AllowedAction("calc", "add")),
                BOB, Set.of(new AllowedAction("calc", "echo"))));

        assertTrue(policy.allows(ALICE, "calc", "add"));
        assertFalse(policy.allows(ALICE, "calc", "echo"));
        assertTrue(policy.allows(BOB, "calc", "echo"));
        assertFalse(policy.allows(BOB, "calc", "add"));
    }

    @Test
    @DisplayName("an identity that is not listed may call nothing, and neither may a caller with no identity by default")
    void unlistedAndUnidentifiedCallersMayCallNothing() {
        CallPolicy policy = CallPolicy.perCaller(Map.of(ALICE, Set.of(new AllowedAction("calc", "add"))));

        assertFalse(policy.allows(BOB, "calc", "add"));
        assertFalse(policy.allows(CallerIdentity.NONE, "calc", "add"));
    }

    @Test
    @DisplayName("a caller with no identity may call what is listed for it, and only that")
    void unidentifiedCallersMayCallWhatIsListedForThem() {
        CallPolicy policy = CallPolicy.perCaller(Map.of(ALICE, Set.of(new AllowedAction("calc", "add"))),
                Set.of(new AllowedAction("status", "get")));

        assertTrue(policy.allows(CallerIdentity.NONE, "status", "get"));
        assertFalse(policy.allows(CallerIdentity.NONE, "calc", "add"));
        assertFalse(policy.allows(BOB, "status", "get"), "an identified caller that is not listed is not 'no identity'");
    }

    @Test
    @DisplayName("a common name that spells a SPIFFE ID is not the holder of that SPIFFE ID")
    void aCommonNameIsNotASpiffeId() {
        CallPolicy policy = CallPolicy.perCaller(Map.of(ALICE, Set.of(new AllowedAction("calc", "add"))));

        assertFalse(policy.allows(CallerIdentity.commonName("spiffe://example.org/alice"), "calc", "add"));
    }
}
