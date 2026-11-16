package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The one definition of the port an application publishes its actors on.
 *
 * <p>Both sides call it: the application that publishes, and the process that calls in. Writing
 * the arithmetic twice would let one side change and the other keep answering the old number,
 * which shows up only as a connection that is refused.
 */
@Tag("WorkflowTab_Port_260906_oo01")
@DisplayName("DistributedIIActorSystem — deriving the publication port")
class PublicationPortTest {

    @Test
    void isTheApplicationsOwnHttpPortPlusOneThousand() {
        assertEquals(29030, DistributedIIActorSystem.publicationPortFor(28030));
        assertEquals(29011, DistributedIIActorSystem.publicationPortFor(28011));
    }

    /** Two instances of the same application differ in their HTTP port, so they differ here too. */
    @Test
    void twoInstancesNeverLandOnTheSamePort() {
        assertEquals(1, DistributedIIActorSystem.publicationPortFor(28031)
                - DistributedIIActorSystem.publicationPortFor(28030));
    }

    /** A port that cannot exist must be refused here rather than at bind time. */
    @Test
    void refusesAnHttpPortThatWouldNotFit() {
        assertThrows(IllegalArgumentException.class, () -> DistributedIIActorSystem.publicationPortFor(0));
        assertThrows(IllegalArgumentException.class, () -> DistributedIIActorSystem.publicationPortFor(-1));
        assertThrows(IllegalArgumentException.class, () -> DistributedIIActorSystem.publicationPortFor(65000));
    }
}
