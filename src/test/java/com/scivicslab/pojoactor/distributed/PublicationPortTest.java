package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The one definition of the port an application publishes its actors on.
 *
 * <p>Both sides call it: the application that publishes, and the process that calls in. Writing
 * the arithmetic twice would let one side change and the other keep answering the old number,
 * which shows up only as a connection that is refused.
 */
@DisplayName("DistributedActorSystem — deriving the publication port")
class PublicationPortTest {

    @Test
    void isTheApplicationsOwnHttpPortPlusOneThousand() {
        assertEquals(29030, DistributedActorSystem.publicationPortFor(28030));
        assertEquals(29011, DistributedActorSystem.publicationPortFor(28011));
    }

    /** Two instances of the same application differ in their HTTP port, so they differ here too. */
    @Test
    void twoInstancesNeverLandOnTheSamePort() {
        assertEquals(1, DistributedActorSystem.publicationPortFor(28031)
                - DistributedActorSystem.publicationPortFor(28030));
    }

    /** A port that cannot exist must be refused here rather than at bind time. */
    @Test
    void refusesAnHttpPortThatWouldNotFit() {
        assertThrows(IllegalArgumentException.class, () -> DistributedActorSystem.publicationPortFor(0));
        assertThrows(IllegalArgumentException.class, () -> DistributedActorSystem.publicationPortFor(-1));
        assertThrows(IllegalArgumentException.class, () -> DistributedActorSystem.publicationPortFor(65000));
    }
}
