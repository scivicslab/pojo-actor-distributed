package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.Delivery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A call handed straight to the {@link Dispatcher}, as both receivers do: first the allow-list alone, then the
 * allow-list behind the de-duplication that {@code DistributedIIActorSystem} builds by default. When
 * {@code dispatch} has returned, the call is in the actor's mailbox.
 */
@Tag("ActorMessageGuarantees_261123_oo01")
@Tag("DistributedActorSystemRedesign_261115_oo01")
@DisplayName("Remote-call guarantees - a call handed to the Dispatcher in the sender's own thread")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class DispatcherGuaranteesTest extends RemoteGuaranteesBase {

    @Override
    List<Delivery> deliveries() {
        return List.of(RemoteGuaranteeSupport.ALLOW_LIST_DISPATCH, RemoteGuaranteeSupport.DEDUP_DISPATCH);
    }
}
