package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.Delivery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A call sent with {@code RemoteActorRef.send} to an {@code HttpActorServer} on the loopback address. The answer
 * is the receipt, 202 when the call is in the actor's mailbox, before the action has run, so a sender that
 * waits for each receipt before it sends the next one has its calls start in that order.
 */
@Tag("ActorMessageGuarantees_261123_oo01")
@Tag("DistributedActorSystemRedesign_261115_oo01")
@DisplayName("Remote-call guarantees - send over HTTP")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class HttpSendGuaranteesTest extends RemoteGuaranteesBase {

    @Override
    List<Delivery> deliveries() {
        return List.of(RemoteGuaranteeSupport.HTTP_SEND);
    }
}
