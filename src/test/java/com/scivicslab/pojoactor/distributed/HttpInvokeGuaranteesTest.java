package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.Delivery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A call made with {@code RemoteActorRef.callByActionName} to an {@code HttpActorServer} on the loopback address.
 * The answer is the result, after the action has run. The server takes each request on a virtual thread of its
 * own; the action must still run on the actor's receive loop, one at a time.
 */
@Tag("ActorMessageGuarantees_261123_oo01")
@Tag("DistributedActorSystemRedesign_261115_oo01")
@DisplayName("Remote-call guarantees - invoke over HTTP")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class HttpInvokeGuaranteesTest extends RemoteGuaranteesBase {

    @Override
    List<Delivery> deliveries() {
        return List.of(RemoteGuaranteeSupport.HTTP_INVOKE);
    }
}
