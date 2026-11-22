package com.scivicslab.pojoactor.distributed;

import com.scivicslab.pojoactor.distributed.RemoteGuaranteeSupport.Delivery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A call that arrives as a record in the Kafka receiver's inbox, read by {@code KafkaActorServer} (a
 * {@code MockConsumer}, so no broker is needed). The receiver hands each record to the dispatcher in the order the
 * records were read, and the actions must then run in that order, one at a time.
 */
@Tag("ActorMessageGuarantees_261123_oo01")
@Tag("DistributedActorSystemRedesign_261115_oo01")
@DisplayName("Remote-call guarantees - a record in the Kafka inbox")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class KafkaInboxGuaranteesTest extends RemoteGuaranteesBase {

    @Override
    List<Delivery> deliveries() {
        return List.of(RemoteGuaranteeSupport.KAFKA);
    }
}
