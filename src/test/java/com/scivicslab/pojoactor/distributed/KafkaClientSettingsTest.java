package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The program's own Kafka settings (TLS, SASL) reach the consumer as they are, and the settings the
 * receiver's promises depend on cannot be changed. The producer is tested in the transport package.
 */
@Tag("MutualTlsForActorCalls_261123_oo01")
@DisplayName("Kafka — the program's TLS settings reach the client")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KafkaClientSettingsTest {

    private static Properties tls() {
        Properties p = new Properties();
        p.put("security.protocol", "SSL");
        p.put("ssl.truststore.location", "/etc/kafka/truststore.p12");
        p.put("ssl.keystore.location", "/etc/kafka/keystore.p12");
        p.put("ssl.endpoint.identification.algorithm", "https");
        return p;
    }

    @Test
    @DisplayName("the consumer keeps the TLS settings of the program and commits by hand")
    void consumer() {
        Properties merged = KafkaActorServer.consumerProperties("kafka:9093", "me", tls());

        assertEquals("SSL", merged.get("security.protocol"));
        assertEquals("/etc/kafka/truststore.p12", merged.get("ssl.truststore.location"));
        assertEquals("false", merged.get("enable.auto.commit"));
        assertEquals("earliest", merged.get("auto.offset.reset"));
        assertEquals("pojo-actor-server-me", merged.get("group.id"));
    }

    @Test
    @DisplayName("a program that sets a setting the receiver depends on to another value is refused, naming it")
    void aFixedSettingCannotBeChanged() {
        Properties autoCommit = new Properties();
        autoCommit.put("enable.auto.commit", "true");
        IllegalArgumentException consumer = assertThrows(IllegalArgumentException.class,
                () -> KafkaActorServer.consumerProperties("kafka:9093", "me", autoCommit));
        assertTrue(consumer.getMessage().contains("enable.auto.commit"), consumer.getMessage());

    }

    @Test
    @DisplayName("setting a fixed setting to the value the library uses is accepted")
    void theSameValueIsAccepted() {
        Properties same = new Properties();
        same.put("enable.auto.commit", "false");

        assertEquals("false", KafkaActorServer.consumerProperties("kafka:9093", "me", same).get("enable.auto.commit"));
    }

    @Test
    @DisplayName("without settings of its own the program gets the same properties as before")
    void withoutProgramSettings() {
        assertEquals(KafkaActorServer.consumerProperties("kafka:9092", "me"),
                KafkaActorServer.consumerProperties("kafka:9092", "me", null));
    }
}
