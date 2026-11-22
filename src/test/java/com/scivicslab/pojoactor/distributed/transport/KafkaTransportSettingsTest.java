package com.scivicslab.pojoactor.distributed.transport;

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
 * The program's own Kafka settings (TLS, SASL) reach the producer as they are, and the settings the
 * receipt depends on cannot be changed.
 */
@Tag("MutualTlsForActorCalls_261123_oo01")
@DisplayName("KafkaTransport — the program's TLS settings reach the producer")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class KafkaTransportSettingsTest {

    @Test
    @DisplayName("the producer keeps the TLS settings of the program and its own acknowledgement settings")
    void producer() {
        Properties tls = new Properties();
        tls.put("security.protocol", "SSL");
        tls.put("ssl.keystore.location", "/etc/kafka/keystore.p12");
        tls.put("ssl.endpoint.identification.algorithm", "https");

        Properties merged = KafkaTransport.producerProperties("kafka:9093", tls);

        assertEquals("SSL", merged.get("security.protocol"));
        assertEquals("/etc/kafka/keystore.p12", merged.get("ssl.keystore.location"));
        assertEquals("https", merged.get("ssl.endpoint.identification.algorithm"));
        assertEquals("kafka:9093", merged.get("bootstrap.servers"));
        assertEquals("all", merged.get("acks"));
        assertEquals("true", merged.get("enable.idempotence"));
    }

    @Test
    @DisplayName("a program that sets the acknowledgement or idempotence to another value is refused, naming the setting")
    void aFixedSettingCannotBeChanged() {
        Properties noAcks = new Properties();
        noAcks.put("acks", "0");
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> KafkaTransport.producerProperties("kafka:9093", noAcks));
        assertTrue(refused.getMessage().contains("acks"), refused.getMessage());

        Properties noIdempotence = new Properties();
        noIdempotence.put("enable.idempotence", "false");
        assertThrows(IllegalArgumentException.class,
                () -> KafkaTransport.producerProperties("kafka:9093", noIdempotence));
    }

    @Test
    @DisplayName("without settings of its own the program gets the same properties as before")
    void withoutProgramSettings() {
        assertEquals(KafkaTransport.producerProperties("kafka:9092"),
                KafkaTransport.producerProperties("kafka:9092", null));
    }
}
