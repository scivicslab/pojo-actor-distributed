package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The libraries on the classpath are at or above the versions that fix the advisories found in the 5.0.0
 * security review: jackson-databind 2.21.7 (polymorphic-typing, ignore/view and deserialization
 * advisories), org.json 20260719 (CVE-2026-59171, unbounded number in JSONTokener), kafka-clients 3.9.2
 * (arbitrary file read and SSRF through the client's SASL/JAAS configuration, privilege escalation to file
 * system reads, producer message corruption, sensitive values in DEBUG logs), the lz4-java of the
 * at.yawk fork at 1.11.4 or later (the original org.lz4 line, last release 1.8.1, has no fix for most of
 * its advisories) and snappy-java 1.1.10.9 (denial of service by crafted input).
 */
@DisplayName("Libraries are at the versions that fix the known advisories")
class DependencyVersionsTest {

    @Test
    void jacksonDatabind_isAtLeast_2_21_7() throws Exception {
        String v = version("com.fasterxml.jackson.core", "jackson-databind");
        assertTrue(compare(v, "2.21.7") >= 0, "jackson-databind " + v);
    }

    @Test
    void orgJson_isAtLeast_20260719() throws Exception {
        String v = version("org.json", "json");
        assertTrue(Long.parseLong(v) >= 20260719L, "org.json " + v);
    }

    @Test
    void kafkaClients_isAtLeast_3_9_2() throws Exception {
        String v = jarVersion(org.apache.kafka.clients.consumer.KafkaConsumer.class, "kafka-clients");
        assertTrue(compare(v, "3.9.2") >= 0, "kafka-clients " + v);
    }

    /**
     * kafka-clients brings it in at run time. The fork keeps the package and the artifact name, so the
     * version tells them apart: the original org.lz4 line ends at 1.8.1.
     */
    @Test
    void lz4Java_isTheMaintainedForkAtLeast_1_11_4() throws Exception {
        String v = jarVersion(net.jpountz.lz4.LZ4Factory.class, "lz4-java");
        assertTrue(compare(v, "1.11.4") >= 0, "at.yawk.lz4:lz4-java " + v);
    }

    @Test
    void snappyJava_isAtLeast_1_1_10_9() throws Exception {
        String v = jarVersion(org.xerial.snappy.Snappy.class, "snappy-java");
        assertTrue(compare(v, "1.1.10.9") >= 0, "snappy-java " + v);
    }

    /**
     * The version in the name of the jar that holds {@code type}, {@code <artifact>-<version>.jar} as Maven
     * names it. Some of these jars (Kafka's) carry no pom.properties.
     */
    private static String jarVersion(Class<?> type, String artifact) throws Exception {
        String file = java.nio.file.Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI())
                .getFileName().toString();
        String prefix = artifact + "-";
        assertTrue(file.startsWith(prefix) && file.endsWith(".jar"), "unexpected jar name " + file);
        return file.substring(prefix.length(), file.length() - ".jar".length());
    }

    /** The version Maven recorded in the jar, read from META-INF/maven/<group>/<artifact>/pom.properties. */
    private static String version(String group, String artifact) throws Exception {
        String path = "/META-INF/maven/" + group + "/" + artifact + "/pom.properties";
        try (InputStream in = DependencyVersionsTest.class.getResourceAsStream(path)) {
            assertNotNull(in, "not on the classpath: " + path);
            Properties p = new Properties();
            p.load(in);
            return p.getProperty("version");
        }
    }

    private static int compare(String a, String b) {
        String[] x = a.split("[.-]");
        String[] y = b.split("[.-]");
        for (int i = 0; i < Math.min(x.length, y.length); i++) {
            int c = Integer.compare(Integer.parseInt(x[i]), Integer.parseInt(y[i]));
            if (c != 0) return c;
        }
        return Integer.compare(x.length, y.length);
    }
}
