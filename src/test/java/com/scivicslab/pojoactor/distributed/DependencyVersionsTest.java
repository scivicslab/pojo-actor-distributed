package com.scivicslab.pojoactor.distributed;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The JSON libraries on the classpath are at or above the versions that fix the advisories found in the
 * 5.0.0 security review: jackson-databind 2.21.7 (polymorphic-typing, ignore/view and deserialization
 * advisories) and org.json 20260719 (CVE-2026-59171, unbounded number in JSONTokener).
 */
@DisplayName("JSON libraries are at the versions that fix the known advisories")
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
