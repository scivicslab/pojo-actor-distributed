package com.scivicslab.pojoactor.distributed;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * The broker pod, seen through the Kubernetes API with the build pod's ServiceAccount (which may
 * get, list and delete pods in its own namespace, nothing else). Plain {@link HttpClient}; the
 * cluster's CA and the token are the files Kubernetes mounts into every pod.
 */
final class BrokerPod {

    private static final Path SA = Path.of("/var/run/secrets/kubernetes.io/serviceaccount");

    private final HttpClient http;
    private final String token;
    private final String base;
    final String name;

    BrokerPod(String name) throws Exception {
        this.name = name;
        String namespace = Files.readString(SA.resolve("namespace")).trim();
        token = Files.readString(SA.resolve("token")).trim();
        base = "https://" + System.getenv("KUBERNETES_SERVICE_HOST") + ":" + System.getenv("KUBERNETES_SERVICE_PORT")
                + "/api/v1/namespaces/" + namespace + "/pods/";
        http = HttpClient.newBuilder().sslContext(clusterTrust()).connectTimeout(Duration.ofSeconds(10)).build();
    }

    private static SSLContext clusterTrust() throws Exception {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        try (InputStream in = Files.newInputStream(SA.resolve("ca.crt"))) {
            int i = 0;
            for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                ks.setCertificateEntry("ca" + i++, c);
            }
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    private HttpResponse<String> call(String method) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(base + name))
                .header("Authorization", "Bearer " + token)
                .timeout(Duration.ofSeconds(15))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    /** The pod as the API returns it; null when there is no pod of that name. */
    JSONObject get() throws Exception {
        HttpResponse<String> r = call("GET");
        if (r.statusCode() == 404) return null;
        if (r.statusCode() != 200) throw new IllegalStateException("GET pod " + name + ": HTTP " + r.statusCode() + " " + r.body());
        return new JSONObject(r.body());
    }

    /** Deletes the pod (its StatefulSet creates a new one with the same name). */
    void delete() throws Exception {
        HttpResponse<String> r = call("DELETE");
        if (r.statusCode() != 200 && r.statusCode() != 202) {
            throw new IllegalStateException("DELETE pod " + name + ": HTTP " + r.statusCode() + " " + r.body());
        }
    }

    static String uid(JSONObject pod) {
        return pod == null ? null : pod.getJSONObject("metadata").getString("uid");
    }

    static boolean ready(JSONObject pod) {
        if (pod == null || pod.getJSONObject("metadata").has("deletionTimestamp")) return false;
        JSONObject status = pod.optJSONObject("status");
        JSONArray conditions = status == null ? null : status.optJSONArray("conditions");
        if (conditions == null) return false;
        for (int i = 0; i < conditions.length(); i++) {
            JSONObject c = conditions.getJSONObject(i);
            if ("Ready".equals(c.optString("type"))) return "True".equals(c.optString("status"));
        }
        return false;
    }
}
