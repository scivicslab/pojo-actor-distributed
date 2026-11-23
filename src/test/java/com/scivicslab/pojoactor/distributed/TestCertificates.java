package com.scivicslab.pojoactor.distributed;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Throw-away certificates for the mutual TLS tests: a certificate authority that exists only inside
 * the test, certificates it issues, and the {@code SSLContext} a node would build from one of them.
 * Nothing here is kept after the test, and no real CA or key is touched.
 */
final class TestCertificates {

    private static final char[] PASSWORD = "test".toCharArray();
    private static final SecureRandom RANDOM = new SecureRandom();

    private TestCertificates() {
    }

    /** A key and the certificate that holds its public half. */
    record Issued(PrivateKey key, X509Certificate certificate, X509Certificate issuer) {
        /** The chain a peer is shown: this certificate, then (for a CA-issued one) nothing more is needed. */
        X509Certificate[] chain() {
            return new X509Certificate[] {certificate};
        }
    }

    /** A new certificate authority: a key and a self-signed certificate that may sign others. */
    static Issued newAuthority(String commonName) throws Exception {
        KeyPair pair = keyPair();
        X500Name name = new X500Name("CN=" + commonName);
        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                name, serial(), notBefore(), notAfter(), name, pair.getPublic());
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
        X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withECDSA").build(pair.getPrivate())));
        return new Issued(pair.getPrivate(), certificate, certificate);
    }

    /**
     * A certificate for a node, signed by {@code authority}, good for both ends of a TLS connection.
     *
     * @param subject    the subject, for example {@code "CN=worker"}; the common name is the identity of
     *                   a certificate that has no SPIFFE ID
     * @param spiffeIds  URIs for the subject alternative name; the SPIFFE ID of the node
     * @param dnsNames   host names the node is reached at
     * @param ipAddresses IP addresses the node is reached at
     */
    static Issued issue(Issued authority, String subject, List<String> spiffeIds, List<String> dnsNames,
                        List<String> ipAddresses) throws Exception {
        KeyPair pair = keyPair();
        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                new X500Name(authority.certificate().getSubjectX500Principal().getName()),
                serial(), notBefore(), notAfter(), new X500Name(subject), pair.getPublic());
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
        builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyAgreement));
        builder.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(
                new KeyPurposeId[] {KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth}));
        List<GeneralName> names = new ArrayList<>();
        for (String uri : spiffeIds) names.add(new GeneralName(GeneralName.uniformResourceIdentifier, uri));
        for (String dns : dnsNames) names.add(new GeneralName(GeneralName.dNSName, dns));
        for (String ip : ipAddresses) names.add(new GeneralName(GeneralName.iPAddress, ip));
        if (!names.isEmpty()) {
            builder.addExtension(Extension.subjectAlternativeName, false,
                    new GeneralNames(names.toArray(new GeneralName[0])));
        }
        X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withECDSA").build(authority.key())));
        return new Issued(pair.getPrivate(), certificate, authority.certificate());
    }

    /** A node's certificate for a server reached on this machine: {@code localhost} and {@code 127.0.0.1}. */
    static Issued issueOnLoopback(Issued authority, String subject, List<String> spiffeIds) throws Exception {
        return issue(authority, subject, spiffeIds, List.of("localhost"), List.of("127.0.0.1"));
    }

    /**
     * The {@code SSLContext} a node builds from its own key and certificate and the authorities it trusts,
     * as a program would build it from what its platform gave it.
     */
    static SSLContext sslContext(Issued own, Issued... trustedAuthorities) throws Exception {
        KeyManagerFactory keys = null;
        if (own != null) {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, null);
            keyStore.setKeyEntry("node", own.key(), PASSWORD, own.chain());
            keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(keyStore, PASSWORD);
        }
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(null, null);
        int i = 0;
        for (Issued authority : trustedAuthorities) {
            trustStore.setCertificateEntry("ca" + i++, authority.certificate());
        }
        TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(trustStore);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys == null ? null : keys.getKeyManagers(), trust.getTrustManagers(), RANDOM);
        return context;
    }

    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(256, RANDOM);
        return generator.generateKeyPair();
    }

    private static BigInteger serial() {
        return new BigInteger(64, RANDOM).abs().add(BigInteger.ONE);
    }

    private static Date notBefore() {
        return new Date(System.currentTimeMillis() - 60_000);
    }

    private static Date notAfter() {
        return new Date(System.currentTimeMillis() + 3_600_000);
    }
}
