package redis.server;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

/**
 * TLS from the PEM files Redis uses: a certificate chain, an unencrypted
 * PKCS#8 private key, and optionally a CA bundle to verify the other side.
 * Only TLS 1.2 and 1.3 are offered.
 */
public final class Tls {

    private static final String[] PROTOCOLS = {"TLSv1.3", "TLSv1.2"};

    private final SSLContext context;
    private final String authClients;

    private Tls(SSLContext context, String authClients) {
        this.context = context;
        this.authClients = authClients;
    }

    /** Null when the config doesn't use TLS at all. */
    public static Tls fromConfig(Config config) throws IOException {
        if (config.tlsPort() == 0 && !config.tlsReplication()) {
            return null;
        }
        try {
            KeyManagerFactory keys = null;
            if (config.tlsCertFile() != null) {
                KeyStore store = KeyStore.getInstance("PKCS12");
                store.load(null, null);
                List<Certificate> chain = certificates(Path.of(config.tlsCertFile()));
                if (chain.isEmpty()) {
                    throw new IOException(config.tlsCertFile() + " contains no certificate");
                }
                store.setKeyEntry("server", privateKey(Path.of(config.tlsKeyFile())), new char[0],
                        chain.toArray(new Certificate[0]));
                keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keys.init(store, new char[0]);
            }
            TrustManagerFactory trust = null;
            if (config.tlsCaCertFile() != null) {
                KeyStore store = KeyStore.getInstance("PKCS12");
                store.load(null, null);
                int i = 0;
                for (Certificate ca : certificates(Path.of(config.tlsCaCertFile()))) {
                    store.setCertificateEntry("ca" + i++, ca);
                }
                trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trust.init(store);
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys == null ? null : keys.getKeyManagers(), trust == null ? null : trust.getTrustManagers(), null);
            return new Tls(context, config.tlsAuthClients());
        } catch (GeneralSecurityException e) {
            throw new IOException("TLS setup failed: " + e.getMessage(), e);
        }
    }

    /** A listening socket that asks for (or requires) client certificates according to tls-auth-clients. */
    public SSLServerSocket serverSocket() throws IOException {
        SSLServerSocket socket = (SSLServerSocket) context.getServerSocketFactory().createServerSocket();
        SSLParameters params = socket.getSSLParameters();
        params.setProtocols(PROTOCOLS);
        params.setNeedClientAuth(authClients.equals("yes"));
        if (authClients.equals("optional")) {
            params.setWantClientAuth(true);
        }
        socket.setSSLParameters(params);
        return socket;
    }

    /**
     * An unconnected client socket that checks the server's certificate,
     * including that it was issued for the host name (or IP) it's connected to.
     */
    public SSLSocket clientSocket() throws IOException {
        SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket();
        SSLParameters params = socket.getSSLParameters();
        params.setProtocols(PROTOCOLS);
        params.setEndpointIdentificationAlgorithm("HTTPS");
        socket.setSSLParameters(params);
        return socket;
    }

    private static List<Certificate> certificates(Path pem) throws IOException, GeneralSecurityException {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        Collection<? extends Certificate> certs = factory.generateCertificates(
                new ByteArrayInputStream(Files.readAllBytes(pem)));
        return List.copyOf(certs);
    }

    private static PrivateKey privateKey(Path pem) throws IOException, GeneralSecurityException {
        String text = Files.readString(pem, StandardCharsets.US_ASCII);
        if (text.contains("BEGIN ENCRYPTED PRIVATE KEY")) {
            throw new IOException(pem + " is encrypted; give an unencrypted key and protect the file instead");
        }
        if (!text.contains("-----BEGIN PRIVATE KEY-----")) {
            throw new IOException(pem + " isn't a PKCS#8 key (BEGIN PRIVATE KEY). Convert it with:"
                    + " openssl pkcs8 -topk8 -nocrypt -in old.key -out new.key");
        }
        String base64 = text.substring(text.indexOf("-----BEGIN PRIVATE KEY-----") + 27, text.indexOf("-----END PRIVATE KEY-----"))
                .replaceAll("\\s", "");
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(base64));
        for (String algorithm : new String[] {"RSA", "EC", "Ed25519", "Ed448"}) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(spec);
            } catch (GeneralSecurityException wrongAlgorithm) {
                // try the next one
            }
        }
        throw new IOException(pem + ": unsupported private key type");
    }
}
