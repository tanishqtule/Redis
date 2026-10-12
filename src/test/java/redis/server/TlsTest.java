package redis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import redis.protocol.RespClient;

/** TLS with certificates made by openssl for each run. Skipped where openssl isn't installed. */
class TlsTest {

    @TempDir
    static Path certs;

    private final List<AutoCloseable> cleanup = new ArrayList<>();

    @BeforeAll
    static void makeCertificates() throws Exception {
        assumeTrue(onPath("openssl"), "openssl not installed");
        ca("ca");
        ca("other-ca");
        leaf("server", "ca", "subjectAltName=DNS:localhost,IP:127.0.0.1");
        leaf("client", "ca", "subjectAltName=DNS:client");
        leaf("impostor", "other-ca", "subjectAltName=DNS:localhost,IP:127.0.0.1");
    }

    @AfterEach
    void stopAll() throws Exception {
        for (int i = cleanup.size() - 1; i >= 0; i--) {
            cleanup.get(i).close();
        }
    }

    private RedisServer start(Config config) throws Exception {
        RedisServer server = new RedisServer(config);
        server.start();
        cleanup.add(server);
        return server;
    }

    private static String file(String name) {
        return certs.resolve(name).toString();
    }

    private static Config tlsServer(String cert) throws IOException {
        return Config.defaults().withPort(0).with("tls-port", Integer.toString(freePort()))
                .with("tls-cert-file", file(cert + ".pem")).with("tls-key-file", file(cert + ".key"));
    }

    /** A client that trusts {@code ca}, optionally presenting the client certificate. */
    private RespClient tlsClient(int port, String ca, boolean withClientCert) throws IOException {
        Config clientSide = Config.defaults().with("tls-replication", "yes").with("tls-ca-cert-file", file(ca + ".pem"));
        if (withClientCert) {
            clientSide = clientSide.with("tls-cert-file", file("client.pem")).with("tls-key-file", file("client.key"));
        }
        RespClient c = new RespClient(Tls.fromConfig(clientSide).clientSocket(), "127.0.0.1", port, 5000);
        cleanup.add(c);
        return c;
    }

    @Test
    void servesClientsOverTls() throws Exception {
        RedisServer server = start(tlsServer("server"));
        RespClient c = tlsClient(server.tlsPort(), "ca", false);
        assertEquals("OK", c.call("SET", "k", "secret"));
        assertEquals("secret", c.call("GET", "k"));
    }

    @Test
    void plainTextPortCanBeTurnedOff() throws Exception {
        RedisServer server = start(tlsServer("server").with("port", "0"));
        assertEquals(server.tlsPort(), server.port(), "the only listener is the TLS one");
        try (Socket plain = new Socket("127.0.0.1", server.tlsPort())) {
            plain.setSoTimeout(3000);
            plain.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes());
            assertTrue(plain.getInputStream().read() != '+', "a plain-text client mustn't get a normal reply");
        } catch (IOException expected) {
            // also fine: the handshake failure closed the connection
        }
    }

    @Test
    void canRequireClientCertificates() throws Exception {
        RedisServer server = start(tlsServer("server").with("tls-auth-clients", "yes")
                .with("tls-ca-cert-file", file("ca.pem")));
        assertThrows(IOException.class, () -> tlsClient(server.tlsPort(), "ca", false).call("PING"));
        assertEquals("PONG", tlsClient(server.tlsPort(), "ca", true).call("PING"));
    }

    @Test
    void clientsRefuseAServerTheyDontTrust() throws Exception {
        RedisServer server = start(tlsServer("impostor"));
        assertThrows(IOException.class, () -> tlsClient(server.tlsPort(), "ca", false).call("PING"));
    }

    @Test
    void replicatesOverTlsAndRefusesAnImpostorMaster() throws Exception {
        RedisServer master = start(tlsServer("server").with("requirepass", "pw"));
        RespClient onMaster = tlsClient(master.tlsPort(), "ca", false);
        onMaster.call("AUTH", "pw");
        onMaster.call("SET", "k", "v1");

        RedisServer replica = start(Config.defaults().withPort(0).withMaster("127.0.0.1", master.tlsPort())
                .with("masterauth", "pw").with("tls-replication", "yes").with("tls-ca-cert-file", file("ca.pem")));
        RespClient onReplica = new RespClient(replica.port());
        cleanup.add(onReplica);
        awaitValue(onReplica, "k", "v1");
        onReplica.call("CLIENT", "CONSISTENCY", "LINEARIZABLE");
        onMaster.call("SET", "k", "v2");
        assertEquals("v2", onReplica.call("GET", "k"), "read index requests go over TLS too");

        RedisServer impostor = start(tlsServer("impostor"));
        RedisServer fooled = start(Config.defaults().withPort(0).withMaster("127.0.0.1", impostor.tlsPort())
                .with("tls-replication", "yes").with("tls-ca-cert-file", file("ca.pem")));
        Thread.sleep(1500);
        try (RespClient c = new RespClient(fooled.port())) {
            assertNotEquals("connected", ((List<?>) c.call("ROLE")).get(3));
        }
    }

    @Test
    void explainsKeysInTheWrongFormat() throws Exception {
        Path pkcs1 = certs.resolve("rsa-pkcs1.key");
        Files.writeString(pkcs1, "-----BEGIN RSA PRIVATE KEY-----\nMIIB\n-----END RSA PRIVATE KEY-----\n");
        Config config = Config.defaults().with("tls-port", Integer.toString(freePort()))
                .with("tls-cert-file", file("server.pem")).with("tls-key-file", pkcs1.toString());
        IOException e = assertThrows(IOException.class, () -> new RedisServer(config));
        assertTrue(e.getMessage().contains("openssl pkcs8"), e.getMessage());
    }

    private static void awaitValue(RespClient c, String key, String expected) throws Exception {
        for (int i = 0; i < 100; i++) {
            if (expected.equals(c.call("GET", key))) {
                return;
            }
            Thread.sleep(50);
        }
        fail(key + " never became " + expected);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void ca(String name) throws Exception {
        openssl("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
                "-keyout", file(name + ".key"), "-out", file(name + ".pem"), "-days", "2", "-subj", "/CN=" + name);
    }

    private static void leaf(String name, String ca, String san) throws Exception {
        openssl("req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
                "-keyout", file(name + ".key"), "-out", file(name + ".csr"), "-subj", "/CN=" + name);
        Files.writeString(certs.resolve(name + ".ext"), san + "\n");
        openssl("x509", "-req", "-in", file(name + ".csr"), "-CA", file(ca + ".pem"), "-CAkey", file(ca + ".key"),
                "-CAcreateserial", "-out", file(name + ".pem"), "-days", "2", "-extfile", file(name + ".ext"));
    }

    private static void openssl(String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("openssl");
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String output = new String(p.getInputStream().readAllBytes());
        if (p.waitFor() != 0) {
            throw new IOException("openssl " + String.join(" ", args) + " failed:\n" + output);
        }
    }

    private static boolean onPath(String exe) {
        String path = System.getenv("PATH");
        if (path == null) {
            return false;
        }
        for (String dir : path.split(File.pathSeparator)) {
            if (Files.isExecutable(Path.of(dir, exe))) {
                return true;
            }
        }
        return false;
    }
}
