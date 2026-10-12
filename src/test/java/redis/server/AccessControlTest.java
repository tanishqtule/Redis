package redis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import redis.protocol.RespClient;
import redis.protocol.RespError;

class AccessControlTest {

    private final List<AutoCloseable> cleanup = new ArrayList<>();

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

    private RespClient client(int port) throws Exception {
        RespClient c = new RespClient(port);
        cleanup.add(c);
        return c;
    }

    @Test
    void everythingButAuthNeedsThePassword() throws Exception {
        RedisServer server = start(Config.defaults().withPort(0).with("requirepass", "s3cret"));
        RespClient c = client(server.port());

        assertEquals(new RespError("NOAUTH Authentication required."), c.call("GET", "k"));
        assertEquals(new RespError("NOAUTH Authentication required."), c.call("PSYNC", "?", "-1"));
        // Not even unknown commands are revealed before AUTH.
        assertEquals(new RespError("NOAUTH Authentication required."), c.call("NOSUCHTHING"));

        RespError wrong = assertInstanceOf(RespError.class, c.call("AUTH", "guess"));
        assertEquals("WRONGPASS", wrong.code());
        assertEquals("WRONGPASS", ((RespError) c.call("AUTH", "admin", "s3cret")).code());
        assertEquals("OK", c.call("AUTH", "s3cret"));
        assertEquals("OK", c.call("SET", "k", "v"));

        RespClient other = client(server.port());
        assertEquals("OK", other.call("AUTH", "default", "s3cret"));
        assertEquals("v", other.call("GET", "k"));
    }

    @Test
    void authWithoutAPasswordConfiguredIsAnError() throws Exception {
        RedisServer server = start(Config.defaults().withPort(0));
        RespError err = assertInstanceOf(RespError.class, client(server.port()).call("AUTH", "anything"));
        assertTrue(err.message().contains("without any password configured"), err.message());
    }

    /** Anonymous clients can't make the server buffer big requests (cf. CVE-2021-32675). */
    @Test
    void unauthenticatedClientsCantSendLargeRequests() throws Exception {
        RedisServer server = start(Config.defaults().withPort(0).with("requirepass", "s3cret"));
        try (Socket s = new Socket("127.0.0.1", server.port())) {
            s.setSoTimeout(5000);
            s.getOutputStream().write("*2\r\n$4\r\nAUTH\r\n$1000000\r\n".getBytes(StandardCharsets.US_ASCII));
            String reply = new String(s.getInputStream().readNBytes(60), StandardCharsets.US_ASCII);
            assertTrue(reply.startsWith("-ERR Protocol error: unauthenticated bulk length"), reply);
            assertEquals(-1, s.getInputStream().read(), "the server should hang up");
        }
        RespClient c = client(server.port());
        c.call("AUTH", "s3cret");
        String big = "x".repeat(1_000_000);
        assertEquals("OK", c.call("SET", "k", big));
        assertEquals(big, c.call("GET", "k"));
    }

    @Test
    void protectedModeOnlyAcceptsLoopbackWithoutAPassword() throws Exception {
        InetAddress external = nonLoopbackAddress();
        assumeTrue(external != null, "no non-loopback address to test from");

        RedisServer open = start(Config.defaults().withPort(0));
        assertEquals("PONG", client(open.port()).call("PING"), "loopback is fine");
        String reply = rawFirstReply(external, open.port());
        assertTrue(reply.startsWith("-DENIED"), reply);

        RedisServer withPassword = start(Config.defaults().withPort(0).with("requirepass", "s3cret"));
        assertTrue(rawFirstReply(external, withPassword.port()).startsWith("-NOAUTH"),
                "with a password, remote clients may connect and must AUTH");

        RedisServer trusted = start(Config.defaults().withPort(0).with("protected-mode", "no"));
        assertTrue(rawFirstReply(external, trusted.port()).startsWith("+PONG"));
    }

    @Test
    void bindRestrictsTheInterface() throws Exception {
        InetAddress external = nonLoopbackAddress();
        assumeTrue(external != null, "no non-loopback address to test from");
        RedisServer server = start(Config.defaults().withPort(0).with("bind", "127.0.0.1"));
        assertEquals("PONG", client(server.port()).call("PING"));
        assertThrows(IOException.class, () -> new Socket(external, server.port()).close());
    }

    @Test
    void maxclientsTurnsAwayTheExtraConnection() throws Exception {
        RedisServer server = start(Config.defaults().withPort(0).with("maxclients", "3"));
        List<RespClient> held = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            RespClient c = client(server.port());
            assertEquals("PONG", c.call("PING"));
            held.add(c);
        }
        RespClient fourth = client(server.port());
        assertEquals(new RespError("ERR max number of clients reached"), fourth.read());
        held.get(0).close();
        Thread.sleep(200);
        assertEquals("PONG", client(server.port()).call("PING"), "a slot frees up when someone leaves");
    }

    @Test
    void idleClientsAreDisconnectedAfterTheTimeout() throws Exception {
        RedisServer server = start(Config.defaults().withPort(0).with("timeout", "1"));
        try (Socket s = new Socket("127.0.0.1", server.port())) {
            s.setSoTimeout(5000);
            long started = System.nanoTime();
            assertEquals(-1, s.getInputStream().read(), "server should close an idle connection");
            long waited = (System.nanoTime() - started) / 1_000_000;
            assertTrue(waited >= 900 && waited < 4000, "closed after " + waited + " ms");
        }
    }

    @Test
    void replicationWorksWithPasswordsAndFailsWithTheWrongOne() throws Exception {
        RedisServer master = start(Config.defaults().withPort(0).with("requirepass", "m4ster"));
        RespClient onMaster = client(master.port());
        onMaster.call("AUTH", "m4ster");
        onMaster.call("SET", "k", "v");

        RedisServer good = start(Config.defaults().withPort(0).withMaster("127.0.0.1", master.port())
                .with("masterauth", "m4ster").with("requirepass", "r3plica"));
        RespClient onGood = client(good.port());
        onGood.call("AUTH", "r3plica");
        awaitValue(onGood, "k", "v");
        onGood.call("CLIENT", "CONSISTENCY", "LINEARIZABLE");
        onMaster.call("SET", "k", "v2");
        assertEquals("v2", onGood.call("GET", "k"), "linearizable reads authenticate to the master too");

        RedisServer bad = start(Config.defaults().withPort(0).withMaster("127.0.0.1", master.port())
                .with("masterauth", "wrong"));
        Thread.sleep(1500);
        List<?> role = (List<?>) client(bad.port()).call("ROLE");
        assertNotEquals("connected", role.get(3), "a replica with the wrong password must not sync");
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

    /** Sends PING from a given local address and returns the first line of whatever comes back. */
    private static String rawFirstReply(InetAddress from, int port) throws IOException {
        try (Socket s = new Socket(from, port)) {
            s.setSoTimeout(5000);
            OutputStream out = s.getOutputStream();
            out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            InputStream in = s.getInputStream();
            StringBuilder line = new StringBuilder();
            int b;
            while ((b = in.read()) != -1 && b != '\n') {
                line.append((char) b);
            }
            return line.toString();
        }
    }

    private static InetAddress nonLoopbackAddress() throws IOException {
        for (NetworkInterface nic : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (!nic.isUp() || nic.isLoopback()) {
                continue;
            }
            for (InetAddress address : Collections.list(nic.getInetAddresses())) {
                if (address instanceof Inet4Address && !address.isLoopbackAddress()) {
                    return address;
                }
            }
        }
        return null;
    }
}
