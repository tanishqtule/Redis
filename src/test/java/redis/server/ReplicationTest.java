package redis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import redis.TestClient;
import redis.TestClient.Err;

class ReplicationTest {

    private final List<AutoCloseable> cleanup = new ArrayList<>();
    private RedisServer master;
    private TestClient onMaster;

    @BeforeEach
    void startMaster() throws Exception {
        master = start(Config.defaults().withPort(0));
        onMaster = client(master);
    }

    @AfterEach
    void stopAll() throws Exception {
        for (int i = cleanup.size() - 1; i >= 0; i--) {
            cleanup.get(i).close();
        }
    }

    @Test
    void replicaGetsExistingDataAndLaterWrites() throws Exception {
        onMaster.call("SET", "before", "1");
        TestClient onReplica = client(replicaOf(master));
        awaitValue(onReplica, "before", "1");

        onMaster.call("SET", "after", "2");
        onMaster.call("INCR", "counter");
        onMaster.call("INCR", "counter");
        onMaster.call("DEL", "before");
        awaitValue(onReplica, "counter", "2");
        assertEquals("2", onReplica.call("GET", "after"));
        assertNull(onReplica.call("GET", "before"));
    }

    @Test
    void transactionsReachReplicasWhole() throws Exception {
        TestClient onReplica = client(replicaOf(master));
        onMaster.call("MULTI");
        onMaster.call("SET", "a", "1");
        onMaster.call("SET", "b", "2");
        onMaster.call("EXEC");
        awaitValue(onReplica, "b", "2");
        assertEquals("1", onReplica.call("GET", "a"));
    }

    @Test
    void replicaRefusesClientWrites() throws Exception {
        TestClient onReplica = client(replicaOf(master));
        Err err = assertInstanceOf(Err.class, onReplica.call("SET", "x", "1"));
        assertTrue(err.message().startsWith("READONLY"));
        assertEquals("PONG", onReplica.call("PING"));
    }

    @Test
    void waitCountsReplicasThatCaughtUp() throws Exception {
        replicaOf(master);
        replicaOf(master);
        awaitInfo(onMaster, "connected_slaves:2");

        assertEquals(2L, onMaster.call("WAIT", "1", "500"), "no writes yet, so every replica is in sync");
        onMaster.call("SET", "k", "v");
        assertEquals(2L, onMaster.call("WAIT", "2", "2000"));

        long started = System.nanoTime();
        onMaster.call("SET", "k", "v2");
        assertEquals(2L, onMaster.call("WAIT", "3", "300"), "only two replicas exist");
        long waitedMs = (System.nanoTime() - started) / 1_000_000;
        assertTrue(waitedMs >= 250, "should have waited for the timeout, waited " + waitedMs + " ms");
    }

    @Test
    void waitWithNoReplicasReturnsZero() throws Exception {
        onMaster.call("SET", "k", "v");
        assertEquals(0L, onMaster.call("WAIT", "0", "100"));
    }

    @Test
    void expiryOnMasterIsPropagated() throws Exception {
        TestClient onReplica = client(replicaOf(master));
        onMaster.call("SET", "short", "v", "PX", "100");
        awaitValue(onReplica, "short", "v");
        Thread.sleep(400);
        // DBSIZE counts uncollected keys too, so 0 means the replica really deleted it.
        assertEquals(0L, onReplica.call("DBSIZE"));
    }

    @Test
    void infoReportsRoles() throws Exception {
        TestClient onReplica = client(replicaOf(master));
        String masterInfo = (String) onMaster.call("INFO", "replication");
        assertTrue(masterInfo.contains("role:master"), masterInfo);
        assertTrue(masterInfo.contains("master_replid:"), masterInfo);
        awaitInfo(onReplica, "master_link_status:up");
        assertTrue(((String) onReplica.call("INFO", "replication")).contains("role:slave"));
    }

    @Test
    void replicaResyncsAfterMasterRestart() throws Exception {
        int port = master.port();
        TestClient onReplica = client(replicaOf(master));
        onMaster.call("SET", "gen", "1");
        awaitValue(onReplica, "gen", "1");

        onMaster.close();
        master.close();
        RedisServer second = startOnSamePort(port);
        TestClient onSecond = client(second);
        onSecond.call("SET", "gen", "2");
        awaitValue(onReplica, "gen", "2");
    }

    private RedisServer start(Config config) throws Exception {
        RedisServer server = new RedisServer(config);
        server.start();
        cleanup.add(server);
        return server;
    }

    // The old master's port came from the ephemeral range, so some outgoing
    // socket can grab it for a moment after it's released. Retry briefly.
    private RedisServer startOnSamePort(int port) throws Exception {
        for (int attempt = 0; ; attempt++) {
            try {
                return start(Config.defaults().withPort(port));
            } catch (java.net.BindException e) {
                if (attempt == 40) {
                    throw e;
                }
                Thread.sleep(50);
            }
        }
    }

    private RedisServer replicaOf(RedisServer of) throws Exception {
        return start(Config.defaults().withPort(0).withMaster("127.0.0.1", of.port()));
    }

    private TestClient client(RedisServer server) throws Exception {
        TestClient c = new TestClient(server.port());
        cleanup.add(c);
        return c;
    }

    private static void awaitValue(TestClient client, String key, String expected) throws Exception {
        Object last = null;
        for (int i = 0; i < 100; i++) {
            last = client.call("GET", key);
            if (Objects.equals(expected, last)) {
                return;
            }
            Thread.sleep(50);
        }
        fail("expected " + key + "=" + expected + " but it stayed " + last);
    }

    private static void awaitInfo(TestClient client, String line) throws Exception {
        for (int i = 0; i < 100; i++) {
            if (((String) client.call("INFO", "replication")).contains(line)) {
                return;
            }
            Thread.sleep(50);
        }
        fail("INFO never showed " + line);
    }
}
