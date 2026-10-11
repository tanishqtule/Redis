package redis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import redis.protocol.RespClient;
import redis.protocol.RespError;

/**
 * Reads from replicas: stale by default, read-your-writes with WAITOFFSET,
 * linearizable with CLIENT CONSISTENCY LINEARIZABLE.
 *
 * Most tests give the master 300 ms of simulated replication lag, which makes
 * the stale case happen every time instead of only under load.
 */
class ReplicaConsistencyTest {

    private static final int LAG_MS = 300;

    private final List<AutoCloseable> cleanup = new ArrayList<>();

    @AfterEach
    void stopAll() throws Exception {
        for (int i = cleanup.size() - 1; i >= 0; i--) {
            cleanup.get(i).close();
        }
    }

    @Test
    void plainReplicaReadsCanBeStale() throws Exception {
        RedisServer master = start(Config.defaults().withPort(0).withSimulatedReplicationLag(LAG_MS));
        RespClient onMaster = client(master);
        RespClient onReplica = client(replicaOf(master));

        onMaster.call("SET", "k", "v1");
        awaitValue(onReplica, "k", "v1");
        onMaster.call("SET", "k", "v2");
        assertEquals("v1", onReplica.call("GET", "k"), "the write is still in flight, so the replica can't have it");
    }

    @Test
    void waitOffsetGivesReadYourWrites() throws Exception {
        RedisServer master = start(Config.defaults().withPort(0).withSimulatedReplicationLag(LAG_MS));
        RespClient onMaster = client(master);
        RespClient onReplica = client(replicaOf(master));
        awaitLinkUp(onReplica);

        onMaster.call("SET", "k", "mine");
        long token = (Long) ((List<?>) onMaster.call("ROLE")).get(1);

        long started = System.nanoTime();
        long reached = (Long) onReplica.call("WAITOFFSET", Long.toString(token), "5000");
        long waitedMs = (System.nanoTime() - started) / 1_000_000;

        assertTrue(reached >= token);
        assertEquals("mine", onReplica.call("GET", "k"));
        assertTrue(waitedMs >= LAG_MS / 2, "should have had to wait for the lagging stream, waited " + waitedMs + " ms");
    }

    @Test
    void waitOffsetReportsATimeoutByReturningALowerOffset() throws Exception {
        RedisServer master = start(Config.defaults().withPort(0).withSimulatedReplicationLag(LAG_MS));
        RespClient onMaster = client(master);
        RespClient onReplica = client(replicaOf(master));
        awaitLinkUp(onReplica);

        onMaster.call("SET", "k", "v");
        long token = (Long) ((List<?>) onMaster.call("ROLE")).get(1);
        long reached = (Long) onReplica.call("WAITOFFSET", Long.toString(token), "20");
        assertTrue(reached < token, "timed out, so the replica can't be at the token yet");
    }

    @Test
    void linearizableReadsAlwaysSeeCompletedWrites() throws Exception {
        RedisServer master = start(Config.defaults().withPort(0).withSimulatedReplicationLag(LAG_MS));
        RespClient onMaster = client(master);
        RespClient onReplica = client(replicaOf(master));
        awaitLinkUp(onReplica);
        assertEquals("OK", onReplica.call("CLIENT", "CONSISTENCY", "LINEARIZABLE"));

        for (int i = 1; i <= 5; i++) {
            onMaster.call("SET", "k", "v" + i);
            assertEquals("v" + i, onReplica.call("GET", "k"));
        }
    }

    @Test
    void linearizableTransactionsWaitAtExec() throws Exception {
        RedisServer master = start(Config.defaults().withPort(0).withSimulatedReplicationLag(LAG_MS));
        RespClient onMaster = client(master);
        RespClient onReplica = client(replicaOf(master));
        awaitLinkUp(onReplica);
        onReplica.call("CLIENT", "CONSISTENCY", "LINEARIZABLE");

        onMaster.call("SET", "a", "1");
        onMaster.call("SET", "b", "2");
        onReplica.call("MULTI");
        onReplica.call("GET", "a");
        onReplica.call("GET", "b");
        assertEquals(List.of("1", "2"), onReplica.call("EXEC"));
    }

    @Test
    void linearizableReadsFailInsteadOfServingStaleData() throws Exception {
        RedisServer master = start(Config.defaults().withPort(0));
        RespClient onMaster = client(master);
        RespClient onReplica = client(replicaOf(master));
        onMaster.call("SET", "k", "v");
        awaitValue(onReplica, "k", "v");

        master.close();
        for (int i = 0; i < 100 && !"connect".equals(((List<?>) onReplica.call("ROLE")).get(3)); i++) {
            Thread.sleep(20);
        }

        assertEquals("v", onReplica.call("GET", "k"), "eventual reads keep working from local data");
        onReplica.call("CLIENT", "CONSISTENCY", "LINEARIZABLE");
        RespError err = assertInstanceOf(RespError.class, onReplica.call("GET", "k"));
        assertEquals("MASTERDOWN", err.code());
    }

    @Test
    void clientConsistencyCommand() throws Exception {
        RedisServer master = start(Config.defaults().withPort(0));
        RespClient onMaster = client(master);
        assertEquals("eventual", onMaster.call("CLIENT", "CONSISTENCY"));
        assertEquals("OK", onMaster.call("CLIENT", "CONSISTENCY", "linearizable"));
        assertEquals("linearizable", onMaster.call("CLIENT", "CONSISTENCY"));
        onMaster.call("SET", "k", "v");
        assertEquals("v", onMaster.call("GET", "k"), "a master's reads are already linearizable");
        assertInstanceOf(RespError.class, onMaster.call("CLIENT", "CONSISTENCY", "sometimes"));
        assertEquals("OK", onMaster.call("CLIENT", "CONSISTENCY", "EVENTUAL"));
    }

    @Test
    void concurrentLinearizableReadsShareRoundTripsToTheMaster() throws Exception {
        RedisServer master = start(Config.defaults().withPort(0));
        RedisServer replica = replicaOf(master);
        RespClient onMaster = client(master);
        onMaster.call("SET", "k", "v");
        awaitValue(client(replica), "k", "v");

        int readers = 16;
        int readsEach = 50;
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        List<Future<?>> done = new ArrayList<>();
        for (int r = 0; r < readers; r++) {
            done.add(pool.submit(() -> {
                try (RespClient c = new RespClient(replica.port())) {
                    c.call("CLIENT", "CONSISTENCY", "LINEARIZABLE");
                    for (int i = 0; i < readsEach; i++) {
                        assertEquals("v", c.call("GET", "k"));
                    }
                }
                return null;
            }));
        }
        for (Future<?> f : done) {
            f.get();
        }
        pool.shutdown();

        String info = (String) client(replica).call("INFO", "replication");
        long reads = infoValue(info, "linearizable_reads");
        long roundTrips = infoValue(info, "read_index_round_trips");
        assertEquals(readers * readsEach, reads);
        assertTrue(roundTrips < reads, roundTrips + " round trips for " + reads + " reads: nothing was shared");
    }

    private RedisServer start(Config config) throws Exception {
        RedisServer server = new RedisServer(config);
        server.start();
        cleanup.add(server);
        return server;
    }

    private RedisServer replicaOf(RedisServer of) throws Exception {
        return start(Config.defaults().withPort(0).withMaster("127.0.0.1", of.port()));
    }

    private RespClient client(RedisServer server) throws Exception {
        RespClient c = new RespClient(server.port());
        cleanup.add(c);
        return c;
    }

    private static void awaitLinkUp(RespClient onReplica) throws Exception {
        for (int i = 0; i < 200; i++) {
            if ("connected".equals(((List<?>) onReplica.call("ROLE")).get(3))) {
                return;
            }
            Thread.sleep(20);
        }
        fail("replica never connected");
    }

    private static void awaitValue(RespClient client, String key, String expected) throws Exception {
        Object last = null;
        for (int i = 0; i < 200; i++) {
            last = client.call("GET", key);
            if (Objects.equals(expected, last)) {
                return;
            }
            Thread.sleep(20);
        }
        fail("expected " + key + "=" + expected + " but it stayed " + last);
    }

    private static long infoValue(String info, String field) {
        for (String line : info.split("\r\n")) {
            if (line.startsWith(field + ":")) {
                return Long.parseLong(line.substring(field.length() + 1));
            }
        }
        throw new AssertionError(field + " missing from INFO:\n" + info);
    }
}
