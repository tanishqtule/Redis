package redis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import redis.protocol.RespClient;
import redis.protocol.RespError;

/**
 * WATCH as Redis 7 does it: any write to a watched key, by anyone, makes
 * the next EXEC return a null reply and run nothing. Every case here was
 * first checked against a real redis-server 7.0.
 */
class WatchTest {

    private RedisServer server;
    private RespClient a;
    private RespClient b;

    @BeforeEach
    void start() throws Exception {
        server = new RedisServer(Config.defaults().withPort(0));
        server.start();
        a = new RespClient(server.port());
        b = new RespClient(server.port());
    }

    @AfterEach
    void stop() throws Exception {
        a.close();
        b.close();
        server.close();
    }

    /** MULTI, GET k, EXEC on connection a; returns EXEC's reply. */
    private Object execGet() throws Exception {
        assertEquals("OK", a.call("MULTI"));
        assertEquals("QUEUED", a.call("GET", "k"));
        return a.call("EXEC");
    }

    @Test
    void writeByAnotherClientAbortsExec() throws Exception {
        a.call("SET", "k", "1");
        assertEquals("OK", a.call("WATCH", "k"));
        b.call("SET", "k", "2");
        assertEquals("OK", a.call("MULTI"));
        a.call("SET", "k", "mine");
        assertNull(a.call("EXEC"));
        assertEquals("2", a.call("GET", "k"), "the aborted transaction wrote nothing");
    }

    @Test
    void untouchedKeyLetsExecRun() throws Exception {
        a.call("SET", "k", "1");
        a.call("WATCH", "k");
        b.call("GET", "k");
        assertEquals(List.of("1"), execGet(), "reads don't count as changes");
    }

    @Test
    void anyWriteCountsEvenIfTheValueStaysTheSame() throws Exception {
        for (String[] write : List.of(
                new String[] {"SET", "k", "1"},
                new String[] {"INCRBY", "k", "0"},
                new String[] {"APPEND", "k", ""},
                new String[] {"EXPIRE", "k", "100"},
                new String[] {"DEL", "k"},
                new String[] {"FLUSHALL"})) {
            a.call("SET", "k", "1");
            a.call("WATCH", "k");
            b.call(write);
            assertNull(execGet(), String.join(" ", write) + " should abort");
        }
    }

    @Test
    void writesThatChangeNothingDontCount() throws Exception {
        for (String[] noop : List.of(
                new String[] {"SETNX", "k", "2"},
                new String[] {"SET", "k", "2", "NX"},
                new String[] {"PERSIST", "k"},
                new String[] {"INCR", "notanumber"})) {
            a.call("SET", "k", "1");
            a.call("SET", "notanumber", "x");
            a.call("WATCH", "k", "notanumber");
            b.call(noop);
            assertEquals(List.of("1"), execGet(), String.join(" ", noop) + " shouldn't abort");
        }
    }

    @Test
    void ownWritesCountToo() throws Exception {
        a.call("SET", "k", "1");
        a.call("WATCH", "k");
        a.call("SET", "k", "2");
        assertNull(execGet());
    }

    @Test
    void creatingAWatchedKeyCountsAndSoDoesCreatingAndDeletingIt() throws Exception {
        a.call("WATCH", "k");
        b.call("SET", "k", "1");
        b.call("DEL", "k");
        assertNull(execGet());

        a.call("WATCH", "k");
        b.call("DEL", "k"); // deleting a key that doesn't exist changes nothing
        b.call("FLUSHALL");
        assertEquals(Collections.singletonList(null), execGet());
    }

    @Test
    void expiringAfterWatchCountsButBeingExpiredAlreadyDoesnt() throws Exception {
        a.call("SET", "k", "1", "PX", "100");
        a.call("WATCH", "k");
        Thread.sleep(250);
        assertNull(execGet(), "expired while watched");

        a.call("SET", "k", "1", "PX", "50");
        Thread.sleep(150);
        a.call("WATCH", "k");
        assertEquals(Collections.singletonList(null), execGet(), "was already gone when watched");
    }

    @Test
    void execAndDiscardAndUnwatchAllEndTheWatch() throws Exception {
        a.call("SET", "k", "1");
        a.call("WATCH", "k");
        assertEquals(List.of("1"), execGet());
        b.call("SET", "k", "2");
        assertEquals(List.of("2"), execGet(), "EXEC unwatched");

        a.call("WATCH", "k");
        b.call("SET", "k", "3");
        a.call("MULTI");
        assertEquals("OK", a.call("DISCARD"));
        assertEquals(List.of("3"), execGet(), "DISCARD unwatched");

        a.call("WATCH", "k");
        b.call("SET", "k", "4");
        assertEquals("OK", a.call("UNWATCH"));
        assertEquals(List.of("4"), execGet(), "UNWATCH unwatched");

        a.call("WATCH", "k");
        b.call("SET", "k", "5");
        assertNull(execGet(), "a failed EXEC unwatches as well...");
        assertEquals(List.of("5"), execGet(), "...so the retry runs");
    }

    @Test
    void watchInsideMultiIsRefusedWithoutDoomingTheTransaction() throws Exception {
        a.call("MULTI");
        RespError err = assertInstanceOf(RespError.class, a.call("WATCH", "k"));
        assertEquals("ERR WATCH inside MULTI is not allowed", err.message());
        a.call("SET", "x", "1");
        assertEquals(List.of("OK"), a.call("EXEC"));
        assertInstanceOf(RespError.class, a.call("WATCH"), "needs at least one key");
    }

    @Test
    void queueErrorsWinOverAChangedKey() throws Exception {
        a.call("SET", "k", "1");
        a.call("WATCH", "k");
        b.call("SET", "k", "2");
        a.call("MULTI");
        a.call("NOSUCHCOMMAND");
        RespError err = assertInstanceOf(RespError.class, a.call("EXEC"));
        assertTrue(err.message().startsWith("EXECABORT"), err.message());
    }

    @Test
    void closingTheConnectionForgetsItsWatches() throws Exception {
        a.call("WATCH", "k1", "k2", "k3");
        String info = (String) b.call("INFO", "clients");
        assertTrue(info.contains("watching_clients:1") && info.contains("total_watched_keys:3"), info);
        a.close();
        for (int i = 0; i < 100 && !((String) b.call("INFO", "clients")).contains("total_watched_keys:0"); i++) {
            Thread.sleep(20);
        }
        info = (String) b.call("INFO", "clients");
        assertTrue(info.contains("watching_clients:0") && info.contains("total_watched_keys:0"), info);
    }

    /**
     * What WATCH is for: concurrent read-modify-write increments, each
     * retried until its EXEC goes through, lose no updates.
     */
    @Test
    void checkAndSetLoopLosesNoUpdates() throws Exception {
        a.call("SET", "counter", "0");
        int clients = 8;
        int increments = 200;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try {
            List<Future<Integer>> retries = new ArrayList<>();
            for (int i = 0; i < clients; i++) {
                retries.add(pool.submit(() -> {
                    int conflicts = 0;
                    try (RespClient c = new RespClient(server.port())) {
                        for (int n = 0; n < increments; n++) {
                            while (true) {
                                c.call("WATCH", "counter");
                                long value = Long.parseLong((String) c.call("GET", "counter"));
                                c.call("MULTI");
                                c.call("SET", "counter", Long.toString(value + 1));
                                if (c.call("EXEC") != null) {
                                    break;
                                }
                                conflicts++;
                            }
                        }
                    }
                    return conflicts;
                }));
            }
            int conflicts = 0;
            for (Future<Integer> f : retries) {
                conflicts += f.get();
            }
            assertEquals(Integer.toString(clients * increments), a.call("GET", "counter"),
                    "lost updates (" + conflicts + " conflicts were retried)");
        } finally {
            pool.shutdownNow();
        }
    }
}
