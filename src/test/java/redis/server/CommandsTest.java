package redis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import redis.protocol.RespClient;
import redis.protocol.RespError;

/** Drives a real server over a socket, the way a client would. */
class CommandsTest {

    private RedisServer server;
    private RespClient client;

    @BeforeEach
    void start() throws Exception {
        server = new RedisServer(Config.defaults().withPort(0));
        server.start();
        client = new RespClient(server.port());
    }

    @AfterEach
    void stop() throws Exception {
        client.close();
        server.close();
    }

    @Test
    void basics() throws Exception {
        assertEquals("PONG", client.call("PING"));
        assertEquals("hey", client.call("echo", "hey"));
        assertEquals("OK", client.call("set", "k", "v"));
        assertEquals("v", client.call("GET", "k"));
        assertNull(client.call("GET", "nope"));
    }

    @Test
    void unknownCommandsAndBadArityGetErrorsNotSilence() throws Exception {
        RespError unknown = assertInstanceOf(RespError.class, client.call("NOPE", "x"));
        assertTrue(unknown.message().startsWith("ERR unknown command 'NOPE'"));
        RespError arity = assertInstanceOf(RespError.class, client.call("GET"));
        assertEquals("ERR wrong number of arguments for 'get' command", arity.message());
        assertEquals("PONG", client.call("PING"));
    }

    @Test
    void setOptions() throws Exception {
        assertEquals("OK", client.call("SET", "k", "1", "NX"));
        assertNull(client.call("SET", "k", "2", "NX"));
        assertNull(client.call("SET", "other", "2", "XX"));
        assertEquals("1", client.call("SET", "k", "3", "GET"));
        assertEquals("3", client.call("GET", "k"));
        assertInstanceOf(RespError.class, client.call("SET", "k", "v", "NX", "XX"));
        assertInstanceOf(RespError.class, client.call("SET", "k", "v", "EX", "0"));
        assertInstanceOf(RespError.class, client.call("SET", "k", "v", "BOGUS"));
    }

    @Test
    void expiry() throws Exception {
        client.call("SET", "k", "v", "PX", "100");
        long pttl = (Long) client.call("PTTL", "k");
        assertTrue(pttl > 0 && pttl <= 100, "pttl was " + pttl);
        Thread.sleep(150);
        assertNull(client.call("GET", "k"));
        assertEquals(-2L, client.call("TTL", "k"));

        client.call("SET", "k", "v", "EX", "100");
        assertEquals(100L, client.call("TTL", "k"));
        assertEquals(1L, client.call("PERSIST", "k"));
        assertEquals(-1L, client.call("TTL", "k"));
        assertEquals(1L, client.call("EXPIRE", "k", "-1"));
        assertEquals(0L, client.call("EXISTS", "k"));
    }

    @Test
    void activeExpiryRemovesUntouchedKeys() throws Exception {
        for (int i = 0; i < 500; i++) {
            client.call("SET", "temp:" + i, "x", "PX", "50");
        }
        client.call("SET", "keep", "x");
        Thread.sleep(400);
        // DBSIZE counts keys that are expired but not yet collected, so this
        // only passes if the background cycle did its job.
        assertEquals(1L, client.call("DBSIZE"));
    }

    @Test
    void counters() throws Exception {
        assertEquals(1L, client.call("INCR", "n"));
        assertEquals(11L, client.call("INCRBY", "n", "10"));
        assertEquals(9L, client.call("DECRBY", "n", "2"));
        client.call("SET", "s", "abc");
        assertEquals(new RespError("ERR value is not an integer or out of range"), client.call("INCR", "s"));
        client.call("SET", "big", String.valueOf(Long.MAX_VALUE));
        assertInstanceOf(RespError.class, client.call("INCR", "big"));
    }

    @Test
    void incrIsAtomicUnderConcurrency() throws Exception {
        int clients = 8;
        int perClient = 500;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        List<Future<?>> done = new ArrayList<>();
        for (int c = 0; c < clients; c++) {
            done.add(pool.submit((Callable<Void>) () -> {
                try (RespClient tc = new RespClient(server.port())) {
                    for (int i = 0; i < perClient; i++) {
                        tc.call("INCR", "hits");
                    }
                }
                return null;
            }));
        }
        for (Future<?> f : done) {
            f.get();
        }
        pool.shutdown();
        assertEquals(String.valueOf(clients * perClient), client.call("GET", "hits"));
    }

    @Test
    void manyIdleClientsDoNotStarveNewOnes() throws Exception {
        List<RespClient> idle = new ArrayList<>();
        try {
            for (int i = 0; i < 200; i++) {
                idle.add(new RespClient(server.port()));
            }
            try (RespClient late = new RespClient(server.port())) {
                assertEquals("PONG", late.call("PING"));
            }
        } finally {
            for (RespClient c : idle) {
                c.close();
            }
        }
    }

    @Test
    void keysAndDel() throws Exception {
        client.call("SET", "user:1", "a");
        client.call("SET", "user:2", "b");
        client.call("SET", "order:1", "c");
        List<?> users = (List<?>) client.call("KEYS", "user:*");
        assertEquals(2, users.size());
        assertEquals(2L, client.call("DEL", "user:1", "user:2", "user:3"));
        assertEquals(List.of("order:1"), client.call("KEYS", "*"));
        assertEquals("string", client.call("TYPE", "order:1"));
        assertEquals("none", client.call("TYPE", "user:1"));
    }

    @Test
    void pipelinedCommandsAllGetAnswered() throws Exception {
        StringBuilder batch = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            batch.append("*2\r\n$4\r\nINCR\r\n$1\r\np\r\n");
        }
        client.sendRaw(batch.toString().getBytes());
        Object last = null;
        for (int i = 0; i < 1000; i++) {
            last = client.read();
        }
        assertEquals(1000L, last);
    }

    @Test
    void transactions() throws Exception {
        assertEquals("OK", client.call("MULTI"));
        assertEquals("QUEUED", client.call("SET", "a", "1"));
        assertEquals("QUEUED", client.call("INCR", "a"));
        assertEquals("QUEUED", client.call("GET", "a"));
        assertEquals(List.of("OK", 2L, "2"), client.call("EXEC"));
        assertEquals("2", client.call("GET", "a"));
    }

    @Test
    void transactionKeepsGoingAfterRuntimeError() throws Exception {
        client.call("SET", "s", "text");
        client.call("MULTI");
        client.call("INCR", "s");
        client.call("SET", "t", "ok");
        List<?> replies = (List<?>) client.call("EXEC");
        assertInstanceOf(RespError.class, replies.get(0));
        assertEquals("OK", replies.get(1));
    }

    @Test
    void transactionAbortsOnQueueTimeError() throws Exception {
        client.call("MULTI");
        assertInstanceOf(RespError.class, client.call("NOTACOMMAND"));
        client.call("SET", "x", "1");
        RespError err = assertInstanceOf(RespError.class, client.call("EXEC"));
        assertTrue(err.message().startsWith("EXECABORT"));
        assertNull(client.call("GET", "x"));
    }

    @Test
    void transactionControlErrors() throws Exception {
        assertEquals(new RespError("ERR EXEC without MULTI"), client.call("EXEC"));
        assertEquals(new RespError("ERR DISCARD without MULTI"), client.call("DISCARD"));
        client.call("MULTI");
        assertEquals(new RespError("ERR MULTI calls can not be nested"), client.call("MULTI"));
        client.call("SET", "x", "1");
        assertEquals("OK", client.call("DISCARD"));
        assertNull(client.call("GET", "x"));
        assertEquals(List.of(), sendMultiExec());
    }

    private Object sendMultiExec() throws Exception {
        client.call("MULTI");
        return client.call("EXEC");
    }

    @Test
    void transactionIsIsolatedFromOtherClients() throws Exception {
        client.call("SET", "n", "0");
        try (RespClient other = new RespClient(server.port())) {
            client.call("MULTI");
            client.call("INCR", "n");
            other.call("INCR", "n");
            client.call("INCR", "n");
            assertEquals(List.of(2L, 3L), client.call("EXEC"));
        }
    }
}
