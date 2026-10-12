package redis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import redis.protocol.RespClient;
import redis.protocol.RespError;

class PracticalFeaturesTest {

    private final List<AutoCloseable> cleanup = new ArrayList<>();

    @AfterEach
    void stopAll() throws Exception {
        for (int i = cleanup.size() - 1; i >= 0; i--) {
            cleanup.get(i).close();
        }
    }

    private RespClient start(Config config) throws Exception {
        RedisServer server = new RedisServer(config);
        server.start();
        cleanup.add(server);
        RespClient c = new RespClient(server.port());
        cleanup.add(c);
        return c;
    }

    /**
     * SCAN's promise: a key present for the whole scan comes back exactly
     * once, however much the keyspace changes between calls.
     */
    @Test
    void scanReturnsEveryStableKeyExactlyOnceWhileOthersComeAndGo() throws Exception {
        RespClient c = start(Config.defaults().withPort(0));
        Set<String> stable = new HashSet<>();
        for (int i = 0; i < 2000; i++) {
            c.call("SET", "stable:" + i, "v");
            stable.add("stable:" + i);
        }
        Map<String, Integer> seen = new HashMap<>();
        String cursor = "0";
        int churn = 0;
        do {
            List<?> reply = (List<?>) c.call("SCAN", cursor, "COUNT", "37");
            cursor = (String) reply.get(0);
            for (Object key : (List<?>) reply.get(1)) {
                seen.merge((String) key, 1, Integer::sum);
            }
            // Keys arriving and leaving mid-scan must not disturb the stable ones.
            c.call("SET", "churn:" + churn, "x");
            c.call("DEL", "churn:" + (churn - 5));
            churn++;
        } while (!cursor.equals("0"));

        for (String key : stable) {
            assertEquals(1, seen.getOrDefault(key, 0), key + " seen " + seen.get(key) + " times");
        }
    }

    @Test
    void scanFiltersByPatternAndType() throws Exception {
        RespClient c = start(Config.defaults().withPort(0));
        for (int i = 0; i < 50; i++) {
            c.call("SET", (i % 2 == 0 ? "user:" : "order:") + i, "v");
        }
        Set<String> users = new HashSet<>();
        String cursor = "0";
        do {
            List<?> reply = (List<?>) c.call("SCAN", cursor, "MATCH", "user:*", "COUNT", "10", "TYPE", "string");
            cursor = (String) reply.get(0);
            for (Object key : (List<?>) reply.get(1)) {
                users.add((String) key);
            }
        } while (!cursor.equals("0"));
        assertEquals(25, users.size());
        assertTrue(users.stream().allMatch(k -> k.startsWith("user:")));

        List<?> none = (List<?>) c.call("SCAN", "0", "TYPE", "hash", "COUNT", "1000");
        assertEquals(List.of(), none.get(1));
        assertEquals(new RespError("ERR invalid cursor"), c.call("SCAN", "abc"));
        assertTrue(c.call("RANDOMKEY") instanceof String);
    }

    @Test
    void noevictionRefusesWritesButAllowsDeletes() throws Exception {
        RespClient c = start(Config.defaults().withPort(0).with("maxmemory", "20kb"));
        String value = "x".repeat(1000);
        RespError oom = null;
        for (int i = 0; i < 100 && oom == null; i++) {
            Object reply = c.call("SET", "k" + i, value);
            if (reply instanceof RespError e) {
                oom = e;
            }
        }
        assertTrue(oom != null && oom.message().startsWith("OOM command not allowed"), String.valueOf(oom));
        assertEquals(1L, c.call("DEL", "k0"), "deletes still work when full");
        assertEquals("OK", c.call("SET", "small", "v"), "and free room for writes again");
    }

    @Test
    void allkeysLruEvictsColdKeysAndKeepsHotOnes() throws Exception {
        RespClient c = start(Config.defaults().withPort(0).with("maxmemory", "60kb")
                .with("maxmemory-policy", "allkeys-lru").with("maxmemory-samples", "64"));
        String value = "x".repeat(1000);
        for (int i = 0; i < 50; i++) {
            c.call("SET", "key:" + i, value);
        }
        Thread.sleep(5);
        for (int i = 0; i < 25; i++) {
            c.call("GET", "key:" + i); // keys 0-24 are now the recently used ones
        }
        for (int i = 50; i < 60; i++) {
            assertEquals("OK", c.call("SET", "key:" + i, value));
        }
        for (int i = 0; i < 25; i++) {
            assertEquals(value, c.call("GET", "key:" + i), "hot key:" + i + " was evicted");
        }
        String info = (String) c.call("INFO", "stats");
        assertTrue(!info.contains("evicted_keys:0\r\n"), info);
    }

    @Test
    void volatileTtlOnlyEvictsKeysWithATtl() throws Exception {
        RespClient c = start(Config.defaults().withPort(0).with("maxmemory", "30kb")
                .with("maxmemory-policy", "volatile-ttl"));
        String value = "x".repeat(1000);
        for (int i = 0; i < 10; i++) {
            c.call("SET", "keep:" + i, value);
        }
        for (int i = 0; i < 40; i++) {
            Object reply = c.call("SET", "cache:" + i, value, "EX", Integer.toString(1000 + i));
            if (reply instanceof RespError) {
                break;
            }
        }
        for (int i = 0; i < 10; i++) {
            assertEquals(value, c.call("GET", "keep:" + i), "a key without a TTL was evicted");
        }
        assertNull(c.call("GET", "cache:0"), "the key closest to expiring should have gone first");
    }

    @Test
    void evictionsReachReplicas() throws Exception {
        RedisServer master = new RedisServer(Config.defaults().withPort(0).with("maxmemory", "30kb")
                .with("maxmemory-policy", "allkeys-random"));
        master.start();
        cleanup.add(master);
        RedisServer replica = new RedisServer(Config.defaults().withPort(0).withMaster("127.0.0.1", master.port()));
        replica.start();
        cleanup.add(replica);
        RespClient onMaster = new RespClient(master.port());
        RespClient onReplica = new RespClient(replica.port());
        cleanup.add(onMaster);
        cleanup.add(onReplica);
        for (int i = 0; i < 200 && !"connected".equals(((List<?>) onReplica.call("ROLE")).get(3)); i++) {
            Thread.sleep(20);
        }
        for (int i = 0; i < 100; i++) {
            onMaster.call("SET", "k" + i, "x".repeat(1000));
        }
        long onMasterKeys = (Long) onMaster.call("DBSIZE");
        assertTrue(onMasterKeys < 100);
        for (int i = 0; i < 100; i++) {
            if (onMasterKeys == (Long) onReplica.call("DBSIZE")) {
                return;
            }
            Thread.sleep(50);
        }
        fail("replica never caught up with the master's evictions");
    }

    @Test
    void stringCommandsClientLibrariesUse() throws Exception {
        RespClient c = start(Config.defaults().withPort(0));
        assertEquals("OK", c.call("MSET", "a", "1", "b", "2"));
        assertEquals(List.of("1", "2"), c.call("MGET", "a", "b"));
        assertEquals(0L, c.call("SETNX", "a", "x"));
        assertEquals(1L, c.call("SETNX", "c", "3"));
        assertEquals("OK", c.call("SETEX", "t", "100", "v"));
        assertEquals(100L, c.call("TTL", "t"));
        assertEquals("OK", c.call("PSETEX", "p", "100000", "v"));
        assertInstanceOf(RespError.class, c.call("SETEX", "t", "0", "v"));
        assertEquals("1", c.call("GETSET", "a", "one"));
        assertEquals("one", c.call("GETDEL", "a"));
        assertNull(c.call("GET", "a"));
        assertInstanceOf(RespError.class, c.call("MSET", "a", "1", "b"));
        List<?> time = (List<?>) c.call("TIME");
        assertTrue(Long.parseLong((String) time.get(0)) > 1_700_000_000L);
    }

    @Test
    void infoAndClientCommands() throws Exception {
        RespClient c = start(Config.defaults().withPort(0));
        c.call("CLIENT", "SETNAME", "worker-1");
        c.call("SET", "k", "v");
        c.call("GET", "k");
        c.call("GET", "missing");
        String info = (String) c.call("INFO");
        for (String field : new String[] {"redis_version:", "connected_clients:1", "used_memory:", "rdb_last_save_time:",
                "keyspace_hits:1", "keyspace_misses:1", "role:master", "db0:keys=1"}) {
            assertTrue(info.contains(field), field + " missing from INFO");
        }
        String list = (String) c.call("CLIENT", "LIST");
        assertTrue(list.contains("name=worker-1") && list.contains("cmd=client|list"), list);

        RespClient other = new RespClient(portOf(c));
        cleanup.add(other);
        long otherId = (Long) other.call("CLIENT", "ID");
        assertEquals(1L, c.call("CLIENT", "KILL", "ID", Long.toString(otherId)));
        Thread.sleep(100);
        assertTrue(!((String) c.call("CLIENT", "LIST")).contains("id=" + otherId + " "));
    }

    /** Also checks that INFO reports the port actually bound, not the configured 0. */
    private int portOf(RespClient c) throws Exception {
        for (Object line : ((String) c.call("INFO", "server")).split("\r\n")) {
            String s = (String) line;
            if (s.startsWith("tcp_port:")) {
                return Integer.parseInt(s.substring(9));
            }
        }
        throw new AssertionError("no tcp_port in INFO");
    }
}
