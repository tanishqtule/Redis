package redis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import redis.persistence.Rdb;
import redis.protocol.RespClient;
import redis.protocol.RespError;

class AutoSaveTest {

    private static Map<String, String> load(Path rdb) throws IOException {
        Map<String, String> keys = new HashMap<>();
        Rdb.read(Files.readAllBytes(rdb), (k, v, e) -> keys.put(k, new String(v)));
        return keys;
    }

    private static Config config(Path dir) {
        return Config.defaults().withPort(0).withDir(dir.toString());
    }

    @Test
    void bgsaveWritesInTheBackgroundAndUpdatesLastsave(@TempDir Path dir) throws Exception {
        try (RedisServer server = new RedisServer(config(dir))) {
            server.start();
            try (RespClient c = new RespClient(server.port())) {
                long before = (Long) c.call("LASTSAVE");
                c.call("SET", "k", "v");
                Thread.sleep(1100);
                assertEquals("Background saving started", c.call("BGSAVE"));
                for (int i = 0; i < 50 && !Files.exists(dir.resolve("dump.rdb")); i++) {
                    Thread.sleep(50);
                }
                assertEquals(Map.of("k", "v"), load(dir.resolve("dump.rdb")));
                for (int i = 0; i < 50 && (Long) c.call("LASTSAVE") == before; i++) {
                    Thread.sleep(50);
                }
                assertTrue((Long) c.call("LASTSAVE") > before);
                assertTrue(((String) c.call("INFO", "persistence")).contains("rdb_changes_since_last_save:0"));
            }
        }
    }

    @Test
    void saveRulesSnapshotByThemselves(@TempDir Path dir) throws Exception {
        try (RedisServer server = new RedisServer(config(dir).with("save", "1", "2"))) {
            server.start();
            try (RespClient c = new RespClient(server.port())) {
                c.call("SET", "a", "1");
                Thread.sleep(2500);
                assertFalse(Files.exists(dir.resolve("dump.rdb")), "one change isn't enough for 'save 1 2'");
                c.call("SET", "b", "2");
                for (int i = 0; i < 60 && !Files.exists(dir.resolve("dump.rdb")); i++) {
                    Thread.sleep(50);
                }
                assertEquals(Map.of("a", "1", "b", "2"), load(dir.resolve("dump.rdb")));
            }
        }
    }

    @Test
    void shutdownSavesUnlessToldNotTo(@TempDir Path dir) throws Exception {
        RedisServer server = new RedisServer(config(dir).with("save", ""));
        server.start();
        try (RespClient c = new RespClient(server.port())) {
            c.call("SET", "k", "v");
            c.send("SHUTDOWN", "NOSAVE");
            assertThrows(IOException.class, c::read, "the server just hangs up");
        }
        assertFalse(Files.exists(dir.resolve("dump.rdb")));
        assertThrows(IOException.class, () -> new RespClient(server.port()).call("PING"));

        RedisServer second = new RedisServer(config(dir).with("save", ""));
        second.start();
        try (RespClient c = new RespClient(second.port())) {
            c.call("SET", "k", "v");
            c.send("SHUTDOWN", "SAVE");
            assertThrows(IOException.class, c::read);
        }
        assertEquals(Map.of("k", "v"), load(dir.resolve("dump.rdb")));
    }

    @Test
    void shutdownRefusesToLoseDataWhenTheSaveFails(@TempDir Path dir) throws Exception {
        Path blocked = dir.resolve("not-a-dir");
        Files.writeString(blocked, "a file where the data directory should be");
        try (RedisServer server = new RedisServer(Config.defaults().withPort(0).withDir(blocked.toString()))) {
            server.start();
            try (RespClient c = new RespClient(server.port())) {
                c.call("SET", "k", "v");
                RespError err = assertInstanceOf(RespError.class, c.call("SHUTDOWN"));
                assertTrue(err.message().contains("Errors trying to SHUTDOWN"), err.message());
                assertEquals("v", c.call("GET", "k"), "still running");
            }
        }
    }

    /** The real thing: a separate JVM, stopped with SIGTERM like docker stop does. */
    @Test
    void sigtermSavesBeforeExiting(@TempDir Path dir) throws Exception {
        String java = ProcessHandle.current().info().command().orElse("java");
        int port;
        try (java.net.ServerSocket probe = new java.net.ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        Process server = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), "redis.Main",
                "--port", Integer.toString(port), "--dir", dir.toString())
                .redirectErrorStream(true).redirectOutput(dir.resolve("server.log").toFile()).start();
        try {
            RespClient c = null;
            for (int i = 0; i < 100 && c == null; i++) {
                try {
                    c = new RespClient(port);
                } catch (IOException notYet) {
                    Thread.sleep(100);
                }
            }
            assertTrue(c != null, "server didn't start: " + Files.readString(dir.resolve("server.log")));
            c.call("SET", "survives", "sigterm");
            c.close();
            server.destroy(); // SIGTERM on Unix
            assertTrue(server.waitFor(20, TimeUnit.SECONDS), "server didn't exit");
        } finally {
            server.destroyForcibly();
        }
        assertEquals(Map.of("survives", "sigterm"), load(dir.resolve("dump.rdb")),
                Files.readString(dir.resolve("server.log")));
    }
}
