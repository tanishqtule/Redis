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
import java.util.concurrent.atomic.AtomicLong;

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

    /**
     * No write may be acknowledged after the final snapshot is taken: one
     * that is would be confirmed to its client and then lost.
     */
    @Test
    void shutdownKeepsEveryAcknowledgedWrite(@TempDir Path dir) throws Exception {
        RedisServer server = new RedisServer(config(dir));
        server.start();
        AtomicLong lastAcknowledged = new AtomicLong();
        Thread writer = new Thread(() -> {
            try (RespClient c = new RespClient(server.port())) {
                for (long i = 1; ; i++) {
                    if (!"OK".equals(c.call("SET", "counter", Long.toString(i)))) {
                        return;
                    }
                    lastAcknowledged.set(i);
                }
            } catch (IOException connectionClosed) {
                // the server went away, as expected
            }
        });
        writer.start();
        while (lastAcknowledged.get() < 1000) {
            Thread.sleep(1);
        }
        try (RespClient c = new RespClient(server.port())) {
            c.send("SHUTDOWN", "SAVE");
            assertThrows(IOException.class, c::read);
        }
        writer.join(10_000);
        long saved = Long.parseLong(load(dir.resolve("dump.rdb")).get("counter"));
        assertTrue(saved >= lastAcknowledged.get(),
                "acknowledged up to " + lastAcknowledged.get() + " but only " + saved + " was saved");
    }

    /** A server in its own JVM, so it can be sent real signals. */
    private record ServerProcess(Process process, int port, Path log) {

        static ServerProcess start(Path dataDir, Path log) throws Exception {
            String java = ProcessHandle.current().info().command().orElse("java");
            int port;
            try (java.net.ServerSocket probe = new java.net.ServerSocket(0)) {
                port = probe.getLocalPort();
            }
            Process process = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), "redis.Main",
                    "--port", Integer.toString(port), "--dir", dataDir.toString())
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start();
            ServerProcess server = new ServerProcess(process, port, log);
            for (int i = 0; i < 100; i++) {
                try (RespClient c = new RespClient(port)) {
                    return server;
                } catch (IOException notYet) {
                    Thread.sleep(100);
                }
            }
            process.destroyForcibly();
            throw new AssertionError("server didn't start: " + Files.readString(log));
        }

        Object call(String... args) throws IOException {
            try (RespClient c = new RespClient(port)) {
                return c.call(args);
            }
        }

        String output() throws IOException {
            return Files.readString(log);
        }
    }

    /** The real thing: a separate JVM, stopped with SIGTERM like docker stop does. */
    @Test
    void sigtermSavesBeforeExiting(@TempDir Path dir) throws Exception {
        ServerProcess server = ServerProcess.start(dir, dir.resolve("server.log"));
        try {
            server.call("SET", "survives", "sigterm");
            server.process().destroy(); // SIGTERM on Unix
            assertTrue(server.process().waitFor(20, TimeUnit.SECONDS), "server didn't exit");
        } finally {
            server.process().destroyForcibly();
        }
        String log = server.output();
        assertEquals(Map.of("survives", "sigterm"), load(dir.resolve("dump.rdb")), log);
        // A clean stop exits 0, as Redis does, and what it logged on the way out isn't lost.
        assertEquals(0, server.process().exitValue(), log);
        assertTrue(log.contains("saved 1 keys") && log.contains("ready to exit"), log);
    }

    /**
     * If the final save fails, SIGTERM leaves the server running rather than
     * lose the data, and the next SIGTERM tries again.
     */
    @Test
    void sigtermKeepsRunningWhenTheSaveFailsAndTriesAgainNextTime(@TempDir Path dir) throws Exception {
        Path dataDir = dir.resolve("data");
        Files.writeString(dataDir, "a file where the data directory should be");
        ServerProcess server = ServerProcess.start(dataDir, dir.resolve("server.log"));
        try {
            server.call("SET", "k", "v");
            server.process().destroy();
            assertFalse(server.process().waitFor(2, TimeUnit.SECONDS), "exited without saving: " + server.output());
            assertEquals("v", server.call("GET", "k"));
            assertTrue(server.output().contains("errors trying to shut down"), server.output());

            Files.delete(dataDir); // the operator fixes the problem
            server.process().destroy();
            assertTrue(server.process().waitFor(20, TimeUnit.SECONDS), "didn't exit: " + server.output());
            assertEquals(0, server.process().exitValue(), server.output());
        } finally {
            server.process().destroyForcibly();
        }
        assertEquals(Map.of("k", "v"), load(dataDir.resolve("dump.rdb")), server.output());
    }
}
