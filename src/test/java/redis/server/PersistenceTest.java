package redis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import redis.TestClient;

class PersistenceTest {

    @Test
    void dataSurvivesSaveAndRestart(@TempDir Path dir) throws Exception {
        Config config = Config.defaults().withPort(0).withDir(dir.toString()).withDbFilename("snap.rdb");

        try (RedisServer first = new RedisServer(config)) {
            first.start();
            try (TestClient c = new TestClient(first.port())) {
                c.call("SET", "kept", "yes");
                c.call("SET", "ttl", "v", "EX", "100");
                c.call("SET", "gone", "v", "PX", "50");
                assertEquals("OK", c.call("SAVE"));
            }
        }
        assertTrue(Files.exists(dir.resolve("snap.rdb")));
        Thread.sleep(100);

        try (RedisServer second = new RedisServer(config)) {
            second.start();
            try (TestClient c = new TestClient(second.port())) {
                assertEquals("yes", c.call("GET", "kept"));
                long ttl = (Long) c.call("TTL", "ttl");
                assertTrue(ttl > 90 && ttl <= 100, "ttl was " + ttl);
                assertNull(c.call("GET", "gone"), "expired keys shouldn't come back after a restart");
            }
        }
    }

    @Test
    void configGet(@TempDir Path dir) throws Exception {
        Config config = Config.defaults().withPort(0).withDir(dir.toString()).withDbFilename("x.rdb");
        try (RedisServer server = new RedisServer(config)) {
            server.start();
            try (TestClient c = new TestClient(server.port())) {
                assertEquals(List.of("dbfilename", "x.rdb"), c.call("CONFIG", "GET", "dbfilename"));
                assertEquals(List.of("dir", dir.toAbsolutePath().normalize().toString()),
                        c.call("CONFIG", "GET", "dir"));
                assertEquals(4, ((List<?>) c.call("CONFIG", "GET", "d*")).size());
            }
        }
    }
}
