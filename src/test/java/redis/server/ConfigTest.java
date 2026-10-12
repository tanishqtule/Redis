package redis.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import redis.store.Eviction;

class ConfigTest {

    @Test
    void defaultsMatchRedis() {
        Config c = Config.defaults();
        assertEquals(6379, c.port());
        assertTrue(c.protectedMode());
        assertNull(c.requirePass());
        assertEquals(Config.DEFAULT_SAVE_RULES, c.saveRules());
        assertEquals(10_000, c.maxClients());
        assertEquals(Eviction.Policy.NOEVICTION, c.maxMemoryPolicy());
        assertEquals(512 * 1024 * 1024, c.protoMaxBulkLen());
    }

    @Test
    void readsAConfigFileWithQuotesAndComments(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("redis.conf");
        Files.writeString(file, """
                # a comment
                port 7000
                bind 127.0.0.1 ::1
                requirepass "s3cret with \\"quotes\\""
                maxmemory 100mb
                maxmemory-policy allkeys-lru
                save 900 1
                save 300 10
                loglevel notice
                """);
        Config c = Config.fromArgs(new String[] {file.toString()});
        assertEquals(7000, c.port());
        assertEquals(List.of("127.0.0.1", "::1"), c.bind());
        assertEquals("s3cret with \"quotes\"", c.requirePass());
        assertEquals(100L * 1024 * 1024, c.maxMemory());
        assertEquals(Eviction.Policy.ALLKEYS_LRU, c.maxMemoryPolicy());
        assertEquals(List.of(new Config.SaveRule(900, 1), new Config.SaveRule(300, 10)), c.saveRules());
    }

    @Test
    void commandLineOverridesTheFile(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("redis.conf");
        Files.writeString(file, "port 7000\n");
        Config c = Config.fromArgs(new String[] {file.toString(), "--port", "7001", "--replicaof", "10.0.0.5 6379",
                "--save", ""});
        assertEquals(7001, c.port());
        assertEquals("10.0.0.5", c.masterHost());
        assertEquals(6379, c.masterPort());
        assertTrue(c.saveRules().isEmpty());
        assertEquals("10.0.0.6", Config.fromArgs(new String[] {"--replicaof", "10.0.0.6", "6380"}).masterHost());
    }

    @Test
    void sizesUseRedisUnits() {
        assertEquals(1000, Config.bytes("x", "1k"));
        assertEquals(1024, Config.bytes("x", "1kb"));
        assertEquals(5_000_000, Config.bytes("x", "5m"));
        assertEquals(2L * 1024 * 1024 * 1024, Config.bytes("x", "2GB"));
        assertEquals(123, Config.bytes("x", "123"));
    }

    @Test
    void secretsCanComeFromFiles(@TempDir Path dir) throws Exception {
        Path secret = dir.resolve("pw");
        Files.writeString(secret, "from-a-file\n");
        Config c = Config.defaults().with("requirepass-file", secret.toString()).with("masterauth-file", secret.toString());
        assertEquals("from-a-file", c.requirePass());
        assertEquals("from-a-file", c.masterAuth());
    }

    @Test
    void rejectsMistakesInsteadOfGuessing() {
        assertThrows(IllegalArgumentException.class, () -> Config.defaults().with("prot", "6379"));
        assertThrows(IllegalArgumentException.class, () -> Config.defaults().with("port", "70000"));
        assertThrows(IllegalArgumentException.class, () -> Config.defaults().with("protected-mode", "maybe"));
        assertThrows(IllegalArgumentException.class, () -> Config.defaults().with("dbfilename", "../../etc/cron.d/x"));
        assertThrows(IllegalArgumentException.class, () -> Config.defaults().with("maxmemory-policy", "lru"));
        assertThrows(IllegalArgumentException.class, () -> Config.defaults().with("appendonly", "yes"));
        assertThrows(IllegalArgumentException.class, () -> Config.defaults().with("save", "60"));
        assertThrows(IllegalArgumentException.class, () -> Config.fromArgs(new String[] {"--port"}));
    }

    @Test
    void replicaofNoOneTurnsReplicationOff() {
        Config c = Config.defaults().withMaster("10.0.0.5", 6379).with("replicaof", "no", "one");
        assertFalse(c.isReplica());
    }

    @Test
    void splitHandlesRedisQuoting() {
        assertEquals(List.of("a", "b c", "d'e", "\n"), Config.split("a \"b c\" 'd\\'e' \"\\x0a\""));
        assertThrows(IllegalArgumentException.class, () -> Config.split("save \"60"));
    }
}
