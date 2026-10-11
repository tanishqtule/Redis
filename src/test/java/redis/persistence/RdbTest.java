package redis.persistence;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import redis.store.Keyspace;

class RdbTest {

    private record Loaded(String value, long expireAt) {
    }

    private static Map<String, Loaded> load(byte[] rdb) throws IOException {
        Map<String, Loaded> out = new HashMap<>();
        Rdb.read(rdb, (k, v, at) -> out.put(k, new Loaded(new String(v, StandardCharsets.UTF_8), at)));
        return out;
    }

    @Test
    void crc64MatchesTheReferenceCheckValue() {
        byte[] check = "123456789".getBytes(StandardCharsets.US_ASCII);
        assertEquals(0xe9c6d914c4b8d9caL, Crc64.of(check, 0, check.length));
    }

    @Test
    void lzfLiteralAndBackReference() throws IOException {
        // One literal 'a', then "copy 9 bytes starting 1 back": ten a's.
        byte[] compressed = {0x00, 'a', (byte) 0xE0, 0x00, 0x00};
        assertArrayEquals("aaaaaaaaaa".getBytes(StandardCharsets.US_ASCII), Lzf.decompress(compressed, 10));
    }

    @Test
    void lzfRejectsCorruptInput() {
        assertThrows(IOException.class, () -> Lzf.decompress(new byte[] {0x20, 0x05}, 10));
        assertThrows(IOException.class, () -> Lzf.decompress(new byte[] {0x05, 'a'}, 6));
    }

    /** The fixture was written by a real redis-server 7.0.15; see src/test/resources. */
    @Test
    void readsADumpWrittenByRealRedis() throws IOException {
        byte[] rdb;
        try (InputStream in = getClass().getResourceAsStream("/redis-7.0-strings.rdb")) {
            rdb = in.readAllBytes();
        }
        Map<String, Loaded> keys = load(rdb);

        assertEquals("hello world", keys.get("greeting").value());
        assertEquals("12345", keys.get("counter").value());      // stored as a 16-bit int
        assertEquals("-7", keys.get("small").value());           // 8-bit int
        assertEquals("2147483000", keys.get("big").value());     // 32-bit int
        assertEquals("abcdefghij".repeat(20), keys.get("long").value()); // LZF-compressed
        assertEquals(4102444800000L, keys.get("expires-far").expireAt());
        assertEquals(Keyspace.NO_EXPIRY, keys.get("greeting").expireAt());
        assertEquals(6, keys.size(), "the key in db 1 should be skipped");
    }

    @Test
    void detectsCorruption() throws IOException {
        byte[] rdb;
        try (InputStream in = getClass().getResourceAsStream("/redis-7.0-strings.rdb")) {
            rdb = in.readAllBytes();
        }
        rdb[170] ^= 0x01;
        assertThrows(IOException.class, () -> load(rdb));
        assertThrows(IOException.class, () -> load("not an rdb".getBytes(StandardCharsets.US_ASCII)));
    }

    @Test
    void roundTrip() throws IOException {
        Keyspace db = new Keyspace();
        long later = System.currentTimeMillis() + 60_000;
        db.set("plain", "value".getBytes(StandardCharsets.UTF_8), Keyspace.NO_EXPIRY);
        db.set("ttl", "soon".getBytes(StandardCharsets.UTF_8), later);
        db.set("big", "x".repeat(70_000).getBytes(StandardCharsets.UTF_8), Keyspace.NO_EXPIRY);
        db.set("binary\u0000key", new byte[] {0, (byte) 0xff, '\r', '\n'}, Keyspace.NO_EXPIRY);

        Map<String, Loaded> keys = load(Rdb.write(db));

        assertEquals(4, keys.size());
        assertEquals(new Loaded("value", Keyspace.NO_EXPIRY), keys.get("plain"));
        assertEquals(new Loaded("soon", later), keys.get("ttl"));
        assertEquals(70_000, keys.get("big").value().length());
    }

    /**
     * Hands our dump to a real redis-server and checks it loads. Skipped when
     * redis-server isn't installed, so CI without Redis still passes.
     */
    @Test
    void realRedisCanLoadWhatWeWrite(@TempDir Path dir) throws Exception {
        assumeTrue(onPath("redis-server") && onPath("redis-cli"), "redis-server not installed");

        Keyspace db = new Keyspace();
        db.set("from-java", "hi".getBytes(StandardCharsets.UTF_8), Keyspace.NO_EXPIRY);
        db.set("with-ttl", "x".getBytes(StandardCharsets.UTF_8), System.currentTimeMillis() + 3_600_000);
        Files.write(dir.resolve("dump.rdb"), Rdb.write(db));

        int port = 17_000 + (int) (ProcessHandle.current().pid() % 1000);
        Process redis = new ProcessBuilder("redis-server", "--port", Integer.toString(port),
                "--dir", dir.toString(), "--dbfilename", "dump.rdb", "--save", "")
                .redirectErrorStream(true).redirectOutput(dir.resolve("redis.log").toFile()).start();
        try {
            String value = "";
            for (int attempt = 0; attempt < 50 && !value.equals("hi"); attempt++) {
                Thread.sleep(100);
                value = cli(port, "GET", "from-java");
            }
            assertEquals("hi", value, Files.readString(dir.resolve("redis.log")));
            long ttl = Long.parseLong(cli(port, "TTL", "with-ttl"));
            assertEquals(3600, ttl, 2);
        } finally {
            redis.destroy();
            redis.waitFor(5, TimeUnit.SECONDS);
        }
    }

    private static String cli(int port, String... args) throws Exception {
        String[] cmd = new String[args.length + 3];
        cmd[0] = "redis-cli";
        cmd[1] = "-p";
        cmd[2] = Integer.toString(port);
        System.arraycopy(args, 0, cmd, 3, args.length);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        p.waitFor();
        return out;
    }

    private static boolean onPath(String exe) {
        String path = System.getenv("PATH");
        if (path == null) {
            return false;
        }
        for (String dir : path.split(java.io.File.pathSeparator)) {
            if (Files.isExecutable(Path.of(dir, exe))) {
                return true;
            }
        }
        return false;
    }
}
