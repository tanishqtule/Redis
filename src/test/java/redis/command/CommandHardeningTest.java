package redis.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.jupiter.api.Test;

import redis.server.Session;
import redis.store.Keyspace;

class CommandHardeningTest {

    private final Keyspace db = new Keyspace();
    private final CommandTable table = new CommandTable(new ReentrantLock(), db, false);
    private final Session session = new Session("test", OutputStream.nullOutputStream(), false);

    CommandHardeningTest() {
        new KeyCommands(db, 16).registerAll(table);
    }

    private String call(String... args) {
        List<byte[]> raw = java.util.Arrays.stream(args).map(a -> a.getBytes(StandardCharsets.ISO_8859_1)).toList();
        return new String(table.dispatch(session, raw), StandardCharsets.ISO_8859_1);
    }

    @Test
    void unknownCommandErrorsStaySmall() {
        String reply = call("NOSUCHCOMMAND", "x".repeat(1_000_000), "y".repeat(1_000_000));
        assertTrue(reply.startsWith("-ERR unknown command 'NOSUCHCOMMAND'"), reply);
        assertTrue(reply.length() < 400, "reply was " + reply.length() + " bytes");
        assertTrue(call("Z".repeat(5000)).length() < 400);
    }

    @Test
    void integersMustBeCanonical() {
        call("SET", "k", "v");
        for (String bad : new String[] {"007", "-0", "+5", " 5", "5 ", "1e3", ""}) {
            assertEquals("-ERR value is not an integer or out of range\r\n", call("EXPIRE", "k", bad), bad);
        }
        assertEquals(":1\r\n", call("EXPIRE", "k", "100"));
        call("SET", "n", "007");
        assertTrue(call("INCR", "n").startsWith("-ERR value is not an integer"));
    }

    /** Same rules as Redis 7's expireGenericCommand. */
    @Test
    void expireRejectsOverflowInsteadOfWrapping() {
        call("SET", "k", "v");
        // Converting seconds to milliseconds would overflow: rejected, key untouched.
        assertTrue(call("EXPIRE", "k", Long.toString(Long.MAX_VALUE)).startsWith("-ERR invalid expire time"));
        assertTrue(call("EXPIRE", "k", Long.toString(Long.MIN_VALUE)).startsWith("-ERR invalid expire time"));
        assertEquals(":1\r\n", call("EXISTS", "k"));
        // In milliseconds nothing overflows; the deadline is simply in the past, so the key goes.
        assertEquals(":1\r\n", call("PEXPIRE", "k", Long.toString(Long.MIN_VALUE)));
        assertEquals(":0\r\n", call("EXISTS", "k"));
    }

    @Test
    void appendCannotGrowPastTheValueLimit() {
        assertEquals(":10\r\n", call("APPEND", "k", "0123456789"));
        assertEquals(":16\r\n", call("APPEND", "k", "012345"));
        assertTrue(call("APPEND", "k", "x").startsWith("-ERR string exceeds maximum allowed size"));
        assertEquals(":16\r\n", call("STRLEN", "k"));
    }

    @Test
    void flushallOnlyAcceptsItsOwnOptions() {
        call("SET", "k", "v");
        assertTrue(call("FLUSHALL", "EVERYTHING").startsWith("-ERR syntax error"));
        assertEquals(":1\r\n", call("EXISTS", "k"));
        assertEquals("+OK\r\n", call("FLUSHALL", "async"));
        assertEquals(":0\r\n", call("EXISTS", "k"));
    }
}
