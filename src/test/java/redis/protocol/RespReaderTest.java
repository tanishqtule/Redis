package redis.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

class RespReaderTest {

    private static RespReader reader(String s) {
        return new RespReader(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
    }

    private static List<String> strings(List<byte[]> args) {
        return args.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toList();
    }

    @Test
    void readsArrayOfBulkStrings() throws IOException {
        RespReader r = reader("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        assertEquals(List.of("SET", "key", "value"), strings(r.readCommand()));
        assertNull(r.readCommand());
    }

    @Test
    void readsPipelinedCommandsOneAtATime() throws IOException {
        RespReader r = reader("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n");
        assertEquals(List.of("PING"), strings(r.readCommand()));
        assertEquals(List.of("ECHO", "hi"), strings(r.readCommand()));
        assertNull(r.readCommand());
    }

    @Test
    void bulkLengthIsInBytesNotChars() throws IOException {
        // "é" is two bytes in UTF-8. Counting chars instead of bytes is an easy bug to write.
        byte[] wire = "*2\r\n$4\r\nECHO\r\n$2\r\né\r\n".getBytes(StandardCharsets.UTF_8);
        List<byte[]> args = new RespReader(new ByteArrayInputStream(wire)).readCommand();
        assertEquals("é", new String(args.get(1), StandardCharsets.UTF_8));
    }

    @Test
    void valuesMayContainCrlfAndStars() throws IOException {
        RespReader r = reader("*2\r\n$4\r\nECHO\r\n$6\r\na\r\n*b\n\r\n");
        assertEquals("a\r\n*b\n", strings(r.readCommand()).get(1));
    }

    @Test
    void survivesCommandSplitAcrossReads() throws IOException {
        // Hand out one byte per read() call, the worst case TCP can give us.
        byte[] wire = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n".getBytes(StandardCharsets.US_ASCII);
        InputStream trickle = new InputStream() {
            int pos;

            @Override
            public int read() {
                return pos < wire.length ? wire[pos++] & 0xff : -1;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (pos >= wire.length) {
                    return -1;
                }
                b[off] = wire[pos++];
                return 1;
            }
        };
        assertEquals(List.of("GET", "foo"), strings(new RespReader(trickle).readCommand()));
    }

    @Test
    void readsInlineCommands() throws IOException {
        RespReader r = reader("SET  greeting   hello\r\nPING\n");
        assertEquals(List.of("SET", "greeting", "hello"), strings(r.readCommand()));
        assertEquals(List.of("PING"), strings(r.readCommand()));
    }

    @Test
    void blankInlineLineIsEmptyCommand() throws IOException {
        assertTrue(reader("\r\n").readCommand().isEmpty());
    }

    @Test
    void countsConsumedBytes() throws IOException {
        String ping = "*1\r\n$4\r\nPING\r\n";
        RespReader r = reader(ping + ping);
        r.readCommand();
        assertEquals(ping.length(), r.consumed());
        r.readCommand();
        assertEquals(2L * ping.length(), r.consumed());
    }

    @Test
    void rejectsGarbage() {
        assertThrows(ProtocolException.class, () -> reader("*1\r\n:12\r\n").readCommand());
        assertThrows(ProtocolException.class, () -> reader("*x\r\n").readCommand());
        assertThrows(ProtocolException.class, () -> reader("*1\r\n$3\r\nabcde\r\n").readCommand());
    }

    @Test
    void truncatedCommandIsEof() {
        assertThrows(EOFException.class, () -> reader("*2\r\n$3\r\nGET\r\n").readCommand());
    }

    @Test
    void readsRdbPayloadWithoutTrailingCrlf() throws IOException {
        RespReader r = reader("$3\r\nabc*1\r\n$4\r\nPING\r\n");
        assertArrayEquals("abc".getBytes(StandardCharsets.US_ASCII), r.readRdbPayload());
        assertEquals(List.of("PING"), strings(r.readCommand()));
    }

    @Test
    void encodesReplies() {
        assertEquals("+OK\r\n", ascii(Resp.OK));
        assertEquals(":-7\r\n", ascii(Resp.integer(-7)));
        assertEquals("$-1\r\n", ascii(Resp.bulk((byte[]) null)));
        assertEquals("$2\r\nhi\r\n", ascii(Resp.bulk("hi")));
        assertEquals("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", ascii(Resp.command("GET", "k")));
        assertEquals("-ERR a  b\r\n", ascii(Resp.error("ERR a\r\nb")));
    }

    private static String ascii(byte[] b) {
        return new String(b, StandardCharsets.US_ASCII);
    }
}
