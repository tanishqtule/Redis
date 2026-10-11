package redis.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Encoders for RESP2 replies. Every reply is a ready-to-write byte array. */
public final class Resp {

    public static final byte[] OK = simple("OK");
    public static final byte[] PONG = simple("PONG");
    public static final byte[] QUEUED = simple("QUEUED");
    public static final byte[] NULL_BULK = ascii("$-1\r\n");
    public static final byte[] NULL_ARRAY = ascii("*-1\r\n");
    public static final byte[] EMPTY_ARRAY = ascii("*0\r\n");

    private Resp() {
    }

    public static byte[] simple(String s) {
        return ascii("+" + s + "\r\n");
    }

    /** {@code message} should start with the error code, e.g. "ERR syntax error". */
    public static byte[] error(String message) {
        // Errors can echo client input (e.g. an unknown command name); a stray
        // CRLF in there would let a client forge extra replies.
        String clean = message.replace('\r', ' ').replace('\n', ' ');
        return ascii("-" + clean + "\r\n");
    }

    public static byte[] integer(long n) {
        return ascii(":" + n + "\r\n");
    }

    public static byte[] bulk(String s) {
        return bulk(s.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] bulk(byte[] data) {
        if (data == null) {
            return NULL_BULK;
        }
        byte[] header = ascii("$" + data.length + "\r\n");
        byte[] out = new byte[header.length + data.length + 2];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(data, 0, out, header.length, data.length);
        out[out.length - 2] = '\r';
        out[out.length - 1] = '\n';
        return out;
    }

    /** Wraps already-encoded replies in an array header. */
    public static byte[] array(List<byte[]> encoded) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ascii("*" + encoded.size() + "\r\n"));
        for (byte[] element : encoded) {
            out.writeBytes(element);
        }
        return out.toByteArray();
    }

    /** Encodes a command the way a client would send it: an array of bulk strings. */
    public static byte[] command(List<byte[]> args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ascii("*" + args.size() + "\r\n"));
        for (byte[] arg : args) {
            out.writeBytes(bulk(arg));
        }
        return out.toByteArray();
    }

    public static byte[] command(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ascii("*" + args.length + "\r\n"));
        for (String arg : args) {
            out.writeBytes(bulk(arg));
        }
        return out.toByteArray();
    }

    public static boolean isError(byte[] reply) {
        return reply != null && reply.length > 0 && reply[0] == '-';
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
