package redis.protocol;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming RESP2 request parser.
 *
 * Reads straight off the socket, so a command split across several TCP
 * segments (or several commands packed into one) is handled naturally.
 * Also tracks how many bytes it has consumed, which the replica side needs
 * for its replication offset.
 */
public final class RespReader {

    private static final int MAX_LINE = 64 * 1024;
    private static final int MAX_ARGS = 1024 * 1024;
    private static final int MAX_BULK = 512 * 1024 * 1024;

    private final InputStream in;
    private long consumed;

    public RespReader(InputStream in) {
        this.in = in instanceof BufferedInputStream ? in : new BufferedInputStream(in);
    }

    public long consumed() {
        return consumed;
    }

    /** True if the next read would not block. Used to decide when to flush pipelined replies. */
    public boolean hasBufferedInput() throws IOException {
        return in.available() > 0;
    }

    /**
     * Reads one command as a list of raw arguments. Returns {@code null} on a
     * clean EOF between commands and an empty list for a blank inline line.
     */
    public List<byte[]> readCommand() throws IOException {
        int first = in.read();
        if (first == -1) {
            return null;
        }
        consumed++;
        if (first != '*') {
            return readInline(first);
        }

        long count = readLength();
        if (count <= 0) {
            return List.of();
        }
        if (count > MAX_ARGS) {
            throw new ProtocolException("invalid multibulk length");
        }
        List<byte[]> args = new ArrayList<>((int) count);
        for (int i = 0; i < count; i++) {
            int type = readByte();
            if (type != '$') {
                throw new ProtocolException("expected '$', got '" + (char) type + "'");
            }
            args.add(readBulkBody());
        }
        return args;
    }

    /** Reads a simple-string reply such as {@code +OK}; an error reply becomes an exception. */
    public String readSimpleString() throws IOException {
        int type = readByte();
        String line = readLine();
        if (type == '-') {
            throw new IOException("server replied with error: " + line);
        }
        if (type != '+') {
            throw new ProtocolException("expected simple string, got '" + (char) type + "'");
        }
        return line;
    }

    /**
     * Reads the snapshot a master sends after FULLRESYNC. It looks like a bulk
     * string but has no trailing CRLF, so it can't go through readBulkBody.
     */
    public byte[] readRdbPayload() throws IOException {
        int type = readByte();
        if (type != '$') {
            throw new ProtocolException("expected RDB payload, got '" + (char) type + "'");
        }
        long length = readLength();
        if (length < 0 || length > MAX_BULK) {
            throw new ProtocolException("invalid RDB payload length " + length);
        }
        return readExactly((int) length);
    }

    private List<byte[]> readInline(int first) throws IOException {
        StringBuilder line = new StringBuilder();
        line.append((char) first);
        line.append(readLine());
        String trimmed = line.toString().trim();
        if (trimmed.isEmpty()) {
            return List.of();
        }
        List<byte[]> args = new ArrayList<>();
        for (String part : trimmed.split("\\s+")) {
            args.add(part.getBytes(StandardCharsets.ISO_8859_1));
        }
        return args;
    }

    private byte[] readBulkBody() throws IOException {
        long length = readLength();
        if (length < 0 || length > MAX_BULK) {
            throw new ProtocolException("invalid bulk length");
        }
        byte[] data = readExactly((int) length);
        if (readByte() != '\r' || readByte() != '\n') {
            throw new ProtocolException("bulk string not terminated by CRLF");
        }
        return data;
    }

    private long readLength() throws IOException {
        String line = readLine();
        try {
            return Long.parseLong(line);
        } catch (NumberFormatException e) {
            throw new ProtocolException("invalid length '" + line + "'");
        }
    }

    /** Reads up to CRLF (or a bare LF, which redis-cli's inline mode can send). */
    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int b = readByte();
            if (b == '\n') {
                int last = sb.length() - 1;
                if (last >= 0 && sb.charAt(last) == '\r') {
                    sb.setLength(last);
                }
                return sb.toString();
            }
            if (sb.length() >= MAX_LINE) {
                throw new ProtocolException("line too long");
            }
            sb.append((char) b);
        }
    }

    private byte[] readExactly(int length) throws IOException {
        byte[] data = in.readNBytes(length);
        if (data.length != length) {
            throw new EOFException("connection closed mid-value");
        }
        consumed += length;
        return data;
    }

    private int readByte() throws IOException {
        int b = in.read();
        if (b == -1) {
            throw new EOFException("connection closed mid-command");
        }
        consumed++;
        return b;
    }
}
