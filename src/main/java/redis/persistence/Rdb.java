package redis.persistence;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import redis.command.Args;
import redis.store.Keyspace;

/**
 * Reads and writes the RDB snapshot format, string values only.
 *
 * Layout: "REDIS" + 4-digit version, then a stream of opcodes (aux fields,
 * select-db, resize-db hints, optional expiry prefixes, key/value pairs),
 * then 0xFF and an 8-byte little-endian CRC-64 of everything before it.
 * Reference: https://rdb.fnordig.de/file_format.html
 */
public final class Rdb {

    /** Called for each key loaded. {@code expireAt} is unix ms or {@link Keyspace#NO_EXPIRY}. */
    @FunctionalInterface
    public interface Sink {
        void accept(String key, byte[] value, long expireAt);
    }

    // Version 9 is old enough that any Redis from 5.0 up will load what we write.
    private static final String WRITE_VERSION = "0009";
    private static final int MAX_READ_VERSION = 12;

    private static final int OP_FUNCTION2 = 0xF5;
    private static final int OP_FUNCTION = 0xF6;
    private static final int OP_MODULE_AUX = 0xF7;
    private static final int OP_IDLE = 0xF8;
    private static final int OP_FREQ = 0xF9;
    private static final int OP_AUX = 0xFA;
    private static final int OP_RESIZEDB = 0xFB;
    private static final int OP_EXPIRETIME_MS = 0xFC;
    private static final int OP_EXPIRETIME = 0xFD;
    private static final int OP_SELECTDB = 0xFE;
    private static final int OP_EOF = 0xFF;
    private static final int TYPE_STRING = 0;

    private Rdb() {
    }

    // ---- writing ----

    /** Serializes a live keyspace. Caller holds the command lock for the whole call. */
    public static byte[] write(Keyspace db) {
        return write(db.snapshot());
    }

    /** Serializes a snapshot; safe to call without the command lock. */
    public static byte[] write(Keyspace.Snapshot snapshot) {
        Out out = new Out();
        out.raw(("REDIS" + WRITE_VERSION).getBytes(StandardCharsets.US_ASCII));
        out.u8(OP_AUX);
        out.string(Args.raw("redis-bits"));
        out.string(Args.raw("64"));

        out.u8(OP_SELECTDB);
        out.length(0);
        out.u8(OP_RESIZEDB);
        out.length(snapshot.size());
        out.length(snapshot.volatileKeys());

        for (int i = 0; i < snapshot.size(); i++) {
            Keyspace.Entry entry = snapshot.entries().get(i);
            if (entry.expireAt() != Keyspace.NO_EXPIRY) {
                out.u8(OP_EXPIRETIME_MS);
                out.int64LittleEndian(entry.expireAt());
            }
            out.u8(TYPE_STRING);
            out.string(Args.raw(snapshot.keys().get(i)));
            out.string(entry.value());
        }

        out.u8(OP_EOF);
        byte[] body = out.toByteArray();
        long crc = Crc64.of(body, 0, body.length);
        out.int64LittleEndian(crc);
        return out.toByteArray();
    }

    private static final class Out extends ByteArrayOutputStream {
        void raw(byte[] b) {
            writeBytes(b);
        }

        void u8(int b) {
            write(b);
        }

        void length(long n) {
            if (n < 1 << 6) {
                u8((int) n);
            } else if (n < 1 << 14) {
                u8(0x40 | (int) (n >> 8));
                u8((int) n & 0xff);
            } else if (n <= 0xffffffffL) {
                u8(0x80);
                for (int shift = 24; shift >= 0; shift -= 8) {
                    u8((int) (n >> shift) & 0xff);
                }
            } else {
                u8(0x81);
                for (int shift = 56; shift >= 0; shift -= 8) {
                    u8((int) (n >> shift) & 0xff);
                }
            }
        }

        void string(byte[] s) {
            length(s.length);
            raw(s);
        }

        void int64LittleEndian(long n) {
            for (int i = 0; i < 8; i++) {
                u8((int) (n >> (8 * i)) & 0xff);
            }
        }
    }

    // ---- reading ----

    /**
     * Parses a whole RDB file. Only keys from database 0 are kept. Snapshots
     * can come from a replication master, so malformed input of any kind
     * surfaces as an IOException, never as some other exception.
     */
    public static void read(byte[] data, Sink sink) throws IOException {
        try {
            new In(data).readAll(sink);
        } catch (RuntimeException e) {
            throw new IOException("corrupt RDB data: " + e, e);
        }
    }

    private static final class In {
        private final byte[] data;
        private int pos;

        In(byte[] data) {
            this.data = data;
        }

        void readAll(Sink sink) throws IOException {
            if (data.length < 9 || !new String(data, 0, 5, StandardCharsets.US_ASCII).equals("REDIS")) {
                throw new IOException("not an RDB file");
            }
            int version;
            try {
                version = Integer.parseInt(new String(data, 5, 4, StandardCharsets.US_ASCII));
            } catch (NumberFormatException e) {
                throw new IOException("bad RDB version");
            }
            if (version < 1 || version > MAX_READ_VERSION) {
                throw new IOException("unsupported RDB version " + version);
            }
            pos = 9;

            long db = 0;
            long expireAt = Keyspace.NO_EXPIRY;
            while (true) {
                int op = u8();
                switch (op) {
                    case OP_EOF -> {
                        verifyChecksum(version);
                        return;
                    }
                    case OP_SELECTDB -> db = length();
                    case OP_RESIZEDB -> {
                        length();
                        length();
                    }
                    case OP_AUX -> {
                        string();
                        string();
                    }
                    case OP_EXPIRETIME_MS -> expireAt = int64LittleEndian();
                    case OP_EXPIRETIME -> expireAt = (int32LittleEndian() & 0xffffffffL) * 1000;
                    case OP_FREQ -> u8();
                    case OP_IDLE -> length();
                    case OP_FUNCTION, OP_FUNCTION2, OP_MODULE_AUX ->
                            throw new IOException("RDB contains functions or module data, which aren't supported");
                    case TYPE_STRING -> {
                        String key = Args.str(string());
                        byte[] value = string();
                        if (db == 0) {
                            sink.accept(key, value, expireAt);
                        }
                        expireAt = Keyspace.NO_EXPIRY;
                    }
                    default -> throw new IOException(
                            "RDB value type " + op + " isn't supported (only strings are)");
                }
            }
        }

        private void verifyChecksum(int version) throws IOException {
            if (version < 5 || pos == data.length) {
                return;
            }
            int end = pos;
            long stored = int64LittleEndian();
            // A zero checksum means the writer had rdbchecksum turned off.
            if (stored != 0 && stored != Crc64.of(data, 0, end)) {
                throw new IOException("RDB checksum mismatch, file is corrupt");
            }
        }

        private byte[] string() throws IOException {
            int first = u8();
            if ((first & 0xC0) != 0xC0) {
                pos--;
                return bytes(checkedInt(length()));
            }
            return switch (first & 0x3F) {
                case 0 -> Args.raw(Integer.toString((byte) u8()));
                case 1 -> Args.raw(Integer.toString((short) (u8() | u8() << 8)));
                case 2 -> Args.raw(Integer.toString(int32LittleEndian()));
                case 3 -> {
                    int compressedLength = checkedInt(length());
                    int length = checkedInt(length());
                    yield Lzf.decompress(bytes(compressedLength), length);
                }
                default -> throw new IOException("unknown string encoding " + (first & 0x3F));
            };
        }

        private long length() throws IOException {
            int first = u8();
            return switch (first >> 6) {
                case 0 -> first & 0x3F;
                case 1 -> (first & 0x3F) << 8 | u8();
                case 2 -> {
                    int width = first == 0x80 ? 4 : first == 0x81 ? 8 : -1;
                    if (width < 0) {
                        throw new IOException("bad length prefix " + first);
                    }
                    long n = 0;
                    for (int i = 0; i < width; i++) {
                        n = n << 8 | u8();
                    }
                    yield n;
                }
                default -> throw new IOException("expected a length, found an encoded value");
            };
        }

        private int int32LittleEndian() throws IOException {
            return u8() | u8() << 8 | u8() << 16 | u8() << 24;
        }

        private long int64LittleEndian() throws IOException {
            long n = 0;
            for (int i = 0; i < 8; i++) {
                n |= (long) u8() << (8 * i);
            }
            return n;
        }

        private byte[] bytes(int n) throws IOException {
            // Written as a subtraction: pos + n can overflow and pass the check.
            if (n < 0 || n > data.length - pos) {
                throw new IOException("RDB file truncated");
            }
            byte[] out = new byte[n];
            System.arraycopy(data, pos, out, 0, n);
            pos += n;
            return out;
        }

        private int u8() throws IOException {
            if (pos >= data.length) {
                throw new IOException("RDB file truncated");
            }
            return data[pos++] & 0xff;
        }

        private static int checkedInt(long n) throws IOException {
            if (n < 0 || n > Integer.MAX_VALUE) {
                throw new IOException("length " + n + " out of range");
            }
            return (int) n;
        }
    }
}
