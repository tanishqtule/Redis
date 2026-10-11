package redis.persistence;

import java.io.IOException;

/**
 * LZF decompression. Redis compresses string values longer than 20 bytes in
 * RDB files with LZF (rdbcompression is on by default), so dumps from a real
 * server can't be loaded without this.
 *
 * Format: a control byte below 32 means "copy the next ctrl+1 bytes
 * literally". Anything else is a back-reference: the top 3 bits are the
 * length (7 = read one more length byte), the low 5 bits plus the next byte
 * are the distance back into the output.
 */
final class Lzf {

    private Lzf() {
    }

    static byte[] decompress(byte[] in, int expectedLength) throws IOException {
        byte[] out = new byte[expectedLength];
        int ip = 0;
        int op = 0;
        try {
            while (ip < in.length) {
                int ctrl = in[ip++] & 0xff;
                if (ctrl < 32) {
                    int len = ctrl + 1;
                    System.arraycopy(in, ip, out, op, len);
                    ip += len;
                    op += len;
                } else {
                    int len = ctrl >> 5;
                    if (len == 7) {
                        len += in[ip++] & 0xff;
                    }
                    len += 2;
                    int ref = op - ((ctrl & 0x1f) << 8) - (in[ip++] & 0xff) - 1;
                    if (ref < 0) {
                        throw new IOException("corrupt LZF data: back-reference before start");
                    }
                    // Byte by byte on purpose: source and destination may overlap.
                    for (int i = 0; i < len; i++) {
                        out[op++] = out[ref++];
                    }
                }
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("corrupt LZF data", e);
        }
        if (op != expectedLength) {
            throw new IOException("LZF data decoded to " + op + " bytes, expected " + expectedLength);
        }
        return out;
    }
}
