package redis.persistence;

/** The CRC-64 variant Redis uses for RDB checksums ("Jones" polynomial, reflected, no final xor). */
final class Crc64 {

    private static final long POLY_REFLECTED = 0x95ac9329ac4bc9b5L;
    private static final long[] TABLE = new long[256];

    static {
        for (int i = 0; i < 256; i++) {
            long crc = i;
            for (int bit = 0; bit < 8; bit++) {
                crc = (crc & 1) != 0 ? (crc >>> 1) ^ POLY_REFLECTED : crc >>> 1;
            }
            TABLE[i] = crc;
        }
    }

    private Crc64() {
    }

    static long of(byte[] data, int offset, int length) {
        long crc = 0;
        for (int i = offset; i < offset + length; i++) {
            crc = TABLE[(int) ((crc ^ data[i]) & 0xff)] ^ (crc >>> 8);
        }
        return crc;
    }
}
