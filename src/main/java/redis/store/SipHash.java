package redis.store;

/**
 * SipHash-2-4 (Aumasson and Bernstein): a fast keyed hash. With a secret
 * key, clients can't predict where their keys land, so they can't line up
 * many keys on one value or learn anything from the order SCAN returns.
 */
final class SipHash {

    private final long k0;
    private final long k1;

    SipHash(long k0, long k1) {
        this.k0 = k0;
        this.k1 = k1;
    }

    long hash(byte[] data) {
        long v0 = 0x736f6d6570736575L ^ k0;
        long v1 = 0x646f72616e646f6dL ^ k1;
        long v2 = 0x6c7967656e657261L ^ k0;
        long v3 = 0x7465646279746573L ^ k1;
        int length = data.length;
        int blocks = length / 8;
        for (int i = 0; i < blocks; i++) {
            long m = littleEndian(data, i * 8);
            v3 ^= m;
            for (int round = 0; round < 2; round++) {
                v0 += v1; v1 = Long.rotateLeft(v1, 13); v1 ^= v0; v0 = Long.rotateLeft(v0, 32);
                v2 += v3; v3 = Long.rotateLeft(v3, 16); v3 ^= v2;
                v0 += v3; v3 = Long.rotateLeft(v3, 21); v3 ^= v0;
                v2 += v1; v1 = Long.rotateLeft(v1, 17); v1 ^= v2; v2 = Long.rotateLeft(v2, 32);
            }
            v0 ^= m;
        }
        long last = ((long) length & 0xff) << 56;
        for (int i = 0; i < length % 8; i++) {
            last |= ((long) data[blocks * 8 + i] & 0xff) << (8 * i);
        }
        v3 ^= last;
        for (int round = 0; round < 2; round++) {
            v0 += v1; v1 = Long.rotateLeft(v1, 13); v1 ^= v0; v0 = Long.rotateLeft(v0, 32);
            v2 += v3; v3 = Long.rotateLeft(v3, 16); v3 ^= v2;
            v0 += v3; v3 = Long.rotateLeft(v3, 21); v3 ^= v0;
            v2 += v1; v1 = Long.rotateLeft(v1, 17); v1 ^= v2; v2 = Long.rotateLeft(v2, 32);
        }
        v0 ^= last;
        v2 ^= 0xff;
        for (int round = 0; round < 4; round++) {
            v0 += v1; v1 = Long.rotateLeft(v1, 13); v1 ^= v0; v0 = Long.rotateLeft(v0, 32);
            v2 += v3; v3 = Long.rotateLeft(v3, 16); v3 ^= v2;
            v0 += v3; v3 = Long.rotateLeft(v3, 21); v3 ^= v0;
            v2 += v1; v1 = Long.rotateLeft(v1, 17); v1 ^= v2; v2 = Long.rotateLeft(v2, 32);
        }
        return v0 ^ v1 ^ v2 ^ v3;
    }

    private static long littleEndian(byte[] b, int at) {
        long n = 0;
        for (int i = 7; i >= 0; i--) {
            n = n << 8 | (b[at + i] & 0xff);
        }
        return n;
    }
}
