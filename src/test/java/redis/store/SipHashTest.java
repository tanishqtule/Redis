package redis.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SipHashTest {

    /** The test vector from the SipHash paper (Appendix A): key 00..0f, message 00..0e. */
    @Test
    void matchesThePublishedTestVector() {
        long k0 = 0x0706050403020100L;
        long k1 = 0x0f0e0d0c0b0a0908L;
        byte[] message = new byte[15];
        for (int i = 0; i < message.length; i++) {
            message[i] = (byte) i;
        }
        assertEquals(0xa129ca6149be45e5L, new SipHash(k0, k1).hash(message));
    }
}
