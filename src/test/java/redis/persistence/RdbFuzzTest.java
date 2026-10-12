package redis.persistence;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * A replica loads whatever snapshot its master sends, so a corrupt or hostile
 * RDB must end in an IOException: never an Error, never a huge allocation,
 * never an unchecked exception that kills the replication thread.
 */
class RdbFuzzTest {

    private static byte[] fixture() throws IOException {
        try (InputStream in = RdbFuzzTest.class.getResourceAsStream("/redis-7.0-strings.rdb")) {
            return in.readAllBytes();
        }
    }

    /** A value that claims to decompress to 2 GB from three bytes. */
    @Test
    void lzfBombIsRejectedBeforeAllocating() {
        byte[] rdb = rdb(new byte[] {0x00, 0x01, 'k', (byte) 0xC3, 0x03, (byte) 0x80, 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                0x00, 'a', 'b'});
        assertThrows(IOException.class, () -> Rdb.read(rdb, (k, v, e) -> { }));
    }

    /** A length chosen so that position + length overflows an int. */
    @Test
    void lengthOverflowIsRejected() {
        byte[] rdb = rdb(new byte[] {0x00, 0x01, 'k', (byte) 0x80, 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xf0, 'x'});
        assertThrows(IOException.class, () -> Rdb.read(rdb, (k, v, e) -> { }));
    }

    @Test
    void mutatedSnapshotsOnlyEverFailWithIOException() throws IOException {
        byte[] original = fixture();
        Random random = new Random(77);
        for (int i = 0; i < 20_000; i++) {
            byte[] data = original.clone();
            switch (random.nextInt(3)) {
                case 0 -> {
                    for (int flips = random.nextInt(5); flips >= 0; flips--) {
                        data[9 + random.nextInt(data.length - 9)] = (byte) random.nextInt(256);
                    }
                }
                case 1 -> data = java.util.Arrays.copyOf(data, 9 + random.nextInt(data.length - 9));
                default -> {
                    byte[] noise = new byte[random.nextInt(40)];
                    random.nextBytes(noise);
                    byte[] longer = java.util.Arrays.copyOf(data, data.length + noise.length);
                    int at = 9 + random.nextInt(data.length - 9);
                    System.arraycopy(data, at, longer, at + noise.length, data.length - at);
                    System.arraycopy(noise, 0, longer, at, noise.length);
                    data = longer;
                }
            }
            try {
                Rdb.read(data, (k, v, e) -> { });
            } catch (IOException expected) {
                // fine: corrupt input reported as corrupt
            }
        }
    }

    private static byte[] rdb(byte[] body) {
        byte[] header = "REDIS0009".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        byte[] out = new byte[header.length + 2 + body.length + 9];
        System.arraycopy(header, 0, out, 0, header.length);
        out[header.length] = (byte) 0xFE;
        out[header.length + 1] = 0x00;
        System.arraycopy(body, 0, out, header.length + 2, body.length);
        out[header.length + 2 + body.length] = (byte) 0xFF;
        return out;
    }
}
