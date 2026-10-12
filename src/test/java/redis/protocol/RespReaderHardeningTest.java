package redis.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.junit.jupiter.api.Test;

/** Inputs from a hostile peer: the parser must fail cleanly and cheaply. */
class RespReaderHardeningTest {

    private static RespReader reader(byte[] bytes) {
        return new RespReader(new ByteArrayInputStream(bytes));
    }

    /** An 11-byte header used to make the server allocate a 4 MB array before any data arrived. */
    @Test
    void declaredCountsDontDriveAllocation() {
        assumeTrue(ManagementFactory.getThreadMXBean() instanceof com.sun.management.ThreadMXBean,
                "needs HotSpot's per-thread allocation counter");
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long thread = Thread.currentThread().threadId();
        byte[] header = "*1048576\r\n".getBytes(StandardCharsets.US_ASCII);

        reader(header); // warm up the classes involved
        long before = threads.getThreadAllocatedBytes(thread);
        for (int i = 0; i < 20; i++) {
            assertThrows(EOFException.class, () -> reader(header).readCommand());
        }
        long allocated = threads.getThreadAllocatedBytes(thread) - before;
        assertTrue(allocated < 2_000_000, "20 headers allocated " + allocated + " bytes");
    }

    @Test
    void unauthenticatedLimitsAreTighter() throws IOException {
        RespReader r = reader("*11\r\n".getBytes(StandardCharsets.US_ASCII));
        r.setLimits(10, 16 * 1024, true);
        ProtocolException tooMany = assertThrows(ProtocolException.class, r::readCommand);
        assertEquals("unauthenticated multibulk length", tooMany.getMessage());

        RespReader big = reader("*2\r\n$4\r\nAUTH\r\n$20000\r\n".getBytes(StandardCharsets.US_ASCII));
        big.setLimits(10, 16 * 1024, true);
        ProtocolException tooBig = assertThrows(ProtocolException.class, big::readCommand);
        assertEquals("unauthenticated bulk length", tooBig.getMessage());
    }

    @Test
    void deeplyNestedRepliesAreRejected() {
        byte[] nested = "*1\r\n".repeat(200_000).getBytes(StandardCharsets.US_ASCII);
        assertThrows(ProtocolException.class, () -> reader(nested).readReply());
    }

    /**
     * Random bytes and mangled commands: the only acceptable outcomes are a
     * parsed command, a clean EOF, or an IOException. Anything else (a runtime
     * exception, an Error) would kill the connection thread unexpectedly.
     */
    @Test
    void fuzzedInputOnlyEverFailsWithIOException() {
        Random random = new Random(1234);
        byte[] valid = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < 50_000; i++) {
            byte[] input;
            if (random.nextBoolean()) {
                input = new byte[random.nextInt(64)];
                random.nextBytes(input);
                if (input.length > 0 && random.nextBoolean()) {
                    input[0] = (byte) "*$+-:"
                            .charAt(random.nextInt(5));
                }
            } else {
                input = valid.clone();
                for (int flips = random.nextInt(4); flips >= 0; flips--) {
                    input[random.nextInt(input.length)] = (byte) random.nextInt(256);
                }
            }
            RespReader r = reader(input);
            RespReader replies = reader(input);
            try {
                while (r.readCommand() != null) {
                    // keep going until the input runs out or breaks
                }
            } catch (IOException expected) {
                // fine
            }
            try {
                replies.readReply();
            } catch (IOException expected) {
                // fine
            }
        }
    }
}
