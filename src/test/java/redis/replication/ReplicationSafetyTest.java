package redis.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.jupiter.api.Test;

import redis.command.CommandTable;
import redis.command.KeyCommands;
import redis.protocol.Resp;
import redis.protocol.RespClient;
import redis.server.Config;
import redis.server.RedisServer;
import redis.server.Session;
import redis.store.Keyspace;

/** Regression tests for the replication problems found in the security review. */
class ReplicationSafetyTest {

    /** Stands in for a socket whose peer stopped reading: anything that reaches it blocks. */
    private static final class StuckStream extends OutputStream {
        @Override
        public void write(int b) {
            block();
        }

        @Override
        public void write(byte[] b, int off, int len) {
            block();
        }

        private static void block() {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private record Setup(ReentrantLock lock, Keyspace db, CommandTable table, ReplicationMaster master) {
    }

    private static Setup setup(long outputLimit) {
        ReentrantLock lock = new ReentrantLock();
        Keyspace db = new Keyspace();
        CommandTable table = new CommandTable(lock, false);
        ReplicationMaster master = new ReplicationMaster(lock, 0, outputLimit);
        new KeyCommands(db, 1 << 20).registerAll(table);
        new ReplicationCommands(master, null, db).registerAll(table);
        table.setReplicationSink(master::propagate);
        return new Setup(lock, db, table, master);
    }

    private static List<byte[]> cmd(String... args) {
        return java.util.Arrays.stream(args).map(a -> a.getBytes(StandardCharsets.ISO_8859_1)).toList();
    }

    @Test
    void psyncFromAClientThatStoppedReadingDoesNotFreezeTheServer() throws Exception {
        Setup s = setup(0);
        BufferedOutputStream buffered = new BufferedOutputStream(new StuckStream());
        buffered.write("+PONG\r\n".getBytes(StandardCharsets.US_ASCII)); // a reply still waiting to be sent
        Session attacker = new Session("attacker", buffered, false);
        Thread.ofVirtual().start(() -> s.table().dispatch(attacker, cmd("PSYNC", "?", "-1")));
        Thread.sleep(200);

        Session victim = new Session("victim", OutputStream.nullOutputStream(), false);
        byte[] reply = CompletableFuture.supplyAsync(() -> s.table().dispatch(victim, cmd("SET", "k", "v")))
                .get(2, TimeUnit.SECONDS);
        assertEquals("+OK\r\n", new String(reply, StandardCharsets.US_ASCII));
    }

    @Test
    void secondPsyncOnTheSameConnectionIsRefused() {
        Setup s = setup(0);
        Session replica = new Session("replica", OutputStream.nullOutputStream(), false);
        assertNull(s.table().dispatch(replica, cmd("PSYNC", "?", "-1")));
        byte[] again = s.table().dispatch(replica, cmd("PSYNC", "?", "-1"));
        assertTrue(Resp.isError(again));
    }

    @Test
    void replicaThatFallsTooFarBehindIsDisconnected() {
        Setup s = setup(64 * 1024);
        Session slow = new Session("slow", new BufferedOutputStream(new StuckStream()), false);
        AtomicBoolean killed = new AtomicBoolean();
        slow.setCloser(() -> killed.set(true));
        s.table().dispatch(slow, cmd("PSYNC", "?", "-1"));

        Session writer = new Session("writer", OutputStream.nullOutputStream(), false);
        String value = "x".repeat(10_000);
        for (int i = 0; i < 20 && !killed.get(); i++) {
            s.table().dispatch(writer, cmd("SET", "k" + i, value));
        }
        assertTrue(killed.get(), "200 KB queued for a replica with a 64 KB limit should have cut it off");
    }

    /**
     * A master that sends a corrupt snapshot used to kill the replication
     * thread for good. Now the replica has to keep coming back.
     */
    @Test
    void replicaKeepsRetryingAfterAHostileSnapshot() throws Exception {
        byte[] bomb = hostileRdb();
        AtomicInteger attempts = new AtomicInteger();
        try (ServerSocket fakeMaster = new ServerSocket(0)) {
            Thread.ofVirtual().start(() -> {
                while (!fakeMaster.isClosed()) {
                    try {
                        Socket c = fakeMaster.accept();
                        attempts.incrementAndGet();
                        Thread.ofVirtual().start(() -> serveHostileSync(c, bomb));
                    } catch (IOException e) {
                        return;
                    }
                }
            });
            Config config = Config.defaults().withPort(0).withMaster("127.0.0.1", fakeMaster.getLocalPort());
            try (RedisServer replica = new RedisServer(config)) {
                replica.start();
                Thread.sleep(3500);
                try (RespClient c = new RespClient(replica.port())) {
                    assertInstanceOf(List.class, c.call("ROLE"));
                }
            }
        }
        assertTrue(attempts.get() >= 3, "replica connected only " + attempts.get() + " times in 3.5 s");
    }

    private static void serveHostileSync(Socket c, byte[] rdb) {
        try (c) {
            InputStream in = c.getInputStream();
            OutputStream out = c.getOutputStream();
            String[] replies = {"+PONG\r\n", "+OK\r\n", "+OK\r\n"};
            for (String reply : replies) {
                skipCommand(in);
                out.write(reply.getBytes(StandardCharsets.US_ASCII));
            }
            skipCommand(in);
            out.write(("+FULLRESYNC " + "a".repeat(40) + " 0\r\n$" + rdb.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(rdb);
            out.flush();
            Thread.sleep(500);
        } catch (IOException | InterruptedException ignored) {
            // the replica hangs up on us, which is the point
        }
    }

    private static void skipCommand(InputStream in) throws IOException {
        int lines = Integer.parseInt(readLine(in).substring(1)) * 2;
        for (int i = 0; i < lines; i++) {
            readLine(in);
        }
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != '\n') {
            if (b < 0) {
                throw new IOException("closed");
            }
            if (b != '\r') {
                sb.append((char) b);
            }
        }
        return sb.toString();
    }

    private static byte[] hostileRdb() {
        byte[] body = {(byte) 0xFE, 0x00, 0x00, 0x01, 'k', (byte) 0xC3, 0x03, (byte) 0x80, 0x7f, (byte) 0xff,
                (byte) 0xff, (byte) 0xff, 0x00, 'a', 'b', (byte) 0xFF, 0, 0, 0, 0, 0, 0, 0, 0};
        byte[] header = "REDIS0009".getBytes(StandardCharsets.US_ASCII);
        byte[] out = new byte[header.length + body.length];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(body, 0, out, header.length, body.length);
        return out;
    }
}
