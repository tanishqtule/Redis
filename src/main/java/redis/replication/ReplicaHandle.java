package redis.replication;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import redis.server.Session;

/**
 * The master's view of one connected replica.
 *
 * Writes are queued and sent by a dedicated thread, so a slow replica never
 * holds up the command lock, and the queue keeps the stream in exactly the
 * order commands executed.
 */
final class ReplicaHandle {

    private static final Logger log = Logger.getLogger(ReplicaHandle.class.getName());

    private record Pending(long dueAt, byte[] bytes) {
    }

    final Session session;
    private final OutputStream out;
    private final long lagMillis;
    private final BlockingQueue<Pending> outbound = new LinkedBlockingQueue<>();
    private final Thread writer;
    private volatile long ackedOffset;

    /** @param lagMillis artificial delay before anything is sent; 0 except in experiments */
    ReplicaHandle(Session session, long startOffset, long lagMillis) {
        this.session = session;
        this.out = session.out();
        this.lagMillis = lagMillis;
        this.ackedOffset = startOffset;
        this.writer = Thread.ofVirtual().name("replica-writer-" + session.peer()).unstarted(this::drain);
    }

    void start() {
        writer.start();
    }

    void send(byte[] bytes) {
        // Stamping the deadline here, rather than when the writer picks the
        // item up, keeps the delay constant instead of piling up under load.
        outbound.add(new Pending(System.currentTimeMillis() + lagMillis, bytes));
    }

    long ackedOffset() {
        return ackedOffset;
    }

    void ack(long offset) {
        ackedOffset = offset;
    }

    void stop() {
        writer.interrupt();
    }

    private void drain() {
        try {
            while (true) {
                Pending next = outbound.take();
                sleepUntil(next.dueAt());
                out.write(next.bytes());
                // Batch whatever else is already due before paying for a flush.
                while ((next = outbound.peek()) != null && next.dueAt() <= System.currentTimeMillis()) {
                    out.write(outbound.poll().bytes());
                }
                out.flush();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.info("lost replica " + session.peer() + ": " + e.getMessage());
            try {
                out.close();
            } catch (IOException ignored) {
                // Already broken; closing just makes the read side notice too.
            }
        }
    }

    private static void sleepUntil(long deadline) throws InterruptedException {
        long wait = deadline - System.currentTimeMillis();
        if (wait > 0) {
            Thread.sleep(wait);
        }
    }
}
