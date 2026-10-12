package redis.replication;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import redis.persistence.Rdb;
import redis.server.Session;
import redis.store.Keyspace;

/**
 * The master's view of one connected replica.
 *
 * Writes are queued and sent by a dedicated thread, so a slow replica never
 * holds up the command lock, and the queue keeps the stream in exactly the
 * order commands executed. Nothing here may block the caller of send(): it
 * runs under the command lock.
 */
final class ReplicaHandle {

    private static final Logger log = Logger.getLogger(ReplicaHandle.class.getName());

    /** One queued item: either bytes, or the snapshot to serialize (outside the lock) and send. */
    private record Pending(long dueAt, byte[] bytes, Keyspace.Snapshot snapshot) {
    }

    final Session session;
    private final OutputStream out;
    private final long lagMillis;
    private final long outputLimit;
    private final BlockingQueue<Pending> outbound = new LinkedBlockingQueue<>();
    private final AtomicLong queuedBytes = new AtomicLong();
    private final AtomicBoolean dropped = new AtomicBoolean();
    private final Thread writer;
    private volatile long ackedOffset;

    /**
     * @param lagMillis   artificial delay before anything is sent; 0 except in experiments
     * @param outputLimit bytes that may wait in the queue before the replica is cut off; 0 for no limit
     */
    ReplicaHandle(Session session, long startOffset, long lagMillis, long outputLimit) {
        this.session = session;
        this.out = session.out();
        this.lagMillis = lagMillis;
        this.outputLimit = outputLimit;
        this.ackedOffset = startOffset;
        this.writer = Thread.ofVirtual().name("replica-writer-" + session.peer()).unstarted(this::drain);
    }

    void start() {
        writer.start();
    }

    void send(byte[] bytes) {
        if (dropped.get()) {
            return;
        }
        long queued = queuedBytes.addAndGet(bytes.length);
        if (outputLimit > 0 && queued > outputLimit) {
            // A replica that can't keep up (or a client that sent PSYNC and
            // stopped reading) would otherwise make the master buffer without bound.
            drop("its output buffer passed " + outputLimit + " bytes");
            return;
        }
        // Stamping the deadline here, rather than when the writer picks the
        // item up, keeps the delay constant instead of piling up under load.
        outbound.add(new Pending(System.currentTimeMillis() + lagMillis, bytes, null));
    }

    /** Queues the initial snapshot. It's serialized by the writer thread, not by the caller. */
    void sendSnapshot(Keyspace.Snapshot snapshot) {
        outbound.add(new Pending(System.currentTimeMillis() + lagMillis, null, snapshot));
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

    private void drop(String reason) {
        if (dropped.compareAndSet(false, true)) {
            log.warning("disconnecting replica " + session.peer() + ": " + reason);
            outbound.clear();
            // Closing the socket never blocks, unlike flushing to a peer that isn't reading.
            session.kill();
            writer.interrupt();
        }
    }

    private void drain() {
        try {
            while (true) {
                Pending next = outbound.take();
                sleepUntil(next.dueAt());
                write(next);
                // Batch whatever else is already due before paying for a flush.
                while ((next = outbound.peek()) != null && next.dueAt() <= System.currentTimeMillis()) {
                    write(outbound.poll());
                }
                out.flush();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            drop("write failed: " + e.getMessage());
        }
    }

    private void write(Pending item) throws IOException {
        if (item.snapshot() != null) {
            byte[] rdb = Rdb.write(item.snapshot());
            out.write(("$" + rdb.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(rdb);
        } else {
            out.write(item.bytes());
            queuedBytes.addAndGet(-item.bytes().length);
        }
    }

    private static void sleepUntil(long deadline) throws InterruptedException {
        long wait = deadline - System.currentTimeMillis();
        if (wait > 0) {
            Thread.sleep(wait);
        }
    }
}
