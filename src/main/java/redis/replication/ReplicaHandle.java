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

    final Session session;
    private final OutputStream out;
    private final BlockingQueue<byte[]> outbound = new LinkedBlockingQueue<>();
    private final Thread writer;
    private volatile long ackedOffset;

    ReplicaHandle(Session session, long startOffset) {
        this.session = session;
        this.out = session.out();
        this.ackedOffset = startOffset;
        this.writer = Thread.ofVirtual().name("replica-writer-" + session.peer()).unstarted(this::drain);
    }

    void start() {
        writer.start();
    }

    void send(byte[] bytes) {
        outbound.add(bytes);
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
                byte[] next = outbound.take();
                out.write(next);
                // Batch whatever else piled up before paying for a flush.
                while ((next = outbound.poll()) != null) {
                    out.write(next);
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
}
