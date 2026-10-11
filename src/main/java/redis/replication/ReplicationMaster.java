package redis.replication;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import redis.command.Args;
import redis.protocol.Resp;
import redis.server.Session;

/**
 * Master-side replication: the replication id and offset, the connected
 * replicas, and the bookkeeping behind WAIT.
 *
 * The offset is the total number of bytes ever put on the replication
 * stream. A replica that reports the same number has applied everything.
 */
public final class ReplicationMaster {

    private static final Logger log = Logger.getLogger(ReplicationMaster.class.getName());
    private static final List<byte[]> GETACK = List.of(Args.raw("REPLCONF"), Args.raw("GETACK"), Args.raw("*"));

    private final String replId = randomReplId();
    private final List<ReplicaHandle> replicas = new CopyOnWriteArrayList<>();
    private final Object ackSignal = new Object();
    private final ReentrantLock commandLock;
    private final long simulatedLagMillis;

    /** Guarded by the command lock for writes; volatile so INFO can read it without the lock. */
    private volatile long offset;

    public ReplicationMaster(ReentrantLock commandLock, long simulatedLagMillis) {
        this.commandLock = commandLock;
        this.simulatedLagMillis = simulatedLagMillis;
    }

    public String replId() {
        return replId;
    }

    public long offset() {
        return offset;
    }

    /** Puts a command on the stream. Caller holds the command lock. */
    public void propagate(List<byte[]> args) {
        byte[] encoded = Resp.command(args);
        offset += encoded.length;
        for (ReplicaHandle replica : replicas) {
            replica.send(encoded);
        }
    }

    /**
     * Turns a connection into a replica. Caller holds the command lock, which
     * is what makes this safe: the snapshot and the start offset describe the
     * same moment, and no write can slip in between.
     */
    public void startFullResync(Session session, byte[] snapshot) {
        ReplicaHandle replica = new ReplicaHandle(session, offset, simulatedLagMillis);
        replica.send(Args.raw("+FULLRESYNC " + replId + " " + offset + "\r\n"));
        replica.send(Args.raw("$" + snapshot.length + "\r\n"));
        replica.send(snapshot);
        session.markReplica();
        replicas.add(replica);
        replica.start();
        log.info("replica " + session.peer() + " attached at offset " + offset);
    }

    public void acknowledge(Session session, long ackedOffset) {
        for (ReplicaHandle replica : replicas) {
            if (replica.session == session) {
                replica.ack(ackedOffset);
            }
        }
        synchronized (ackSignal) {
            ackSignal.notifyAll();
        }
    }

    public void remove(Session session) {
        for (ReplicaHandle replica : replicas) {
            if (replica.session == session) {
                replica.stop();
                replicas.remove(replica);
                log.info("replica " + session.peer() + " detached");
            }
        }
    }

    public int replicaCount() {
        return replicas.size();
    }

    /**
     * WAIT: blocks until {@code needed} replicas have acknowledged every write
     * made before the call, or the timeout passes (0 = no timeout). Returns
     * how many had, which can be fewer than asked for.
     */
    public int waitForReplicas(int needed, long timeoutMillis) throws InterruptedException {
        long target;
        commandLock.lock();
        try {
            target = offset;
            if (caughtUp(target) >= needed) {
                return caughtUp(target);
            }
            // Ask for fresh ACKs. GETACK goes on the stream after the target
            // offset, so a replica's reply to it covers everything we're waiting for.
            propagate(GETACK);
        } finally {
            commandLock.unlock();
        }

        long deadline = timeoutMillis == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMillis;
        synchronized (ackSignal) {
            while (caughtUp(target) < needed) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }
                ackSignal.wait(remaining);
            }
        }
        return caughtUp(target);
    }

    /** Lines for the replication section of INFO. */
    public List<String> info() {
        List<String> lines = new ArrayList<>();
        lines.add("role:master");
        lines.add("connected_slaves:" + replicas.size());
        int i = 0;
        for (ReplicaHandle replica : replicas) {
            lines.add("slave" + i++ + ":ip=" + ipOf(replica) + ",port=" + replica.session.listeningPort()
                    + ",state=online,offset=" + replica.ackedOffset());
        }
        lines.add("master_replid:" + replId);
        lines.add("master_repl_offset:" + offset);
        return lines;
    }

    /** ROLE reply, shaped like Redis': master, offset, then [ip, port, acked offset] per replica. */
    public byte[] role() {
        List<byte[]> entries = new ArrayList<>();
        for (ReplicaHandle replica : replicas) {
            entries.add(Resp.array(List.of(
                    Resp.bulk(ipOf(replica)),
                    Resp.bulk(Integer.toString(replica.session.listeningPort())),
                    Resp.bulk(Long.toString(replica.ackedOffset())))));
        }
        return Resp.array(List.of(Resp.bulk("master"), Resp.integer(offset), Resp.array(entries)));
    }

    private static String ipOf(ReplicaHandle replica) {
        // peer() looks like "/127.0.0.1:53412".
        return replica.session.peer().replaceFirst("^/", "").replaceFirst(":\\d+$", "");
    }

    private int caughtUp(long target) {
        int n = 0;
        for (ReplicaHandle replica : replicas) {
            if (replica.ackedOffset() >= target) {
                n++;
            }
        }
        return n;
    }

    private static String randomReplId() {
        byte[] bytes = new byte[20];
        new SecureRandom().nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }
}
