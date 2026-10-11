package redis.replication;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import redis.protocol.RespClient;

/**
 * Asks the master for its current replication position, the "read index" a
 * replica must reach before it can answer a read linearizably. This is the
 * ReadIndex idea from Raft, which etcd and TiKV use for follower reads; here
 * the master's INFO replication provides the index, so it works against a
 * real Redis master too.
 *
 * Concurrent callers share round trips, but only in a way that keeps the
 * guarantee: a caller joins the next request that is sent *after* it
 * arrived, never one already in flight. An offset fetched before a read
 * started could miss a write that finished just before the read.
 */
final class ReadIndex implements AutoCloseable {

    /**
     * Where the master's stream was. The replication id matters as much as the
     * offset: offsets from two different histories (say, before and after the
     * master restarted) can't be compared.
     */
    record Position(String replId, long offset) {
    }

    private final String host;
    private final int port;
    private final int timeoutMillis;

    private final Object lock = new Object();
    /** Callers waiting for a request that hasn't been sent yet. Guarded by lock. */
    private CompletableFuture<Position> nextRound;
    private boolean roundInFlight;

    /** Used by one round at a time (rounds never overlap); close() may also shut it from outside. */
    private volatile RespClient master;
    private final AtomicLong roundTrips = new AtomicLong();

    ReadIndex(String host, int port, int timeoutMillis) {
        this.host = host;
        this.port = port;
        this.timeoutMillis = timeoutMillis;
    }

    /** Returns a position the master had reached at some point after this call began. */
    Position fetch(long timeoutMillis) throws IOException, TimeoutException, InterruptedException {
        CompletableFuture<Position> round;
        synchronized (lock) {
            if (nextRound == null) {
                nextRound = new CompletableFuture<>();
            }
            round = nextRound;
            if (!roundInFlight) {
                startRound();
            }
        }
        try {
            return round.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
        }
    }

    long roundTrips() {
        return roundTrips.get();
    }

    @Override
    public void close() {
        closeConnection();
    }

    // Caller holds lock.
    private void startRound() {
        CompletableFuture<Position> round = nextRound;
        nextRound = null;
        roundInFlight = true;
        Thread.ofVirtual().name("read-index").start(() -> {
            try {
                round.complete(askMaster());
            } catch (IOException | RuntimeException e) {
                round.completeExceptionally(e);
            } finally {
                synchronized (lock) {
                    roundInFlight = false;
                    if (nextRound != null) {
                        startRound();
                    }
                }
            }
        });
    }

    private Position askMaster() throws IOException {
        roundTrips.incrementAndGet();
        try {
            RespClient client = master;
            if (client == null) {
                client = new RespClient(host, port, timeoutMillis);
                master = client;
            }
            Object reply = client.call("INFO", "replication");
            if (reply instanceof String info) {
                return parse(info);
            }
            throw new IOException("unexpected INFO reply from master: " + reply);
        } catch (IOException e) {
            closeConnection();
            throw e;
        }
    }

    private Position parse(String info) throws IOException {
        String role = null;
        String replId = null;
        long offset = -1;
        for (String line : info.split("\r\n")) {
            if (line.startsWith("role:")) {
                role = line.substring(5);
            } else if (line.startsWith("master_replid:")) {
                replId = line.substring(14);
            } else if (line.startsWith("master_repl_offset:")) {
                offset = Long.parseLong(line.substring(19));
            }
        }
        if (!"master".equals(role) || replId == null || offset < 0) {
            throw new IOException(host + ":" + port + " isn't answering like a master (role " + role + ")");
        }
        return new Position(replId, offset);
    }

    private void closeConnection() {
        RespClient client = master;
        master = null;
        if (client != null) {
            try {
                client.close();
            } catch (IOException ignored) {
                // Nothing useful to do; the next round reconnects.
            }
        }
    }
}
