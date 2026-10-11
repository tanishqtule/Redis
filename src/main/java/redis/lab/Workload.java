package redis.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import redis.protocol.RespClient;
import redis.protocol.RespError;

/**
 * Runs concurrent clients against a server and records every operation with
 * its timing, for {@link Linearizability} to judge.
 *
 * Each client writes to the master and reads from {@code reads}, which is the
 * master itself or a replica. Half the time a client reads the key it just
 * wrote, which is exactly where a stale replica gets caught.
 *
 * Keys come in two kinds: registers get SET with values that are never
 * reused (so every read can be traced to the write that produced it), and
 * counters get INCR.
 */
public final class Workload {

    public record Endpoint(String host, int port) {
        static Endpoint parse(String hostPort) {
            int colon = hostPort.lastIndexOf(':');
            if (colon < 0) {
                return new Endpoint(hostPort, 6379);
            }
            return new Endpoint(hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)));
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    public record Settings(int clients, int opsPerClient, int registers, int counters, long seed) {
    }

    /** @param failedReads reads that came back as errors (say TRYAGAIN); they changed nothing so they're left out */
    public record Run(List<Op> history, int failedReads, long elapsedNanos) {
    }

    private Workload() {
    }

    /**
     * @param linearizableReads put the read connections in CLIENT CONSISTENCY LINEARIZABLE mode
     * @throws UnsupportedOperationException if the read server doesn't know that mode
     */
    public static Run run(Endpoint master, Endpoint reads, boolean linearizableReads, Settings settings)
            throws IOException, InterruptedException {
        String prefix = "lab:" + Long.toHexString(new Random().nextLong()) + ":";
        ExecutorService pool = Executors.newFixedThreadPool(settings.clients());
        List<Future<ClientResult>> futures = new ArrayList<>();
        long started = System.nanoTime();
        for (int c = 0; c < settings.clients(); c++) {
            int client = c;
            futures.add(pool.submit(() -> runClient(client, master, reads, linearizableReads, settings, prefix)));
        }
        List<Op> history = new ArrayList<>();
        int failedReads = 0;
        try {
            for (Future<ClientResult> future : futures) {
                ClientResult result = future.get();
                history.addAll(result.ops());
                failedReads += result.failedReads();
            }
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnsupportedOperationException u) {
                throw u;
            }
            throw cause instanceof IOException io ? io : new IOException(cause);
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
        long elapsed = System.nanoTime() - started;
        cleanUp(master, prefix, settings);
        return new Run(Collections.unmodifiableList(history), failedReads, elapsed);
    }

    private record ClientResult(List<Op> ops, int failedReads) {
    }

    private static ClientResult runClient(int client, Endpoint master, Endpoint reads, boolean linearizableReads,
                                          Settings settings, String prefix) throws IOException {
        Random random = new Random(settings.seed() * 31 + client);
        List<Op> ops = new ArrayList<>();
        int failedReads = 0;
        int written = 0;
        try (RespClient writer = new RespClient(master.host(), master.port(), 10_000);
             RespClient reader = new RespClient(reads.host(), reads.port(), 10_000)) {
            if (linearizableReads) {
                Object reply = reader.call("CLIENT", "CONSISTENCY", "LINEARIZABLE");
                if (reply instanceof RespError) {
                    throw new UnsupportedOperationException(reads + " doesn't support CLIENT CONSISTENCY");
                }
            }
            int keys = settings.registers() + settings.counters();
            for (int i = 0; i < settings.opsPerClient(); i++) {
                int pick = random.nextInt(keys);
                boolean counter = pick >= settings.registers();
                String key = prefix + (counter ? "ctr" + (pick - settings.registers()) : "reg" + pick);
                if (random.nextBoolean()) {
                    if (counter) {
                        ops.add(timed(writer, client, Op.Kind.INCR, key, null));
                    } else {
                        ops.add(timed(writer, client, Op.Kind.SET, key, "c" + client + "-" + written++));
                    }
                }
                Op read = timed(reader, client, Op.Kind.GET, key, null);
                if (read == null) {
                    failedReads++;
                } else {
                    ops.add(read);
                }
            }
        }
        return new ClientResult(ops, failedReads);
    }

    /** Sends one command and records it. Returns null for a read that failed; a failed write aborts the run. */
    private static Op timed(RespClient conn, int client, Op.Kind kind, String key, String value) throws IOException {
        long call = System.nanoTime();
        Object reply = switch (kind) {
            case GET -> conn.call("GET", key);
            case SET -> conn.call("SET", key, value);
            case INCR -> conn.call("INCR", key);
        };
        long ret = System.nanoTime();
        if (reply instanceof RespError err) {
            if (kind == Op.Kind.GET) {
                return null;
            }
            // We can't know whether a failed write took effect, so the history would be ambiguous.
            throw new IOException(kind + " " + key + " failed: " + err.message());
        }
        return new Op(client, kind, key, value, reply, call, ret);
    }

    private static void cleanUp(Endpoint master, String prefix, Settings settings) {
        try (RespClient c = new RespClient(master.host(), master.port(), 10_000)) {
            for (int i = 0; i < settings.registers(); i++) {
                c.call("DEL", prefix + "reg" + i);
            }
            for (int i = 0; i < settings.counters(); i++) {
                c.call("DEL", prefix + "ctr" + i);
            }
        } catch (IOException ignored) {
            // Leftover lab keys are harmless.
        }
    }
}
