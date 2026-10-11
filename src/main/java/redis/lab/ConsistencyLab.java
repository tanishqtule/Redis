package redis.lab;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import redis.lab.Linearizability.Result;
import redis.lab.Linearizability.Verdict;
import redis.lab.Workload.Endpoint;
import redis.lab.Workload.Run;
import redis.lab.Workload.Settings;
import redis.protocol.RespClient;

/**
 * Measures what consistency a Redis-compatible deployment actually gives.
 *
 * Runs the same concurrent workload a few ways (reads from the master; from
 * a replica as usual; from a replica in linearizable mode), records every
 * operation, and checks each history for linearizability. Works against
 * real Redis too; scenarios a server can't run are reported as such.
 *
 * <pre>
 * java -cp redis-java.jar redis.lab.ConsistencyLab --master localhost:6379 --replica localhost:6380
 * </pre>
 */
public final class ConsistencyLab {

    /** One line of the report. */
    public record Outcome(String scenario, Run run, Result result, String note) {
    }

    private ConsistencyLab() {
    }

    public static void main(String[] args) throws Exception {
        Endpoint master = null;
        Endpoint replica = null;
        int clients = 8;
        int ops = 300;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--master" -> master = Endpoint.parse(args[++i]);
                case "--replica" -> replica = Endpoint.parse(args[++i]);
                case "--clients" -> clients = Integer.parseInt(args[++i]);
                case "--ops" -> ops = Integer.parseInt(args[++i]);
                default -> {
                    System.err.println("usage: ConsistencyLab --master host:port [--replica host:port]"
                            + " [--clients N] [--ops N-per-client]");
                    System.exit(2);
                }
            }
        }
        if (master == null) {
            System.err.println("--master is required");
            System.exit(2);
        }
        List<Outcome> outcomes = runAll(master, replica, new Settings(clients, ops, 3, 1, System.nanoTime()));
        print(outcomes, System.out);
        boolean masterOk = outcomes.get(0).result() != null
                && outcomes.get(0).result().verdict() == Verdict.LINEARIZABLE;
        System.exit(masterOk ? 0 : 1);
    }

    public static List<Outcome> runAll(Endpoint master, Endpoint replica, Settings settings)
            throws IOException, InterruptedException {
        List<Outcome> outcomes = new ArrayList<>();
        outcomes.add(scenario("master only", master, master, false, settings));
        if (replica != null) {
            outcomes.add(scenario("replica, default reads", master, replica, false, settings));
            long before = roundTrips(replica);
            Outcome linearizable = scenario("replica, CLIENT CONSISTENCY LINEARIZABLE", master, replica, true, settings);
            long after = roundTrips(replica);
            if (linearizable.run() != null && before >= 0 && after >= 0) {
                long reads = linearizable.run().history().stream().filter(Op::isRead).count();
                linearizable = new Outcome(linearizable.scenario(), linearizable.run(), linearizable.result(),
                        reads + " reads needed " + (after - before) + " requests to the master");
            }
            outcomes.add(linearizable);
        }
        return outcomes;
    }

    private static Outcome scenario(String name, Endpoint master, Endpoint reads, boolean linearizable,
                                    Settings settings) throws IOException, InterruptedException {
        Run run;
        try {
            run = Workload.run(master, reads, linearizable, settings);
        } catch (UnsupportedOperationException e) {
            return new Outcome(name, null, null, "not supported by this server");
        }
        Result result = Linearizability.check(run.history(), Duration.ofSeconds(60));
        String note = run.failedReads() > 0 ? run.failedReads() + " reads refused (left out of the check)" : "";
        return new Outcome(name, run, result, note);
    }

    /** read_index_round_trips from a redis-java replica's INFO, or -1 for servers that don't report it. */
    private static long roundTrips(Endpoint replica) {
        try (RespClient c = new RespClient(replica.host(), replica.port(), 5_000)) {
            Object info = c.call("INFO", "replication");
            if (info instanceof String text) {
                for (String line : text.split("\r\n")) {
                    if (line.startsWith("read_index_round_trips:")) {
                        return Long.parseLong(line.substring("read_index_round_trips:".length()));
                    }
                }
            }
        } catch (IOException | NumberFormatException e) {
            // Not ours, or not reachable; the report just won't have the number.
        }
        return -1;
    }

    public static void print(List<Outcome> outcomes, PrintStream out) {
        out.printf("%-42s %6s  %-18s %7s  %9s %9s%n",
                "scenario", "ops", "linearizable?", "stale", "read p50", "read p99");
        for (Outcome o : outcomes) {
            if (o.run() == null) {
                out.printf("%-42s %6s  %s%n", o.scenario(), "-", o.note());
                continue;
            }
            List<Op> reads = o.run().history().stream().filter(Op::isRead).toList();
            out.printf("%-42s %6d  %-18s %7d  %9s %9s%n",
                    o.scenario(),
                    o.run().history().size(),
                    label(o.result().verdict()),
                    o.result().anomalies().size(),
                    millis(percentile(reads, 0.50)),
                    millis(percentile(reads, 0.99)));
        }
        for (Outcome o : outcomes) {
            if (o.result() != null && o.result().verdict() != Verdict.LINEARIZABLE) {
                out.println();
                out.println(o.scenario() + ": " + o.result().explanation());
            }
            if (o.run() != null && !o.note().isEmpty()) {
                out.println(o.scenario() + ": " + o.note());
            }
        }
        out.println();
        out.println("\"stale\" counts reads proven older than a write that had already finished.");
        out.println("A pass means this run's history is linearizable, not that every run will be.");
    }

    private static String label(Verdict verdict) {
        return switch (verdict) {
            case LINEARIZABLE -> "yes";
            case NOT_LINEARIZABLE -> "NO";
            case UNKNOWN -> "unknown (timeout)";
        };
    }

    private static long percentile(List<Op> ops, double p) {
        if (ops.isEmpty()) {
            return 0;
        }
        long[] durations = ops.stream().mapToLong(o -> o.ret() - o.call()).sorted().toArray();
        return durations[Math.min(durations.length - 1, (int) Math.ceil(p * durations.length) - 1)];
    }

    private static String millis(long nanos) {
        return String.format("%.2f ms", nanos / 1_000_000.0);
    }
}
