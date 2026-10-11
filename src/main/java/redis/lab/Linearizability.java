package redis.lab;

import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Decides whether a recorded history of GET/SET/INCR operations is
 * linearizable: whether there is one order of all the operations that
 * respects real time (an operation that finished before another started
 * comes first) and in which every result is what a single copy of the data
 * would have returned.
 *
 * Linearizability is compositional (Herlihy and Wing), so each key is checked
 * on its own, which keeps the search small. Per key this is the Wing and Gong
 * search with Lowe's memoization, the same algorithm as Knossos and Porcupine.
 *
 * Before searching, a cheap pass looks for the plain anomalies (a read older
 * than a write that already finished, two INCRs returning the same number)
 * because those come with an explanation a person can read.
 *
 * A passing result means this history is linearizable. It can't prove the
 * system always behaves that way.
 */
public final class Linearizability {

    public enum Verdict { LINEARIZABLE, NOT_LINEARIZABLE, UNKNOWN }

    /** @param anomalies every violation the quick pass found, each explained; empty if it found none */
    public record Result(Verdict verdict, String explanation, List<String> anomalies) {
    }

    private Linearizability() {
    }

    public static Result check(List<Op> history, Duration budget) {
        long origin = history.stream().mapToLong(Op::call).min().orElse(0);
        Map<String, List<Op>> byKey = new LinkedHashMap<>();
        for (Op op : history) {
            byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
        }

        List<String> anomalies = new ArrayList<>();
        for (List<Op> ops : byKey.values()) {
            findAnomalies(ops, origin, anomalies);
        }
        if (!anomalies.isEmpty()) {
            return new Result(Verdict.NOT_LINEARIZABLE, anomalies.get(0), anomalies);
        }

        long deadline = System.nanoTime() + budget.toNanos();
        boolean gaveUp = false;
        for (Map.Entry<String, List<Op>> key : byKey.entrySet()) {
            Search search = new Search(key.getValue(), deadline);
            Verdict verdict = search.run();
            if (verdict == Verdict.NOT_LINEARIZABLE) {
                return new Result(verdict, "key " + key.getKey() + ": no valid order exists; the search got stuck at "
                        + describe(search.blockedAt, origin), anomalies);
            }
            gaveUp |= verdict == Verdict.UNKNOWN;
        }
        return gaveUp
                ? new Result(Verdict.UNKNOWN, "ran out of time before finishing the search", anomalies)
                : new Result(Verdict.LINEARIZABLE, "every key has a valid order", anomalies);
    }

    // ---- quick pass: anomalies that prove a violation on their own ----

    /**
     * These checks are only sound for the two shapes of key the lab uses:
     * registers (SET, never reusing a value) and counters (INCR, never SET).
     * Any other key is left to the full search.
     */
    private static void findAnomalies(List<Op> ops, long origin, List<String> out) {
        boolean hasIncr = ops.stream().anyMatch(o -> o.kind() == Op.Kind.INCR);
        List<String> setValues = ops.stream().filter(o -> o.kind() == Op.Kind.SET).map(Op::value).toList();
        if (hasIncr && setValues.isEmpty()) {
            counterAnomalies(ops, origin, out);
        } else if (!hasIncr && new HashSet<>(setValues).size() == setValues.size()) {
            registerAnomalies(ops, origin, out);
        }
    }

    private static void registerAnomalies(List<Op> ops, long origin, List<String> out) {
        Map<String, Op> writerOf = new HashMap<>();
        List<Op> writes = new ArrayList<>();
        for (Op op : ops) {
            if (op.kind() == Op.Kind.SET) {
                writerOf.put(op.value(), op);
                writes.add(op);
            }
        }
        for (Op read : ops) {
            if (!read.isRead()) {
                continue;
            }
            if (read.result() == null) {
                // Nothing deletes register keys, so once a SET has finished the key exists for good.
                for (Op w : writes) {
                    if (w.ret() < read.call()) {
                        out.add(describe(read, origin) + " missed " + describe(w, origin)
                                + ", which had already finished");
                        break;
                    }
                }
                continue;
            }
            Op source = writerOf.get((String) read.result());
            if (source == null) {
                out.add(describe(read, origin) + " returned a value nobody wrote");
                continue;
            }
            if (source.call() > read.ret()) {
                out.add(describe(read, origin) + " returned a value from the future: " + describe(source, origin));
                continue;
            }
            for (Op newer : writes) {
                if (newer != source && source.ret() < newer.call() && newer.ret() < read.call()) {
                    out.add(describe(read, origin) + " is stale: " + describe(newer, origin)
                            + " had replaced that value before the read started");
                    break;
                }
            }
        }
    }

    private static void counterAnomalies(List<Op> ops, long origin, List<String> out) {
        Map<Long, Op> seen = new HashMap<>();
        for (Op op : ops) {
            if (op.kind() == Op.Kind.INCR) {
                Op twin = seen.putIfAbsent((Long) op.result(), op);
                if (twin != null) {
                    out.add("lost update: " + describe(twin, origin) + " and " + describe(op, origin)
                            + " both returned " + op.result());
                }
            }
        }
        for (Op read : ops) {
            if (!read.isRead()) {
                continue;
            }
            long value;
            try {
                value = read.result() == null ? 0 : Long.parseLong((String) read.result());
            } catch (NumberFormatException e) {
                out.add(describe(read, origin) + " returned something no INCR could have produced");
                continue;
            }
            for (Op incr : ops) {
                if (incr.kind() == Op.Kind.INCR && incr.ret() < read.call() && (Long) incr.result() > value) {
                    out.add(describe(read, origin) + " is stale: " + describe(incr, origin)
                            + " had already finished");
                    break;
                }
            }
        }
    }

    static String describe(Op op, long origin) {
        if (op == null) {
            return "(nothing)";
        }
        return op + " [" + ms(op.call() - origin) + " to " + ms(op.ret() - origin) + " ms, client " + op.client() + "]";
    }

    private static String ms(long nanos) {
        return String.format("%.3f", nanos / 1_000_000.0);
    }

    // ---- full search: Wing and Gong with Lowe's memoization ----

    private static final class Search {

        private static final class Entry {
            final Op op;
            final int id;
            final boolean isCall;
            final long time;
            Entry match;
            Entry prev;
            Entry next;

            Entry(Op op, int id, boolean isCall, long time) {
                this.op = op;
                this.id = id;
                this.isCall = isCall;
                this.time = time;
            }
        }

        private record Frame(Entry entry, String stateBefore) {
        }

        private record Seen(BitSet linearized, String state) {
        }

        private final Entry head = new Entry(null, -1, false, Long.MIN_VALUE);
        private final long deadline;
        Op blockedAt;

        Search(List<Op> ops, long deadline) {
            this.deadline = deadline;
            List<Entry> events = new ArrayList<>(ops.size() * 2);
            for (int i = 0; i < ops.size(); i++) {
                Op op = ops.get(i);
                Entry call = new Entry(op, i, true, op.call());
                Entry ret = new Entry(op, i, false, op.ret());
                call.match = ret;
                ret.match = call;
                events.add(call);
                events.add(ret);
            }
            // On a tie, put calls first: that treats the two operations as
            // overlapping, the more forgiving reading of the timestamps.
            events.sort(Comparator.comparingLong((Entry e) -> e.time).thenComparing(e -> e.isCall ? 0 : 1));
            Entry prev = head;
            for (Entry e : events) {
                prev.next = e;
                e.prev = prev;
                prev = e;
            }
        }

        /**
         * Walks the events in time order trying to place each call. Placing an
         * operation means applying it to the model now; it's only allowed if
         * the model agrees with the result the client saw. Reaching a return
         * whose call hasn't been placed means an earlier choice was wrong, so
         * undo the most recent one and try the next candidate.
         */
        Verdict run() {
            BitSet linearized = new BitSet();
            Set<Seen> seen = new HashSet<>();
            List<Frame> stack = new ArrayList<>();
            String state = null;
            Entry entry = head.next;
            long steps = 0;

            while (head.next != null) {
                if ((++steps & 0xFFFF) == 0 && System.nanoTime() > deadline) {
                    return Verdict.UNKNOWN;
                }
                if (entry.isCall) {
                    String next = apply(state, entry.op);
                    if (next != ILLEGAL) {
                        BitSet after = (BitSet) linearized.clone();
                        after.set(entry.id);
                        // Same set of operations done and same resulting state as a
                        // configuration already explored: nothing new can come of it.
                        if (seen.add(new Seen(after, next))) {
                            stack.add(new Frame(entry, state));
                            state = next;
                            linearized.set(entry.id);
                            lift(entry);
                            entry = head.next;
                            continue;
                        }
                    }
                    entry = entry.next;
                } else {
                    if (stack.isEmpty()) {
                        blockedAt = entry.op;
                        return Verdict.NOT_LINEARIZABLE;
                    }
                    Frame top = stack.remove(stack.size() - 1);
                    state = top.stateBefore();
                    linearized.clear(top.entry().id);
                    unlift(top.entry());
                    entry = top.entry().next;
                }
            }
            return Verdict.LINEARIZABLE;
        }

        private static void lift(Entry call) {
            unlink(call);
            unlink(call.match);
        }

        private static void unlink(Entry e) {
            e.prev.next = e.next;
            if (e.next != null) {
                e.next.prev = e.prev;
            }
        }

        // Reverse order of lift; the removed entries still remember their neighbours.
        private static void unlift(Entry call) {
            relink(call.match);
            relink(call);
        }

        private static void relink(Entry e) {
            e.prev.next = e;
            if (e.next != null) {
                e.next.prev = e;
            }
        }
    }

    // ---- the sequential model: one Redis string key ----

    private static final String ILLEGAL = new String("illegal");

    /** The key's value after {@code op}, or ILLEGAL if a single copy couldn't have produced op's result. */
    private static String apply(String state, Op op) {
        return switch (op.kind()) {
            case GET -> Objects.equals(state, op.result()) ? state : ILLEGAL;
            case SET -> op.value();
            case INCR -> {
                long current;
                try {
                    current = state == null ? 0 : Long.parseLong(state);
                } catch (NumberFormatException e) {
                    yield ILLEGAL;
                }
                yield op.result() instanceof Long n && n == current + 1 ? Long.toString(n) : ILLEGAL;
            }
        };
    }
}
