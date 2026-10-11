package redis.lab;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.junit.jupiter.api.Test;

import redis.lab.Linearizability.Verdict;

/**
 * Differential test: the checker against a brute force that tries every
 * order allowed by real time, on thousands of small random histories. The
 * model here is written separately on purpose, so a bug in the checker's
 * model can't hide behind the same bug in the reference.
 */
class LinearizabilityBruteForceTest {

    private static final String[] VALUES = {"a", "b", "1"};
    private static final String[] READS = {null, "a", "b", "1", "2"};

    private enum Shape { MIXED, REGISTER, COUNTER }

    @Test
    void agreesWithBruteForceOnRandomHistories() {
        compare(Shape.MIXED, new Random(2026));
    }

    /** The shapes the quick pass takes shortcuts on, so its shortcuts get checked too. */
    @Test
    void agreesOnRegistersWithUniqueValues() {
        compare(Shape.REGISTER, new Random(11));
    }

    @Test
    void agreesOnCounters() {
        compare(Shape.COUNTER, new Random(12));
    }

    private static void compare(Shape shape, Random random) {
        int linearizable = 0;
        int notLinearizable = 0;
        for (int i = 0; i < 4000; i++) {
            List<Op> history = randomHistory(random, 2 + random.nextInt(6), shape);
            boolean expected = bruteForce(history);
            Verdict actual = Linearizability.check(history, Duration.ofSeconds(10)).verdict();
            assertEquals(expected ? Verdict.LINEARIZABLE : Verdict.NOT_LINEARIZABLE, actual, history.toString());
            if (expected) {
                linearizable++;
            } else {
                notLinearizable++;
            }
        }
        // Make sure the generator exercised both outcomes properly.
        assertTrue(linearizable > 300 && notLinearizable > 300, shape + ": " + linearizable + " vs " + notLinearizable);
    }

    private static List<Op> randomHistory(Random random, int size, Shape shape) {
        if (shape == Shape.COUNTER) {
            return randomCounterHistory(random, size);
        }
        List<Op> ops = new ArrayList<>();
        int written = 0;
        for (int i = 0; i < size; i++) {
            long call = random.nextInt(12);
            long ret = call + 1 + random.nextInt(6);
            int client = i;
            ops.add(switch (shape) {
                case MIXED -> switch (random.nextInt(3)) {
                    case 0 -> new Op(client, Op.Kind.SET, "k", VALUES[random.nextInt(VALUES.length)], "OK", call, ret);
                    case 1 -> new Op(client, Op.Kind.GET, "k", null, READS[random.nextInt(READS.length)], call, ret);
                    default -> new Op(client, Op.Kind.INCR, "k", null, (long) (1 + random.nextInt(3)), call, ret);
                };
                case REGISTER -> random.nextBoolean()
                        ? new Op(client, Op.Kind.SET, "k", "v" + written++, "OK", call, ret)
                        : new Op(client, Op.Kind.GET, "k", null,
                                random.nextInt(4) == 0 ? null : "v" + random.nextInt(Math.max(1, written + 1)), call, ret);
                case COUNTER -> throw new IllegalStateException();
            });
        }
        return ops;
    }

    /**
     * INCR results are a shuffle of 1..n, as a correct server would hand out,
     * except that sometimes one is duplicated (a lost update). The timing is
     * random, so plenty of histories still come out invalid.
     */
    private static List<Op> randomCounterHistory(Random random, int size) {
        boolean[] isIncr = new boolean[size];
        List<Long> results = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            isIncr[i] = random.nextBoolean();
            if (isIncr[i]) {
                results.add((long) results.size() + 1);
            }
        }
        java.util.Collections.shuffle(results, random);
        if (results.size() > 1 && random.nextInt(4) == 0) {
            results.set(0, results.get(1));
        }
        List<Op> ops = new ArrayList<>();
        int next = 0;
        for (int i = 0; i < size; i++) {
            long call = random.nextInt(12);
            long ret = call + 1 + random.nextInt(6);
            if (isIncr[i]) {
                ops.add(new Op(i, Op.Kind.INCR, "k", null, results.get(next++), call, ret));
            } else {
                int seen = random.nextInt(results.size() + 1);
                ops.add(new Op(i, Op.Kind.GET, "k", null, seen == 0 ? null : Integer.toString(seen), call, ret));
            }
        }
        return ops;
    }

    private static boolean bruteForce(List<Op> ops) {
        return search(ops, new boolean[ops.size()], 0, null);
    }

    private static boolean search(List<Op> ops, boolean[] done, int count, String state) {
        if (count == ops.size()) {
            return true;
        }
        for (int i = 0; i < ops.size(); i++) {
            if (done[i] || !mayGoNext(ops, done, i)) {
                continue;
            }
            Op op = ops.get(i);
            String next;
            switch (op.kind()) {
                case GET -> {
                    if (!Objects.equals(state, op.result())) {
                        continue;
                    }
                    next = state;
                }
                case SET -> next = op.value();
                default -> {
                    if (state != null && !state.chars().allMatch(Character::isDigit)) {
                        continue;
                    }
                    long n = (state == null ? 0 : Long.parseLong(state)) + 1;
                    if (!op.result().equals(n)) {
                        continue;
                    }
                    next = Long.toString(n);
                }
            }
            done[i] = true;
            if (search(ops, done, count + 1, next)) {
                return true;
            }
            done[i] = false;
        }
        return false;
    }

    /** An operation can go next only if nothing still waiting finished before it began. */
    private static boolean mayGoNext(List<Op> ops, boolean[] done, int candidate) {
        for (int j = 0; j < ops.size(); j++) {
            if (!done[j] && j != candidate && ops.get(j).ret() < ops.get(candidate).call()) {
                return false;
            }
        }
        return true;
    }
}
