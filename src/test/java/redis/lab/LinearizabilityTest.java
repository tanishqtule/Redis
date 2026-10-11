package redis.lab;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import redis.lab.Linearizability.Result;
import redis.lab.Linearizability.Verdict;

/**
 * Hand-built histories with known answers. Times are plain numbers; only
 * their order matters.
 */
class LinearizabilityTest {

    private static Op set(int client, String value, long call, long ret) {
        return new Op(client, Op.Kind.SET, "k", value, "OK", call, ret);
    }

    private static Op get(int client, String result, long call, long ret) {
        return new Op(client, Op.Kind.GET, "k", null, result, call, ret);
    }

    private static Op incr(int client, long result, long call, long ret) {
        return new Op(client, Op.Kind.INCR, "n", null, result, call, ret);
    }

    private static Op getN(int client, String result, long call, long ret) {
        return new Op(client, Op.Kind.GET, "n", null, result, call, ret);
    }

    private static Verdict verdict(Op... ops) {
        return Linearizability.check(List.of(ops), Duration.ofSeconds(10)).verdict();
    }

    @Test
    void sequentialHistoryIsLinearizable() {
        assertEquals(Verdict.LINEARIZABLE, verdict(
                set(1, "a", 0, 1),
                get(1, "a", 2, 3),
                set(1, "b", 4, 5),
                get(2, "b", 6, 7)));
    }

    @Test
    void readThatMissesAFinishedWriteIsNot() {
        Result result = Linearizability.check(List.of(
                set(1, "a", 0, 1),
                set(1, "b", 2, 3),
                get(2, "a", 4, 5)), Duration.ofSeconds(10));
        assertEquals(Verdict.NOT_LINEARIZABLE, result.verdict());
        assertTrue(result.explanation().contains("stale"), result.explanation());
    }

    @Test
    void readOfMissingKeyAfterAWriteIsNot() {
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(
                set(1, "a", 0, 1),
                get(2, null, 2, 3)));
    }

    @Test
    void concurrentReadMayGoEitherWay() {
        // The read overlaps the write, so both the old and the new value are fine.
        assertEquals(Verdict.LINEARIZABLE, verdict(set(1, "a", 0, 10), get(2, null, 2, 3)));
        assertEquals(Verdict.LINEARIZABLE, verdict(set(1, "a", 0, 10), get(2, "a", 2, 3)));
    }

    @Test
    void concurrentWritesCanBeOrderedEitherWay() {
        assertEquals(Verdict.LINEARIZABLE, verdict(
                set(1, "a", 0, 10),
                set(2, "b", 1, 9),
                get(3, "a", 11, 12)));
        assertEquals(Verdict.LINEARIZABLE, verdict(
                set(1, "a", 0, 10),
                set(2, "b", 1, 9),
                get(3, "b", 11, 12)));
    }

    @Test
    void readsMustNotGoBackInTime() {
        // Two writes that overlap, so either order is allowed on its own. But once
        // one reader has seen "b" after "a", a later reader can't see "a" again.
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(
                set(1, "a", 0, 10),
                set(2, "b", 1, 10),
                get(3, "a", 2, 3),
                get(3, "b", 4, 5),
                get(4, "a", 6, 7)));
    }

    @Test
    void readOfAValueNotYetWrittenIsNot() {
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(
                get(1, "a", 0, 1),
                set(2, "a", 2, 3)));
    }

    @Test
    void incrementsMustNotBeLost() {
        assertEquals(Verdict.LINEARIZABLE, verdict(incr(1, 1, 0, 10), incr(2, 2, 1, 9)));
        Result lost = Linearizability.check(List.of(incr(1, 1, 0, 10), incr(2, 1, 1, 9)), Duration.ofSeconds(10));
        assertEquals(Verdict.NOT_LINEARIZABLE, lost.verdict());
        assertTrue(lost.explanation().startsWith("lost update"), lost.explanation());
    }

    @Test
    void counterReadsMustNotGoBackwards() {
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(
                incr(1, 1, 0, 1),
                incr(1, 2, 2, 3),
                getN(2, "1", 4, 5)));
    }

    @Test
    void searchCatchesWhatTheQuickPassCannot() {
        // No single read is provably stale on its own; the contradiction is only
        // visible across all four operations, so this one needs the full search.
        Result result = Linearizability.check(List.of(
                set(1, "a", 0, 10),
                set(2, "b", 0, 10),
                get(3, "a", 1, 2),
                get(4, "b", 3, 4),
                get(5, "a", 5, 6)), Duration.ofSeconds(10));
        assertEquals(Verdict.NOT_LINEARIZABLE, result.verdict());
        assertTrue(result.anomalies().isEmpty(), "quick pass shouldn't have caught this: " + result.anomalies());
        assertTrue(result.explanation().contains("no valid order"), result.explanation());
    }

    @Test
    void keysAreCheckedIndependently() {
        List<Op> history = new ArrayList<>();
        history.add(new Op(1, Op.Kind.SET, "x", "1", "OK", 0, 1));
        history.add(new Op(2, Op.Kind.SET, "y", "2", "OK", 2, 3));
        history.add(new Op(3, Op.Kind.GET, "x", null, "1", 4, 5));
        history.add(new Op(3, Op.Kind.GET, "y", null, "2", 6, 7));
        assertEquals(Verdict.LINEARIZABLE, Linearizability.check(history, Duration.ofSeconds(10)).verdict());
    }

    /**
     * Simulates a correct server (operations take effect at a random point
     * inside their window, one at a time) with lots of overlap, and checks the
     * search accepts it in reasonable time. A correct history must never be
     * rejected, however tangled.
     */
    @Test
    void acceptsLargeConcurrentHistoriesFromACorrectSystem() {
        Random random = new Random(42);
        for (int round = 0; round < 20; round++) {
            List<Op> history = simulateCorrectRegister(random, 8, 60);
            Result result = Linearizability.check(history, Duration.ofSeconds(20));
            assertEquals(Verdict.LINEARIZABLE, result.verdict(), "round " + round + ": " + result.explanation());
        }
    }

    @Test
    void rejectsTheSameHistoriesOnceOneReadIsCorrupted() {
        Random random = new Random(7);
        int rejected = 0;
        for (int round = 0; round < 20; round++) {
            List<Op> history = simulateCorrectRegister(random, 8, 60);
            // Make the last read return the very first value written: stale for sure,
            // provided something else was written and finished in between.
            Op firstWrite = history.stream().filter(o -> o.kind() == Op.Kind.SET)
                    .min((a, b) -> Long.compare(a.ret(), b.ret())).orElseThrow();
            Op lastRead = history.stream().filter(Op::isRead)
                    .max((a, b) -> Long.compare(a.call(), b.call())).orElseThrow();
            boolean overwritten = history.stream().anyMatch(o -> o.kind() == Op.Kind.SET && o != firstWrite
                    && o.call() > firstWrite.ret() && o.ret() < lastRead.call());
            if (!overwritten) {
                continue;
            }
            history.set(history.indexOf(lastRead), new Op(lastRead.client(), lastRead.kind(), lastRead.key(),
                    null, firstWrite.value(), lastRead.call(), lastRead.ret()));
            assertFalse(Linearizability.check(history, Duration.ofSeconds(20)).verdict() == Verdict.LINEARIZABLE,
                    "round " + round);
            rejected++;
        }
        assertTrue(rejected > 10, "only " + rejected + " rounds had a usable corruption");
    }

    /**
     * Each client issues operations back to back. Every operation gets a window
     * [call, ret] and takes effect at a moment strictly inside it; effects are
     * applied in the order of those moments, exactly like a correct server.
     */
    private static List<Op> simulateCorrectRegister(Random random, int clients, int opsPerClient) {
        record Pending(int client, boolean write, String value, long call, long ret, long effect) {
        }
        List<Pending> pending = new ArrayList<>();
        int written = 0;
        for (int c = 0; c < clients; c++) {
            long t = random.nextInt(5);
            for (int i = 0; i < opsPerClient; i++) {
                long call = t;
                long ret = call + 2 + random.nextInt(20);
                long effect = call + 1 + random.nextInt((int) (ret - call - 1));
                boolean write = random.nextInt(3) == 0;
                pending.add(new Pending(c, write, write ? "v" + written++ : null, call, ret, effect));
                t = ret + 1 + random.nextInt(3);
            }
        }
        pending.sort((a, b) -> Long.compare(a.effect(), b.effect()));
        List<Op> history = new ArrayList<>();
        String state = null;
        long lastEffect = Long.MIN_VALUE;
        for (Pending p : pending) {
            // Two effects at the same instant would be ambiguous; nudge to keep them distinct.
            long effect = Math.max(p.effect(), lastEffect + 1);
            lastEffect = effect;
            if (p.write()) {
                state = p.value();
                history.add(new Op(p.client(), Op.Kind.SET, "k", p.value(), "OK", p.call(), Math.max(p.ret(), effect + 1)));
            } else {
                history.add(new Op(p.client(), Op.Kind.GET, "k", null, state, p.call(), Math.max(p.ret(), effect + 1)));
            }
        }
        return history;
    }
}
