package redis.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class KeyspaceTest {

    private final AtomicLong clock = new AtomicLong(1_000_000);
    private final Keyspace db = new Keyspace(clock::get);

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void setAndGet() {
        db.set("k", b("v"), Keyspace.NO_EXPIRY);
        assertArrayEquals(b("v"), db.get("k"));
        assertNull(db.get("missing"));
    }

    @Test
    void keyDisappearsOnceDeadlinePasses() {
        db.set("k", b("v"), clock.get() + 100);
        clock.addAndGet(99);
        assertTrue(db.exists("k"));
        clock.addAndGet(1);
        assertFalse(db.exists("k"));
        assertEquals(0, db.size());
    }

    @Test
    void expireCycleCollectsKeysNobodyReads() {
        List<String> expired = new ArrayList<>();
        db.onExpire(expired::add);
        db.set("a", b("1"), clock.get() + 10);
        db.set("b", b("2"), clock.get() + 20);
        db.set("c", b("3"), Keyspace.NO_EXPIRY);

        clock.addAndGet(15);
        assertEquals(1, db.expireCycle(100));
        assertEquals(List.of("a"), expired);
        assertEquals(2, db.size());

        clock.addAndGet(10);
        db.expireCycle(100);
        assertEquals(List.of("a", "b"), expired);
        assertEquals(1, db.size());
    }

    @Test
    void overwritingAKeyCancelsItsOldDeadline() {
        db.set("k", b("old"), clock.get() + 10);
        db.set("k", b("new"), Keyspace.NO_EXPIRY);
        clock.addAndGet(50);
        assertEquals(0, db.expireCycle(100));
        assertArrayEquals(b("new"), db.get("k"));
    }

    @Test
    void persistRemovesDeadline() {
        db.set("k", b("v"), clock.get() + 10);
        assertTrue(db.persist("k"));
        assertEquals(-1, db.expireAt("k"));
        assertEquals(0, db.volatileKeys());
        clock.addAndGet(50);
        assertTrue(db.exists("k"));
    }

    @Test
    void expireInThePastDeletes() {
        db.set("k", b("v"), Keyspace.NO_EXPIRY);
        assertTrue(db.setExpireAt("k", clock.get() - 1));
        assertFalse(db.exists("k"));
        assertFalse(db.setExpireAt("missing", clock.get() + 10));
    }

    @Test
    void replicaHidesExpiredKeysButLeavesDeletionToMaster() {
        db.setOwnsExpiry(false);
        db.set("k", b("v"), clock.get() + 10);
        clock.addAndGet(20);
        assertNull(db.get("k"));
        assertEquals(0, db.expireCycle(100));
        assertEquals(1, db.size());
    }

    @Test
    void staleDeadlinesDoNotPileUp() {
        for (int i = 0; i < 100_000; i++) {
            db.set("k", b("v"), clock.get() + 1_000_000 + i);
        }
        assertEquals(1, db.volatileKeys());
        clock.addAndGet(2_000_000);
        db.expireCycle(10_000);
        assertEquals(0, db.size());
    }
}
