package redis.store;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * The database: string keys to byte-array values, with optional expiry.
 *
 * Not thread-safe. All access happens under the server's command lock.
 *
 * Expired keys are removed in two ways, like Redis: lazily when someone
 * touches them, and by {@link #expireCycle} which the server runs every
 * 100 ms so keys nobody reads again don't sit in memory forever. Deadlines
 * live in a min-heap; entries are never removed from the heap when a key is
 * overwritten, they're just skipped when they come up and no longer match.
 *
 * Every key also has a position: a keyed hash (SipHash with a per-process
 * secret). Keys are indexed by position, which gives SCAN a cursor that
 * stays valid while keys come and go, and gives eviction cheap random
 * samples.
 */
public final class Keyspace {

    public static final long NO_EXPIRY = -1;

    /**
     * Rough JVM cost of one key beyond its bytes: the String and its array
     * header, the entry object, the hash map node and the position index
     * node. An estimate, but a consistent one, which is what maxmemory needs.
     */
    static final int KEY_OVERHEAD = 180;
    /** Extra for keys with a TTL: the deadline heap and the volatile index. */
    static final int VOLATILE_OVERHEAD = 90;

    /**
     * A stored value. The value and deadline never change after creation,
     * which is what lets snapshots copy references instead of data. Only the
     * access time (for LRU eviction) is updated in place.
     */
    public static final class Entry {
        private final byte[] value;
        private final long expireAt;
        private long lastAccess;

        Entry(byte[] value, long expireAt, long now) {
            this.value = value;
            this.expireAt = expireAt;
            this.lastAccess = now;
        }

        public byte[] value() {
            return value;
        }

        /** Unix time in ms, or NO_EXPIRY. */
        public long expireAt() {
            return expireAt;
        }

        boolean expiredAt(long now) {
            return expireAt != NO_EXPIRY && expireAt <= now;
        }
    }

    private record Deadline(long at, String key) {
    }

    /** A page of SCAN results. A next cursor of 0 means the scan is complete. */
    public record ScanPage(long nextCursor, List<String> keys) {
    }

    /**
     * A point-in-time copy of the live data, for writing snapshots without
     * holding the command lock. Values and deadlines are immutable, so
     * copying references is enough for a consistent copy.
     */
    public record Snapshot(List<String> keys, List<Entry> entries) {
        public int size() {
            return keys.size();
        }

        public int volatileKeys() {
            int n = 0;
            for (Entry entry : entries) {
                if (entry.expireAt() != NO_EXPIRY) {
                    n++;
                }
            }
            return n;
        }
    }

    /**
     * The keys one connection has WATCHed. Any write, delete, expiry or
     * eviction of one of them marks it, and EXEC then refuses to run.
     */
    public static final class Watch {
        /** Key -> whether it existed when watched; only those can expire afterwards. */
        private final Map<String, Boolean> keys = new HashMap<>();
        private boolean touched;

        public boolean isEmpty() {
            return keys.isEmpty();
        }
    }

    private final Map<String, Entry> entries = new HashMap<>();
    private final PriorityQueue<Deadline> deadlines = new PriorityQueue<>(Comparator.comparingLong(Deadline::at));
    /** Position -> key, or -> List of keys on the (astronomically rare) collision. */
    private final TreeMap<Long, Object> positions = new TreeMap<>();
    /** The same, for keys with a TTL only, so volatile-* eviction can sample them. */
    private final TreeMap<Long, Object> volatilePositions = new TreeMap<>();
    /** Watched key -> the watches to mark when it changes. Empty unless someone uses WATCH. */
    private final Map<String, List<Watch>> watchers = new HashMap<>();
    private final SipHash hasher;
    private final LongSupplier clock;
    private int volatileKeys;
    private long usedMemory;

    private boolean ownsExpiry = true;
    private Consumer<String> onExpire = key -> { };

    public Keyspace(LongSupplier clock) {
        this.clock = clock;
        SecureRandom random = new SecureRandom();
        this.hasher = new SipHash(random.nextLong(), random.nextLong());
    }

    public Keyspace() {
        this(System::currentTimeMillis);
    }

    /**
     * A replica doesn't delete expired keys itself; it hides them and waits
     * for the master's DEL, so the two never disagree about what exists.
     */
    public void setOwnsExpiry(boolean ownsExpiry) {
        this.ownsExpiry = ownsExpiry;
    }

    /** Called with each key this keyspace expires, so a master can tell its replicas. */
    public void onExpire(Consumer<String> listener) {
        this.onExpire = listener;
    }

    public long now() {
        return clock.getAsLong();
    }

    public Entry lookup(String key) {
        Entry entry = entries.get(key);
        if (entry == null) {
            return null;
        }
        long now = now();
        if (!entry.expiredAt(now)) {
            // Writing only when the millisecond changed keeps hot keys read-only
            // in the common case, so their cache line isn't bounced between cores.
            if (entry.lastAccess != now) {
                entry.lastAccess = now;
            }
            return entry;
        }
        if (ownsExpiry) {
            remove(key);
            onExpire.accept(key);
        }
        return null;
    }

    public byte[] get(String key) {
        Entry entry = lookup(key);
        return entry == null ? null : entry.value();
    }


    public boolean exists(String key) {
        return lookup(key) != null;
    }

    public void set(String key, byte[] value, long expireAt) {
        put(key, new Entry(value, expireAt, now()));
    }

    /**
     * Returns whether a live key was deleted. An expired key still gets
     * physically removed: on a replica, the master's DEL is the only thing
     * that ever clears it out.
     */
    public boolean delete(String key) {
        boolean live = lookup(key) != null;
        remove(key);
        return live;
    }

    /** -2 if the key doesn't exist, -1 if it has no expiry, otherwise the unix ms deadline. */
    public long expireAt(String key) {
        Entry entry = lookup(key);
        if (entry == null) {
            return -2;
        }
        return entry.expireAt();
    }

    /** Sets a deadline on an existing key. A deadline in the past deletes it straight away, as in Redis. */
    public boolean setExpireAt(String key, long at) {
        Entry entry = lookup(key);
        if (entry == null) {
            return false;
        }
        if (at <= now()) {
            remove(key);
        } else {
            put(key, new Entry(entry.value(), at, now()));
        }
        return true;
    }

    public boolean persist(String key) {
        Entry entry = lookup(key);
        if (entry == null || entry.expireAt() == NO_EXPIRY) {
            return false;
        }
        put(key, new Entry(entry.value(), NO_EXPIRY, now()));
        return true;
    }

    public List<String> keys(Predicate<String> filter) {
        long now = now();
        List<String> out = new ArrayList<>();
        entries.forEach((key, entry) -> {
            if (!entry.expiredAt(now) && filter.test(key)) {
                out.add(key);
            }
        });
        return out;
    }

    /**
     * One step of SCAN: looks at about {@code count} keys from position
     * {@code cursor} on, in position order. A key that exists for the whole
     * scan is returned exactly once, however many keys are added or removed
     * meanwhile, which is the guarantee Redis gives (minus its duplicates).
     */
    public ScanPage scan(long cursor, int count, Predicate<String> filter) {
        long now = now();
        List<String> out = new ArrayList<>();
        int examined = 0;
        Long last = null;
        for (Map.Entry<Long, Object> bucket : positions.tailMap(cursor, true).entrySet()) {
            if (examined >= count) {
                break;
            }
            // A whole bucket at a time, so the next cursor never splits one.
            for (String key : keysAt(bucket.getValue())) {
                examined++;
                Entry entry = entries.get(key);
                if (entry != null && !entry.expiredAt(now) && filter.test(key)) {
                    out.add(key);
                }
            }
            last = bucket.getKey();
        }
        if (last == null) {
            return new ScanPage(0, out);
        }
        Long next = positions.higherKey(last);
        return new ScanPage(next == null ? 0 : next, out);
    }

    /** A random key, or null if there are none. Close to uniform, since positions are hashes. */
    public String randomKey(Random random) {
        return randomFrom(positions, random);
    }

    /** Snapshot copy of the live entries; O(keys), and only references are copied. Caller holds the lock. */
    public Snapshot snapshot() {
        long now = now();
        List<String> keys = new ArrayList<>(entries.size());
        List<Entry> values = new ArrayList<>(entries.size());
        entries.forEach((key, entry) -> {
            if (!entry.expiredAt(now)) {
                keys.add(key);
                values.add(entry);
            }
        });
        return new Snapshot(keys, values);
    }

    /** Includes keys that have expired but haven't been collected yet, same as Redis' DBSIZE. */
    public int size() {
        return entries.size();
    }

    public int volatileKeys() {
        return volatileKeys;
    }

    /** Estimated bytes used by keys and values, as maxmemory sees it. */
    public long usedMemory() {
        return usedMemory;
    }

    public void clear() {
        watchers.forEach((key, watches) -> {
            if (entries.containsKey(key)) {
                watches.forEach(watch -> watch.touched = true);
            }
        });
        entries.clear();
        deadlines.clear();
        positions.clear();
        volatilePositions.clear();
        volatileKeys = 0;
        usedMemory = 0;
    }

    /** WATCH: from now on, any change to {@code key} marks {@code watch}. */
    public void watch(Watch watch, String key) {
        if (watch.keys.containsKey(key)) {
            return;
        }
        // lookup() drops a key that has already expired, so that dropping it
        // later doesn't count as a change. Redis behaves the same way.
        watch.keys.put(key, lookup(key) != null);
        watchers.computeIfAbsent(key, k -> new ArrayList<>(1)).add(watch);
    }

    /** UNWATCH, and what EXEC and DISCARD do once they're done. */
    public void unwatch(Watch watch) {
        for (String key : watch.keys.keySet()) {
            List<Watch> watches = watchers.get(key);
            watches.remove(watch);
            if (watches.isEmpty()) {
                watchers.remove(key);
            }
        }
        watch.keys.clear();
        watch.touched = false;
    }

    /** How many distinct keys are being watched, for INFO. */
    public int watchedKeys() {
        return watchers.size();
    }

    /** Whether any watched key was modified, deleted or expired since it was watched. */
    public boolean changedSince(Watch watch) {
        if (watch.touched) {
            return true;
        }
        // A key can have expired without anything having removed it yet.
        long now = now();
        for (Map.Entry<String, Boolean> watched : watch.keys.entrySet()) {
            Entry entry = entries.get(watched.getKey());
            if (watched.getValue() && entry != null && entry.expiredAt(now)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Removes keys whose deadline has passed, at most {@code budget} heap
     * entries per call so one cycle can't stall the server. Returns how many
     * keys were actually deleted.
     */
    public int expireCycle(int budget) {
        if (!ownsExpiry) {
            return 0;
        }
        long now = now();
        int removed = 0;
        while (budget-- > 0 && !deadlines.isEmpty() && deadlines.peek().at() <= now) {
            Deadline d = deadlines.poll();
            Entry entry = entries.get(d.key());
            if (entry != null && entry.expireAt() == d.at()) {
                remove(d.key());
                onExpire.accept(d.key());
                removed++;
            }
        }
        return removed;
    }

    /**
     * Evicts keys until the estimate is back under {@code maxMemory}. Returns
     * false if that wasn't possible (noeviction, or no candidates left), in
     * which case the caller should refuse the write.
     */
    public boolean evictUntilUnder(long maxMemory, Eviction.Policy policy, int samples, Random random,
                                   Consumer<String> onEvict) {
        while (usedMemory > maxMemory) {
            if (policy == Eviction.Policy.NOEVICTION) {
                return false;
            }
            String victim = pickVictim(policy, samples, random);
            if (victim == null) {
                return false;
            }
            remove(victim);
            onEvict.accept(victim);
        }
        return true;
    }

    private String pickVictim(Eviction.Policy policy, int samples, Random random) {
        TreeMap<Long, Object> pool = policy.volatileOnly() ? volatilePositions : positions;
        if (pool.isEmpty()) {
            return null;
        }
        if (policy == Eviction.Policy.ALLKEYS_RANDOM || policy == Eviction.Policy.VOLATILE_RANDOM) {
            return randomFrom(pool, random);
        }
        // Approximated LRU/TTL, as in Redis: look at a few random keys and take the best of them.
        long now = now();
        String best = null;
        long bestScore = Long.MAX_VALUE;
        for (int i = 0; i < samples; i++) {
            String key = randomFrom(pool, random);
            Entry entry = entries.get(key);
            if (entry == null) {
                continue;
            }
            if (entry.expiredAt(now)) {
                return key;
            }
            long score = policy == Eviction.Policy.VOLATILE_TTL ? entry.expireAt() : entry.lastAccess;
            if (score < bestScore) {
                bestScore = score;
                best = key;
            }
        }
        return best;
    }

    private static String randomFrom(TreeMap<Long, Object> index, Random random) {
        if (index.isEmpty()) {
            return null;
        }
        Map.Entry<Long, Object> bucket = index.ceilingEntry(random.nextLong() >>> 1);
        if (bucket == null) {
            bucket = index.firstEntry();
        }
        List<String> keys = keysAt(bucket.getValue());
        return keys.get(random.nextInt(keys.size()));
    }

    private void put(String key, Entry entry) {
        touch(key);
        Entry old = entries.put(key, entry);
        boolean wasVolatile = old != null && old.expireAt() != NO_EXPIRY;
        boolean isVolatile = entry.expireAt() != NO_EXPIRY;
        // Hashing only when an index actually changes keeps the common
        // overwrite (SET on an existing key without a TTL) as cheap as before.
        long position = old == null || wasVolatile || isVolatile ? position(key) : 0;
        if (old == null) {
            index(positions, position, key);
            usedMemory += KEY_OVERHEAD + key.length();
        } else {
            usedMemory -= old.value.length;
            if (wasVolatile) {
                volatileKeys--;
                usedMemory -= VOLATILE_OVERHEAD;
                unindex(volatilePositions, position, key);
            }
        }
        usedMemory += entry.value.length;
        if (isVolatile) {
            volatileKeys++;
            usedMemory += VOLATILE_OVERHEAD;
            index(volatilePositions, position, key);
            deadlines.add(new Deadline(entry.expireAt(), key));
            compactDeadlinesIfBloated();
        }
    }

    private boolean remove(String key) {
        Entry old = entries.remove(key);
        if (old == null) {
            return false;
        }
        touch(key);
        long position = position(key);
        unindex(positions, position, key);
        usedMemory -= KEY_OVERHEAD + key.length() + old.value.length;
        if (old.expireAt() != NO_EXPIRY) {
            volatileKeys--;
            usedMemory -= VOLATILE_OVERHEAD;
            unindex(volatilePositions, position, key);
        }
        return true;
    }

    private void touch(String key) {
        if (!watchers.isEmpty()) {
            List<Watch> watches = watchers.get(key);
            if (watches != null) {
                watches.forEach(watch -> watch.touched = true);
            }
        }
    }

    private long position(String key) {
        return hasher.hash(key.getBytes(StandardCharsets.ISO_8859_1)) >>> 1;
    }

    @SuppressWarnings("unchecked")
    private static List<String> keysAt(Object bucket) {
        return bucket instanceof String key ? List.of(key) : (List<String>) bucket;
    }

    @SuppressWarnings("unchecked")
    private static void index(TreeMap<Long, Object> index, long position, String key) {
        Object current = index.get(position);
        if (current == null) {
            index.put(position, key);
        } else if (current instanceof String other) {
            List<String> both = new ArrayList<>(2);
            both.add(other);
            both.add(key);
            index.put(position, both);
        } else {
            ((List<String>) current).add(key);
        }
    }

    @SuppressWarnings("unchecked")
    private static void unindex(TreeMap<Long, Object> index, long position, String key) {
        Object current = index.get(position);
        if (current instanceof String) {
            index.remove(position);
        } else if (current != null) {
            List<String> keys = (List<String>) current;
            keys.remove(key);
            if (keys.size() == 1) {
                index.put(position, keys.get(0));
            }
        }
    }

    // Refreshing the TTL of the same key over and over leaves dead heap
    // entries behind. Rebuild once they clearly outnumber the live ones.
    private void compactDeadlinesIfBloated() {
        if (deadlines.size() < 1024 || deadlines.size() < 4 * volatileKeys) {
            return;
        }
        deadlines.clear();
        entries.forEach((key, entry) -> {
            if (entry.expireAt() != NO_EXPIRY) {
                deadlines.add(new Deadline(entry.expireAt(), key));
            }
        });
    }
}
