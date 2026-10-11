package redis.store;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.BiConsumer;
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
 */
public final class Keyspace {

    public static final long NO_EXPIRY = -1;

    /** An immutable stored value. {@code expireAt} is unix time in ms, or NO_EXPIRY. */
    public record Entry(byte[] value, long expireAt) {
        boolean expiredAt(long now) {
            return expireAt != NO_EXPIRY && expireAt <= now;
        }
    }

    private record Deadline(long at, String key) {
    }

    private final Map<String, Entry> entries = new HashMap<>();
    private final PriorityQueue<Deadline> deadlines = new PriorityQueue<>(Comparator.comparingLong(Deadline::at));
    private final LongSupplier clock;
    private int volatileKeys;

    private boolean ownsExpiry = true;
    private Consumer<String> onExpire = key -> { };

    public Keyspace(LongSupplier clock) {
        this.clock = clock;
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
        if (entry == null || !entry.expiredAt(now())) {
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
        put(key, new Entry(value, expireAt));
    }

    public boolean delete(String key) {
        return lookup(key) != null && remove(key);
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
            put(key, new Entry(entry.value(), at));
        }
        return true;
    }

    public boolean persist(String key) {
        Entry entry = lookup(key);
        if (entry == null || entry.expireAt() == NO_EXPIRY) {
            return false;
        }
        put(key, new Entry(entry.value(), NO_EXPIRY));
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

    /** Visits every live entry. Used to write snapshots. */
    public void forEachLive(BiConsumer<String, Entry> visitor) {
        long now = now();
        entries.forEach((key, entry) -> {
            if (!entry.expiredAt(now)) {
                visitor.accept(key, entry);
            }
        });
    }

    /** Includes keys that have expired but haven't been collected yet, same as Redis' DBSIZE. */
    public int size() {
        return entries.size();
    }

    public int volatileKeys() {
        return volatileKeys;
    }

    public void clear() {
        entries.clear();
        deadlines.clear();
        volatileKeys = 0;
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

    private void put(String key, Entry entry) {
        Entry old = entries.put(key, entry);
        if (old != null && old.expireAt() != NO_EXPIRY) {
            volatileKeys--;
        }
        if (entry.expireAt() != NO_EXPIRY) {
            volatileKeys++;
            deadlines.add(new Deadline(entry.expireAt(), key));
            compactDeadlinesIfBloated();
        }
    }

    private boolean remove(String key) {
        Entry old = entries.remove(key);
        if (old != null && old.expireAt() != NO_EXPIRY) {
            volatileKeys--;
        }
        return old != null;
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
