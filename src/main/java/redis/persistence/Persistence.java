package redis.persistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import redis.server.Config;
import redis.store.Keyspace;

/**
 * Snapshots to disk: on request (SAVE, BGSAVE), when a save rule is met,
 * and at shutdown.
 *
 * The data is copied while holding the command lock, which is quick because
 * stored entries are immutable, so only references get copied. Serializing
 * and writing the file happen without the lock, so a big dataset doesn't
 * stall clients. That's the same split Redis gets from fork().
 */
public final class Persistence {

    private static final Logger log = Logger.getLogger(Persistence.class.getName());
    /** After a failed automatic save, wait this long before trying again (Redis uses 5 s too). */
    private static final long RETRY_DELAY_MS = 5000;

    private final Config config;
    private final ReentrantLock commandLock;
    private final Keyspace db;
    private final AtomicBoolean inProgress = new AtomicBoolean();

    /** Writes since the last successful save. Guarded by the command lock. */
    private long dirty;
    private long dirtyAtSnapshot;
    private volatile long lastSaveAt = System.currentTimeMillis();
    private volatile long lastAttemptAt;
    private volatile boolean lastSaveOk = true;
    private volatile long saves;

    public Persistence(Config config, ReentrantLock commandLock, Keyspace db) {
        this.config = config;
        this.commandLock = commandLock;
        this.db = db;
    }

    /** Records writes. Caller holds the command lock. */
    public void changed(long count) {
        dirty += count;
    }

    /** SAVE: snapshot now and return once it's on disk. Fails if another save is running. */
    public void save() throws IOException {
        if (!inProgress.compareAndSet(false, true)) {
            throw new IOException("Background save already in progress");
        }
        try {
            write(copy());
        } finally {
            inProgress.set(false);
        }
    }

    /** Like save(), but waits for a running save to finish first. Used at shutdown. */
    public void saveWhenIdle(long timeoutMillis) throws IOException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!inProgress.compareAndSet(false, true)) {
            if (System.currentTimeMillis() > deadline) {
                throw new IOException("timed out waiting for a running save");
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for a running save");
            }
        }
        try {
            write(copy());
        } finally {
            inProgress.set(false);
        }
    }

    /** BGSAVE: snapshot now, write in the background. False if a save is already running. */
    public boolean saveInBackground() {
        if (!inProgress.compareAndSet(false, true)) {
            return false;
        }
        Keyspace.Snapshot snapshot = copy();
        Thread.ofVirtual().name("bgsave").start(() -> {
            try {
                write(snapshot);
            } catch (IOException e) {
                // write() already logged it and recorded the failure for INFO.
            } finally {
                inProgress.set(false);
            }
        });
        return true;
    }

    /** Called every second: starts a background save when any save rule is met. */
    public void checkSaveRules() {
        if (config.saveRules().isEmpty() || inProgress.get()) {
            return;
        }
        long now = System.currentTimeMillis();
        if (!lastSaveOk && now - lastAttemptAt < RETRY_DELAY_MS) {
            return;
        }
        long elapsedSeconds = (now - lastSaveAt) / 1000;
        long changes;
        commandLock.lock();
        try {
            changes = dirty;
        } finally {
            commandLock.unlock();
        }
        for (Config.SaveRule rule : config.saveRules()) {
            if (changes >= rule.changes() && elapsedSeconds >= rule.seconds()) {
                log.info(changes + " changes in " + elapsedSeconds + " seconds. Saving...");
                saveInBackground();
                return;
            }
        }
    }

    public boolean inProgress() {
        return inProgress.get();
    }

    /** Unix time in seconds of the last successful save (or of startup, before any save). */
    public long lastSaveSeconds() {
        return lastSaveAt / 1000;
    }

    public List<String> info() {
        List<String> lines = new ArrayList<>();
        lines.add("loading:0");
        lines.add("rdb_changes_since_last_save:" + dirty);
        lines.add("rdb_bgsave_in_progress:" + (inProgress.get() ? 1 : 0));
        lines.add("rdb_last_save_time:" + lastSaveSeconds());
        lines.add("rdb_last_bgsave_status:" + (lastSaveOk ? "ok" : "err"));
        lines.add("rdb_saves:" + saves);
        lines.add("aof_enabled:0");
        return lines;
    }

    private Keyspace.Snapshot copy() {
        commandLock.lock();
        try {
            dirtyAtSnapshot = dirty;
            return db.snapshot();
        } finally {
            commandLock.unlock();
        }
    }

    private void write(Keyspace.Snapshot snapshot) throws IOException {
        Path target = config.rdbPath();
        try {
            writeAtomically(target, Rdb.write(snapshot));
        } catch (IOException | RuntimeException e) {
            lastSaveOk = false;
            lastAttemptAt = System.currentTimeMillis();
            log.warning("saving " + target + " failed: " + e);
            throw e instanceof IOException io ? io : new IOException(e);
        }
        commandLock.lock();
        try {
            // Writes made while the file was being written still count for the next save.
            dirty -= dirtyAtSnapshot;
        } finally {
            commandLock.unlock();
        }
        lastSaveAt = System.currentTimeMillis();
        lastSaveOk = true;
        saves++;
        log.info("saved " + snapshot.size() + " keys to " + target);
    }

    /**
     * Writes to a temp file in the same directory, forces it to disk, and
     * renames it over the target, so neither a crash mid-write nor a power
     * cut right after the rename can leave a half-written or empty dump
     * behind. The temp file is created owner-only (0600 on POSIX), and the
     * rename keeps that.
     */
    public static void writeAtomically(Path target, byte[] data) throws IOException {
        Path dir = target.toAbsolutePath().getParent();
        Files.createDirectories(dir);
        Path tmp = Files.createTempFile(dir, "temp-", ".rdb");
        try {
            try (FileChannel file = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                while (buffer.hasRemaining()) {
                    file.write(buffer);
                }
                file.force(true);
            }
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
        // Make the rename itself durable. Not every platform lets you sync a directory.
        try (FileChannel directory = FileChannel.open(dir, StandardOpenOption.READ)) {
            directory.force(true);
        } catch (IOException ignored) {
            // best effort
        }
    }
}
