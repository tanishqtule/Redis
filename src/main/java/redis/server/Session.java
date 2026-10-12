package redis.server;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import redis.store.Keyspace;

/** Per-connection state. Apart from the listening port, only the connection's own thread touches it. */
public final class Session {

    private static final AtomicLong ids = new AtomicLong();

    private final long id = ids.incrementAndGet();
    private final long createdAt = System.currentTimeMillis();
    // Written by the connection's thread on every command and read only by
    // CLIENT LIST from other threads. Not volatile on purpose: a slightly
    // stale idle time is fine, a memory fence on every command isn't.
    private long lastActiveAt = createdAt;
    /** The raw name of the last command; only turned into text when someone runs CLIENT LIST. */
    private byte[] lastCommand;
    private final String peer;
    private final OutputStream out;
    private final boolean fromMaster;

    private boolean inMulti;
    private boolean multiFailed;
    private final List<List<byte[]>> queued = new ArrayList<>();
    private final Keyspace.Watch watch = new Keyspace.Watch();

    private boolean closeRequested;
    private boolean replica;
    private volatile int listeningPort;
    private String name = "";
    private boolean authenticated;
    private boolean linearizableReads;
    private volatile Runnable closer = () -> { };

    public Session(String peer, OutputStream out, boolean fromMaster) {
        this.peer = peer;
        this.out = out;
        this.fromMaster = fromMaster;
    }

    public long id() {
        return id;
    }

    public long createdAt() {
        return createdAt;
    }

    public long lastActiveAt() {
        return lastActiveAt;
    }

    /**
     * The last command's name, for CLIENT LIST. It's client-supplied, so it's
     * cut short and stripped of anything that could break the line-per-client
     * output.
     */
    public String lastCommand() {
        byte[] name = lastCommand;
        if (name == null) {
            return "NULL";
        }
        int length = Math.min(name.length, 32);
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            int c = name[i] & 0xff;
            chars[i] = c > ' ' && c < 0x7f ? Character.toLowerCase((char) c) : '?';
        }
        return new String(chars);
    }

    // Keyspace hits and misses for INFO, counted per connection: a counter shared
    // by all connections, bumped under the command lock, slowed hot GETs by a
    // measurable amount. Totals are summed when someone asks.
    private long keyspaceHits;
    private long keyspaceMisses;

    /** Counts a key lookup for INFO's keyspace_hits / keyspace_misses. */
    public void countLookup(boolean hit) {
        if (hit) {
            keyspaceHits++;
        } else {
            keyspaceMisses++;
        }
    }

    public long keyspaceHits() {
        return keyspaceHits;
    }

    public long keyspaceMisses() {
        return keyspaceMisses;
    }

    /** Bookkeeping for CLIENT LIST: when this client last did something, and what. */
    public void commandStarted(byte[] name) {
        lastActiveAt = System.currentTimeMillis();
        lastCommand = name;
    }

    public String peer() {
        return peer;
    }

    /** How to cut this connection off from another thread: closes the socket without flushing anything. */
    public void setCloser(Runnable closer) {
        this.closer = closer;
    }

    /** Disconnects the client immediately. Safe from any thread, and never blocks. */
    public void kill() {
        closer.run();
    }

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /** Whether this client has passed AUTH. Only matters when the server has a password. */
    public boolean authenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    /** CLIENT CONSISTENCY LINEARIZABLE: on a replica, reads wait until they're sure to be up to date. */
    public boolean linearizableReads() {
        return linearizableReads;
    }

    public void setLinearizableReads(boolean on) {
        this.linearizableReads = on;
    }

    public OutputStream out() {
        return out;
    }

    /** True for the link a replica keeps open to its master; writes arriving on it are applied silently. */
    public boolean isFromMaster() {
        return fromMaster;
    }

    public boolean inMulti() {
        return inMulti;
    }

    public void beginMulti() {
        inMulti = true;
        multiFailed = false;
        queued.clear();
    }

    public void queue(List<byte[]> args) {
        queued.add(args);
    }

    /** The keys this connection has WATCHed. Only touched under the command lock. */
    public Keyspace.Watch watch() {
        return watch;
    }

    /** A command failed to queue (unknown name, bad arity); EXEC must then refuse to run anything. */
    public void markMultiFailed() {
        multiFailed = true;
    }

    public boolean multiFailed() {
        return multiFailed;
    }

    /** Ends the transaction and hands back whatever was queued. */
    public List<List<byte[]>> endMulti() {
        List<List<byte[]>> commands = List.copyOf(queued);
        queued.clear();
        inMulti = false;
        multiFailed = false;
        return commands;
    }

    public void requestClose() {
        closeRequested = true;
    }

    public boolean closeRequested() {
        return closeRequested;
    }

    /**
     * After PSYNC this connection belongs to the replication stream: a
     * dedicated writer owns the output, and the read loop must stop replying.
     */
    public void markReplica() {
        replica = true;
    }

    public boolean isReplica() {
        return replica;
    }

    /** The port a replica says it listens on (REPLCONF listening-port). Only shown in INFO. */
    public int listeningPort() {
        return listeningPort;
    }

    public void setListeningPort(int port) {
        this.listeningPort = port;
    }
}
