package redis.server;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Per-connection state. Apart from the listening port, only the connection's own thread touches it. */
public final class Session {

    private static final AtomicLong ids = new AtomicLong();

    private final long id = ids.incrementAndGet();
    private final String peer;
    private final OutputStream out;
    private final boolean fromMaster;

    private boolean inMulti;
    private boolean multiFailed;
    private final List<List<byte[]>> queued = new ArrayList<>();

    private boolean closeRequested;
    private boolean replica;
    private volatile int listeningPort;
    private String name = "";
    private boolean linearizableReads;

    public Session(String peer, OutputStream out, boolean fromMaster) {
        this.peer = peer;
        this.out = out;
        this.fromMaster = fromMaster;
    }

    public long id() {
        return id;
    }

    public String peer() {
        return peer;
    }

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
