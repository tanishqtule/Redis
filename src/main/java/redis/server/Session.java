package redis.server;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/** Per-connection state. Only ever touched by the connection's own thread. */
public final class Session {

    private final String peer;
    private final OutputStream out;
    private final boolean fromMaster;

    private boolean inMulti;
    private boolean multiFailed;
    private final List<List<byte[]>> queued = new ArrayList<>();

    private boolean closeRequested;
    private boolean replica;

    public Session(String peer, OutputStream out, boolean fromMaster) {
        this.peer = peer;
        this.out = out;
        this.fromMaster = fromMaster;
    }

    public String peer() {
        return peer;
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
}
