package redis.replication;

import static redis.command.Command.Flag.BLOCKING;

import java.util.List;
import java.util.Set;

import redis.command.Args;
import redis.command.CommandException;
import redis.command.CommandTable;
import redis.protocol.Resp;
import redis.server.Session;
import redis.store.Keyspace;

/**
 * REPLCONF, PSYNC, WAIT, ROLE and WAITOFFSET. Exactly one of {@code master}
 * and {@code link} is set, depending on whether this server is a master or a
 * replica.
 */
public final class ReplicationCommands {

    private final ReplicationMaster master;
    private final MasterLink link;
    private final Keyspace db;

    public ReplicationCommands(ReplicationMaster master, MasterLink link, Keyspace db) {
        this.master = master;
        this.link = link;
        this.db = db;
    }

    public void registerAll(CommandTable table) {
        table.register("REPLCONF", -2, this::replconf);
        table.register("PSYNC", 3, this::psync);
        table.register("WAIT", 3, Set.of(BLOCKING), this::waitForReplicas);
        table.register("ROLE", 1, (s, args) -> master != null ? master.role() : link.role());
        table.register("WAITOFFSET", 3, Set.of(BLOCKING), this::waitOffset);
    }

    private byte[] replconf(Session session, List<byte[]> args) {
        String option = Args.upper(args.get(1));
        switch (option) {
            case "ACK" -> {
                if (master != null && args.size() >= 3) {
                    master.acknowledge(session, Args.parseLong(args.get(2)));
                }
                return null; // replicas don't expect a reply to ACK
            }
            case "LISTENING-PORT" -> {
                if (args.size() >= 3) {
                    session.setListeningPort((int) Args.parseLong(args.get(2)));
                }
                return Resp.OK;
            }
            default -> {
                // capa, ip-address and friends: accepted and ignored.
                return Resp.OK;
            }
        }
    }

    private byte[] psync(Session session, List<byte[]> args) {
        if (master == null) {
            throw new CommandException("ERR this replica can't serve other replicas (chained replication isn't supported)");
        }
        if (session.isReplica()) {
            // A second writer on the same socket would interleave two streams.
            throw new CommandException("ERR this connection is already a replica");
        }
        // Partial resync would need a backlog buffer; always doing a full one is correct, just slower.
        // Nothing is flushed here: this runs under the command lock, and a peer
        // that stopped reading would freeze the whole server. Replies still
        // sitting in the output buffer go out first anyway, because the replica
        // writer appends after them and is the only one writing from now on.
        master.startFullResync(session, db.snapshot());
        return null;
    }

    private byte[] waitForReplicas(Session session, List<byte[]> args) {
        if (master == null) {
            throw new CommandException("ERR WAIT cannot be used with replica instances.");
        }
        long needed = Args.parseLong(args.get(1));
        long timeout = Args.parseLong(args.get(2));
        if (needed < 0 || timeout < 0) {
            throw new CommandException("ERR timeout is negative");
        }
        try {
            return Resp.integer(master.waitForReplicas((int) Math.min(needed, Integer.MAX_VALUE), timeout));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandException("ERR interrupted while waiting for replicas");
        }
    }

    /**
     * WAITOFFSET offset timeout: the replica-side counterpart of WAIT. Blocks
     * until this replica has applied the master's stream up to {@code offset}
     * (as reported by ROLE on the master), then returns the offset reached.
     * A result below the requested offset means the timeout passed first.
     *
     * A client that remembers the master's offset after its write and calls
     * this before reading from a replica is guaranteed to see that write.
     */
    private byte[] waitOffset(Session session, List<byte[]> args) {
        long target = Args.parseLong(args.get(1));
        long timeout = Args.parseLong(args.get(2));
        if (timeout < 0) {
            throw new CommandException("ERR timeout is negative");
        }
        if (master != null) {
            // A master has applied everything it has ever streamed.
            return Resp.integer(master.offset());
        }
        try {
            return Resp.integer(link.awaitOffset(target, timeout));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandException("ERR interrupted while waiting for the replication offset");
        }
    }
}
