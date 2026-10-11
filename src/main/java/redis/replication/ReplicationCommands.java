package redis.replication;

import static redis.command.Command.Flag.BLOCKING;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;

import redis.command.Args;
import redis.command.CommandException;
import redis.command.CommandTable;
import redis.persistence.Rdb;
import redis.protocol.Resp;
import redis.server.Session;
import redis.store.Keyspace;

/** REPLCONF, PSYNC and WAIT. {@code master} is null when this server is itself a replica. */
public final class ReplicationCommands {

    private final ReplicationMaster master;
    private final Keyspace db;

    public ReplicationCommands(ReplicationMaster master, Keyspace db) {
        this.master = master;
        this.db = db;
    }

    public void registerAll(CommandTable table) {
        table.register("REPLCONF", -2, this::replconf);
        table.register("PSYNC", 3, this::psync);
        table.register("WAIT", 3, Set.of(BLOCKING), this::waitForReplicas);
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
        if (session.inMulti()) {
            throw new CommandException("ERR PSYNC inside MULTI is not allowed");
        }
        // Partial resync would need a backlog buffer; always doing a full one is correct, just slower.
        byte[] snapshot = Rdb.write(db);
        try {
            // Anything already buffered for this client (REPLCONF replies) must go out first;
            // after this the replica writer owns the stream.
            session.out().flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        master.startFullResync(session, snapshot);
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
}
