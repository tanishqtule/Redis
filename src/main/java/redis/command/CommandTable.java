package redis.command;

import static redis.command.Command.Flag.BLOCKING;
import static redis.command.Command.Flag.READ;
import static redis.command.Command.Flag.TX_CONTROL;
import static redis.command.Command.Flag.WRITE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import redis.protocol.Resp;
import redis.server.Session;

/**
 * Looks up commands and runs them, including MULTI/EXEC.
 *
 * Every command executes while holding one global lock, which gives the same
 * guarantee real Redis gets from being single-threaded: each command is
 * atomic and they apply in one total order. Connection threads still do
 * their socket I/O in parallel; only execution is serialised. That single
 * order is also what keeps replicas consistent, because writes are handed to
 * the replication stream while the lock is still held.
 */
public final class CommandTable {

    private static final Logger log = Logger.getLogger(CommandTable.class.getName());
    private static final byte[] MULTI = Args.raw("MULTI");
    private static final byte[] EXEC = Args.raw("EXEC");

    private final Map<String, Command> commands = new HashMap<>();
    private final ReentrantLock lock;
    private final boolean readOnly;

    private boolean authRequired;
    private Consumer<List<byte[]>> replicationSink = args -> { };
    /** Run before reads on connections that asked for linearizable reads. Only replicas set one. */
    private Runnable linearizableReadBarrier;
    /** Writes made by the EXEC currently running, so they can be propagated as one MULTI/EXEC block. */
    private List<List<byte[]>> execWrites;

    /** @param readOnly true on a replica: ordinary clients may not write */
    public CommandTable(ReentrantLock lock, boolean readOnly) {
        this.lock = lock;
        this.readOnly = readOnly;
        register("MULTI", 1, Set.of(TX_CONTROL), this::multi);
        register("EXEC", 1, Set.of(TX_CONTROL), this::exec);
        register("DISCARD", 1, Set.of(TX_CONTROL), this::discard);
    }

    public void register(String name, int arity, Set<Command.Flag> flags, Command.Handler handler) {
        commands.put(name, new Command(name, arity, flags, handler));
    }

    public void register(String name, int arity, Command.Handler handler) {
        register(name, arity, Set.of(), handler);
    }

    /** With a password set, everything but AUTH and QUIT needs an authenticated connection. */
    public void setAuthRequired(boolean required) {
        this.authRequired = required;
    }

    /**
     * Installs the check a replica runs before a linearizable read. It either
     * returns once the replica is caught up, or throws a CommandException.
     */
    public void setLinearizableReadBarrier(Runnable barrier) {
        this.linearizableReadBarrier = barrier;
    }

    /** Where successful writes go. Set by the server when replication is in play. */
    public void setReplicationSink(Consumer<List<byte[]>> sink) {
        this.replicationSink = sink;
    }

    public byte[] dispatch(Session session, List<byte[]> args) {
        String name = Args.upper(args.get(0));
        if (authRequired && !session.authenticated() && !session.isFromMaster()
                && !name.equals("AUTH") && !name.equals("QUIT")) {
            // Checked first, so an anonymous client learns nothing, not even which commands exist.
            return Resp.error("NOAUTH Authentication required.");
        }
        Command command = commands.get(name);

        String problem = null;
        if (command == null) {
            problem = unknownCommand(args);
        } else if (!command.acceptsArgCount(args.size())) {
            problem = "ERR wrong number of arguments for '" + name.toLowerCase(Locale.ROOT) + "' command";
        } else if (readOnly && command.has(WRITE) && !session.isFromMaster()) {
            problem = "READONLY You can't write against a read only replica.";
        }
        if (problem != null) {
            if (session.inMulti()) {
                session.markMultiFailed();
            }
            return Resp.error(problem);
        }

        if (session.inMulti() && !command.has(TX_CONTROL)) {
            if (command.has(BLOCKING)) {
                session.markMultiFailed();
                return Resp.error("ERR " + name.toLowerCase(Locale.ROOT) + " is not allowed inside MULTI");
            }
            session.queue(args);
            return Resp.QUEUED;
        }

        if (needsReadBarrier(session, command)) {
            // Outside the lock: waiting for the replica to catch up must not stall everyone else.
            try {
                linearizableReadBarrier.run();
            } catch (CommandException e) {
                if (command.name().equals("EXEC")) {
                    session.endMulti();
                }
                return Resp.error(e.getMessage());
            }
        }
        if (command.has(BLOCKING)) {
            return invoke(command, session, args);
        }
        lock.lock();
        try {
            return execute(command, session, args);
        } finally {
            lock.unlock();
        }
    }

    private boolean needsReadBarrier(Session session, Command command) {
        if (linearizableReadBarrier == null || !session.linearizableReads() || session.isFromMaster()) {
            return false;
        }
        // Inside MULTI only EXEC/DISCARD get this far; queued reads are checked once, at EXEC.
        return command.has(READ) || command.name().equals("EXEC") && session.inMulti();
    }

    /** Sends a write to replicas. Callers must hold the lock so the stream order matches execution order. */
    public void propagate(List<byte[]> args) {
        if (execWrites != null) {
            execWrites.add(args);
        } else {
            replicationSink.accept(args);
        }
    }

    private byte[] execute(Command command, Session session, List<byte[]> args) {
        byte[] reply = invoke(command, session, args);
        if (command.has(WRITE) && !Resp.isError(reply)) {
            propagate(args);
        }
        return reply;
    }

    private byte[] invoke(Command command, Session session, List<byte[]> args) {
        try {
            return command.handler().handle(session, args);
        } catch (CommandException e) {
            return Resp.error(e.getMessage());
        } catch (RuntimeException e) {
            log.log(Level.WARNING, "command " + command.name() + " failed", e);
            return Resp.error("ERR internal error while running '" + command.name().toLowerCase(Locale.ROOT) + "'");
        }
    }

    private byte[] multi(Session session, List<byte[]> args) {
        if (session.inMulti()) {
            return Resp.error("ERR MULTI calls can not be nested");
        }
        session.beginMulti();
        return Resp.OK;
    }

    private byte[] discard(Session session, List<byte[]> args) {
        if (!session.inMulti()) {
            return Resp.error("ERR DISCARD without MULTI");
        }
        session.endMulti();
        return Resp.OK;
    }

    private byte[] exec(Session session, List<byte[]> args) {
        if (!session.inMulti()) {
            return Resp.error("ERR EXEC without MULTI");
        }
        boolean failed = session.multiFailed();
        List<List<byte[]>> queued = session.endMulti();
        if (failed) {
            return Resp.error("EXECABORT Transaction discarded because of previous errors.");
        }

        // We already hold the lock (EXEC isn't BLOCKING), so nothing can interleave.
        execWrites = new ArrayList<>();
        List<byte[]> replies = new ArrayList<>(queued.size());
        try {
            for (List<byte[]> queuedArgs : queued) {
                Command command = commands.get(Args.upper(queuedArgs.get(0)));
                // As in Redis, a command that fails at run time doesn't roll back the others.
                replies.add(execute(command, session, queuedArgs));
            }
        } finally {
            List<List<byte[]>> writes = execWrites;
            execWrites = null;
            if (writes.size() == 1) {
                replicationSink.accept(writes.get(0));
            } else if (writes.size() > 1) {
                replicationSink.accept(List.of(MULTI));
                writes.forEach(replicationSink);
                replicationSink.accept(List.of(EXEC));
            }
        }
        return Resp.array(replies);
    }

    /**
     * Same shape and limits as Redis: the name and the quoted arguments are cut
     * at 128 characters, so an unknown command with a huge argument can't be
     * used to make the server echo megabytes back.
     */
    private static String unknownCommand(List<byte[]> args) {
        StringBuilder quoted = new StringBuilder();
        for (int i = 1; i < args.size() && quoted.length() < 128; i++) {
            String arg = Args.str(args, i);
            quoted.append('\'').append(arg, 0, Math.min(arg.length(), 128 - quoted.length())).append("' ");
        }
        String name = Args.str(args, 0);
        return "ERR unknown command '" + name.substring(0, Math.min(name.length(), 128))
                + "', with args beginning with: " + quoted;
    }
}
