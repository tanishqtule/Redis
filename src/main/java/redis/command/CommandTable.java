package redis.command;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import redis.protocol.Resp;
import redis.server.Session;

/**
 * Looks up commands and runs them.
 *
 * Every command executes while holding one global lock, which gives the same
 * guarantee real Redis gets from being single-threaded: each command is
 * atomic and they apply in one total order. Connection threads still do
 * their socket I/O in parallel; only execution is serialised.
 */
public final class CommandTable {

    private static final Logger log = Logger.getLogger(CommandTable.class.getName());

    private final Map<String, Command> commands = new HashMap<>();
    private final ReentrantLock lock;

    public CommandTable(ReentrantLock lock) {
        this.lock = lock;
    }

    public void register(String name, int arity, Set<Command.Flag> flags, Command.Handler handler) {
        commands.put(name, new Command(name, arity, flags, handler));
    }

    public void register(String name, int arity, Command.Handler handler) {
        register(name, arity, Set.of(), handler);
    }

    public byte[] dispatch(Session session, List<byte[]> args) {
        String name = Args.upper(args.get(0));
        Command command = commands.get(name);
        if (command == null) {
            return Resp.error(unknownCommand(args));
        }
        if (!command.acceptsArgCount(args.size())) {
            return Resp.error("ERR wrong number of arguments for '" + name.toLowerCase(Locale.ROOT) + "' command");
        }
        if (command.has(Command.Flag.BLOCKING)) {
            return invoke(command, session, args);
        }
        lock.lock();
        try {
            return invoke(command, session, args);
        } finally {
            lock.unlock();
        }
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

    private static String unknownCommand(List<byte[]> args) {
        StringBuilder sb = new StringBuilder("ERR unknown command '")
                .append(Args.str(args, 0))
                .append("', with args beginning with: ");
        for (int i = 1; i < args.size(); i++) {
            sb.append('\'').append(Args.str(args, i)).append("' ");
        }
        return sb.toString();
    }
}
