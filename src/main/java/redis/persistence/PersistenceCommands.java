package redis.persistence;

import static redis.command.Command.Flag.BLOCKING;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import redis.command.Args;
import redis.command.CommandException;
import redis.command.CommandTable;
import redis.protocol.Resp;
import redis.server.Session;

/** SAVE, BGSAVE, LASTSAVE and SHUTDOWN. */
public final class PersistenceCommands {

    /** Stops the server. {@code save} is true for SAVE, false for NOSAVE, null for "if save rules exist". */
    @FunctionalInterface
    public interface Shutdown {
        boolean shutdown(Boolean save);
    }

    private final Persistence persistence;
    private final Shutdown shutdown;

    public PersistenceCommands(Persistence persistence, Shutdown shutdown) {
        this.persistence = persistence;
        this.shutdown = shutdown;
    }

    public void registerAll(CommandTable table) {
        // SAVE waits for the file but doesn't hold the command lock while writing it.
        table.register("SAVE", 1, Set.of(BLOCKING), this::save);
        table.register("BGSAVE", -1, (s, args) -> {
            if (args.size() > 2 || args.size() == 2 && !Args.upper(args.get(1)).equals("SCHEDULE")) {
                throw CommandException.syntax();
            }
            if (!persistence.saveInBackground()) {
                throw new CommandException("ERR Background save already in progress");
            }
            return Resp.simple("Background saving started");
        });
        table.register("LASTSAVE", 1, (s, args) -> Resp.integer(persistence.lastSaveSeconds()));
        table.register("SHUTDOWN", -1, Set.of(BLOCKING), this::shutdown);
    }

    private byte[] save(Session session, List<byte[]> args) {
        try {
            persistence.save();
            return Resp.OK;
        } catch (IOException e) {
            throw new CommandException("ERR " + e.getMessage());
        }
    }

    /** SHUTDOWN [NOSAVE | SAVE]. On success the connection just closes, as in Redis. */
    private byte[] shutdown(Session session, List<byte[]> args) {
        Boolean save = null;
        for (int i = 1; i < args.size(); i++) {
            switch (Args.upper(args.get(i))) {
                case "SAVE" -> save = Boolean.TRUE;
                case "NOSAVE" -> save = Boolean.FALSE;
                case "NOW", "FORCE" -> { }
                default -> throw CommandException.syntax();
            }
        }
        if (!shutdown.shutdown(save)) {
            throw new CommandException("ERR Errors trying to SHUTDOWN. Check logs.");
        }
        session.requestClose();
        return null;
    }
}
