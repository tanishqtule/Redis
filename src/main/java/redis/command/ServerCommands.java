package redis.command;

import java.util.List;

import redis.protocol.Resp;
import redis.server.Session;

/** Connection and server-level commands that don't touch the keyspace. */
public final class ServerCommands {

    public void registerAll(CommandTable table) {
        table.register("PING", -1, this::ping);
        table.register("ECHO", 2, (s, args) -> Resp.bulk(args.get(1)));
        table.register("QUIT", 1, this::quit);
        // redis-cli sends COMMAND DOCS on startup to drive its hints. An empty
        // reply is enough to keep it happy.
        table.register("COMMAND", -1, (s, args) -> Resp.EMPTY_ARRAY);
    }

    private byte[] ping(Session session, List<byte[]> args) {
        if (args.size() > 2) {
            throw new CommandException("ERR wrong number of arguments for 'ping' command");
        }
        return args.size() == 2 ? Resp.bulk(args.get(1)) : Resp.PONG;
    }

    private byte[] quit(Session session, List<byte[]> args) {
        session.requestClose();
        return Resp.OK;
    }
}
