package redis.command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import redis.persistence.Rdb;
import redis.protocol.Resp;
import redis.server.Config;
import redis.server.Session;
import redis.store.Glob;
import redis.store.Keyspace;

/** Connection and server-level commands: PING, ECHO, CONFIG, SAVE, ... */
public final class ServerCommands {

    private final Config config;
    private final Keyspace db;

    public ServerCommands(Config config, Keyspace db) {
        this.config = config;
        this.db = db;
    }

    public void registerAll(CommandTable table) {
        table.register("PING", -1, this::ping);
        table.register("ECHO", 2, (s, args) -> Resp.bulk(args.get(1)));
        table.register("QUIT", 1, this::quit);
        // redis-cli sends COMMAND DOCS on startup to drive its hints. An empty
        // reply is enough to keep it happy.
        table.register("COMMAND", -1, (s, args) -> Resp.EMPTY_ARRAY);
        table.register("CONFIG", -2, this::config);
        table.register("SAVE", 1, this::save);
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

    /** Only CONFIG GET, and only for the handful of settings this server has. */
    private byte[] config(Session session, List<byte[]> args) {
        String sub = Args.upper(args.get(1));
        if (!sub.equals("GET")) {
            throw new CommandException("ERR CONFIG " + sub + " is not supported");
        }
        if (args.size() < 3) {
            throw new CommandException("ERR wrong number of arguments for 'config|get' command");
        }
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("dir", Path.of(config.dir()).toAbsolutePath().normalize().toString());
        settings.put("dbfilename", config.dbFilename());
        settings.put("port", Integer.toString(config.port()));
        settings.put("save", "");
        settings.put("appendonly", "no");

        Map<String, String> matched = new LinkedHashMap<>();
        for (int i = 2; i < args.size(); i++) {
            String pattern = Args.str(args, i).toLowerCase();
            settings.forEach((name, value) -> {
                if (Glob.matches(pattern, name)) {
                    matched.put(name, value);
                }
            });
        }
        List<byte[]> reply = new ArrayList<>();
        matched.forEach((name, value) -> {
            reply.add(Resp.bulk(name));
            reply.add(Resp.bulk(value));
        });
        return Resp.array(reply);
    }

    private byte[] save(Session session, List<byte[]> args) {
        try {
            writeSnapshot(config.rdbPath(), Rdb.write(db));
        } catch (IOException e) {
            throw new CommandException("ERR failed to save snapshot: " + e.getMessage());
        }
        return Resp.OK;
    }

    /** Writes to a temp file and renames it, so a crash mid-write never leaves a half-written dump behind. */
    static void writeSnapshot(Path target, byte[] snapshot) throws IOException {
        Path dir = target.toAbsolutePath().getParent();
        Files.createDirectories(dir);
        Path tmp = Files.createTempFile(dir, "temp-", ".rdb");
        try {
            Files.write(tmp, snapshot);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
    }
}
