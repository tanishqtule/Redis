package redis.command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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
    private final Supplier<List<String>> replicationInfo;
    private final long startedAt = System.currentTimeMillis();

    public ServerCommands(Config config, Keyspace db, Supplier<List<String>> replicationInfo) {
        this.config = config;
        this.db = db;
        this.replicationInfo = replicationInfo;
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
        table.register("INFO", -1, this::info);
        table.register("CLIENT", -2, this::client);
        // Only database 0 exists. Accepting SELECT 0 matters because a real
        // master starts its replication stream with it.
        table.register("SELECT", 2, (s, args) -> {
            if (Args.parseLong(args.get(1)) != 0) {
                throw new CommandException("ERR DB index is out of range");
            }
            return Resp.OK;
        });
    }

    private byte[] client(Session session, List<byte[]> args) {
        String sub = Args.upper(args.get(1));
        return switch (sub) {
            case "ID" -> Resp.integer(session.id());
            case "GETNAME" -> session.name().isEmpty() ? Resp.NULL_BULK : Resp.bulk(session.name());
            case "SETNAME" -> {
                String name = Args.str(args, args.size() == 3 ? 2 : 1);
                if (args.size() != 3 || !name.chars().allMatch(c -> c > ' ' && c <= '~')) {
                    throw new CommandException("ERR Client names cannot contain spaces, newlines or special characters.");
                }
                session.setName(name);
                yield Resp.OK;
            }
            // Client libraries announce their name and version on connect; nothing to keep.
            case "SETINFO" -> Resp.OK;
            case "CONSISTENCY" -> consistency(session, args);
            default -> throw new CommandException("ERR unknown subcommand '" + Args.str(args, 1) + "'");
        };
    }

    /**
     * CLIENT CONSISTENCY [LINEARIZABLE | EVENTUAL]. Linearizable makes reads on
     * a replica wait until they can't miss any write the master had already
     * acknowledged. A master's reads are always linearizable, so there it's
     * accepted and changes nothing.
     */
    private byte[] consistency(Session session, List<byte[]> args) {
        if (args.size() == 2) {
            return Resp.bulk(session.linearizableReads() ? "linearizable" : "eventual");
        }
        if (args.size() != 3) {
            throw CommandException.syntax();
        }
        switch (Args.upper(args.get(2))) {
            case "LINEARIZABLE" -> session.setLinearizableReads(true);
            case "EVENTUAL" -> session.setLinearizableReads(false);
            default -> throw new CommandException("ERR consistency must be LINEARIZABLE or EVENTUAL");
        }
        return Resp.OK;
    }

    private byte[] info(Session session, List<byte[]> args) {
        List<String> wanted = new ArrayList<>();
        for (int i = 1; i < args.size(); i++) {
            wanted.add(Args.str(args, i).toLowerCase());
        }
        boolean all = wanted.isEmpty() || wanted.contains("all") || wanted.contains("everything");

        StringBuilder out = new StringBuilder();
        if (all || wanted.contains("server")) {
            section(out, "Server", List.of(
                    "server_name:redis-java",
                    "redis_version:0.1.0",
                    "tcp_port:" + config.port(),
                    "uptime_in_seconds:" + (System.currentTimeMillis() - startedAt) / 1000));
        }
        if (all || wanted.contains("replication")) {
            section(out, "Replication", replicationInfo.get());
        }
        if (all || wanted.contains("keyspace")) {
            section(out, "Keyspace", db.size() == 0 ? List.of()
                    : List.of("db0:keys=" + db.size() + ",expires=" + db.volatileKeys() + ",avg_ttl=0"));
        }
        return Resp.bulk(out.toString());
    }

    private static void section(StringBuilder out, String title, List<String> lines) {
        if (!out.isEmpty()) {
            out.append("\r\n");
        }
        out.append("# ").append(title).append("\r\n");
        for (String line : lines) {
            out.append(line).append("\r\n");
        }
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
