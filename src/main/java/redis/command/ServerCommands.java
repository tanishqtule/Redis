package redis.command;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import redis.protocol.Resp;
import redis.server.Config;
import redis.server.Session;
import redis.server.Stats;
import redis.store.Glob;
import redis.store.Keyspace;

/** Connection and server-level commands: PING, AUTH, INFO, CONFIG GET, CLIENT, ... */
public final class ServerCommands {

    private static final Logger log = Logger.getLogger(ServerCommands.class.getName());
    public static final String VERSION = "0.3.0";

    /** What the server knows about itself that INFO and CLIENT report. */
    public interface ServerState {
        List<String> replicationInfo();

        List<String> persistenceInfo();

        Collection<Session> sessions();

        Stats stats();

        long commandsProcessed();

        /** Keyspace hits and misses, live connections plus those already closed. */
        long[] keyspaceHitsAndMisses();

        /** The port actually bound, which differs from the configured one when that was 0. */
        int port();

        int tlsPort();
    }

    private final Config config;
    private final Keyspace db;
    private final ServerState state;
    private final AtomicLong authFailures = new AtomicLong();
    private final long startedAt = System.currentTimeMillis();
    private volatile long lastAuthWarning;

    public ServerCommands(Config config, Keyspace db, ServerState state) {
        this.config = config;
        this.db = db;
        this.state = state;
    }

    public void registerAll(CommandTable table) {
        table.register("PING", -1, this::ping);
        table.register("ECHO", 2, (s, args) -> Resp.bulk(args.get(1)));
        table.register("QUIT", 1, this::quit);
        // redis-cli sends COMMAND DOCS on startup to drive its hints. An empty
        // reply is enough to keep it happy.
        table.register("COMMAND", -1, (s, args) -> Resp.EMPTY_ARRAY);
        table.register("CONFIG", -2, this::config);
        table.register("INFO", -1, this::info);
        table.register("CLIENT", -2, this::client);
        table.register("AUTH", -2, this::auth);
        table.register("TIME", 1, (s, args) -> {
            java.time.Instant now = java.time.Instant.now();
            return Resp.array(List.of(Resp.bulk(Long.toString(now.getEpochSecond())),
                    Resp.bulk(Long.toString(now.getNano() / 1000))));
        });
        // Only database 0 exists. Accepting SELECT 0 matters because a real
        // master starts its replication stream with it.
        table.register("SELECT", 2, (s, args) -> {
            if (Args.parseLong(args.get(1)) != 0) {
                throw new CommandException("ERR DB index is out of range");
            }
            return Resp.OK;
        });
    }

    // ---- AUTH ----

    /** AUTH password, or AUTH default password (the Redis 6 form; "default" is the only user). */
    private byte[] auth(Session session, List<byte[]> args) {
        if (args.size() > 3) {
            throw CommandException.syntax();
        }
        String expected = config.requirePass();
        if (expected == null) {
            throw new CommandException("ERR AUTH <password> called without any password configured for the default user."
                    + " Are you sure your configuration is correct?");
        }
        boolean userOk = args.size() == 2 || Args.str(args, 1).equals("default");
        boolean passwordOk = sameSecret(args.get(args.size() - 1), expected.getBytes(StandardCharsets.UTF_8));
        if (!(userOk & passwordOk)) {
            long failures = authFailures.incrementAndGet();
            long now = System.currentTimeMillis();
            if (now - lastAuthWarning > 10_000) {
                lastAuthWarning = now;
                log.warning("failed AUTH from " + session.peer() + " (" + failures + " failures so far)");
            }
            throw new CommandException("WRONGPASS invalid username-password pair or user is disabled.");
        }
        session.setAuthenticated(true);
        return Resp.OK;
    }

    /**
     * Compares in constant time. Hashing first gives both sides the same
     * length, so not even the password's length leaks through timing.
     */
    private static boolean sameSecret(byte[] given, byte[] expected) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            byte[] a = sha.digest(given);
            byte[] b = sha.digest(expected);
            return MessageDigest.isEqual(a, b);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is always available", e);
        }
    }

    // ---- CLIENT ----

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
            case "LIST" -> Resp.bulk(clientList(session));
            case "KILL" -> kill(session, args);
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

    /** One line per client, in Redis' key=value format (a subset of its fields). */
    private String clientList(Session self) {
        long now = System.currentTimeMillis();
        StringBuilder out = new StringBuilder();
        for (Session s : state.sessions()) {
            out.append("id=").append(s.id())
                    .append(" addr=").append(address(s))
                    .append(" name=").append(s.name())
                    .append(" age=").append((now - s.createdAt()) / 1000)
                    .append(" idle=").append((now - s.lastActiveAt()) / 1000)
                    .append(" flags=").append(s.isReplica() ? "S" : "N")
                    .append(" cmd=").append(s == self ? "client|list" : s.lastCommand())
                    .append('\n');
        }
        return out.toString();
    }

    /** CLIENT KILL ID id | CLIENT KILL ADDR ip:port | CLIENT KILL ip:port (old form). */
    private byte[] kill(Session self, List<byte[]> args) {
        if (args.size() == 3) {
            String addr = Args.str(args, 2);
            for (Session s : state.sessions()) {
                if (address(s).equals(addr)) {
                    s.kill();
                    return Resp.OK;
                }
            }
            throw new CommandException("ERR No such client");
        }
        if (args.size() != 4) {
            throw CommandException.syntax();
        }
        String filter = Args.upper(args.get(2));
        String value = Args.str(args, 3);
        int killed = 0;
        for (Session s : state.sessions()) {
            boolean match = switch (filter) {
                case "ID" -> Long.toString(s.id()).equals(value);
                case "ADDR" -> address(s).equals(value);
                default -> throw CommandException.syntax();
            };
            if (match) {
                s.kill();
                killed++;
            }
        }
        return Resp.integer(killed);
    }

    private static String address(Session s) {
        return s.peer().replaceFirst("^/", "");
    }

    // ---- CONFIG GET ----

    /** Only CONFIG GET, and only for the settings this server has. */
    private byte[] config(Session session, List<byte[]> args) {
        String sub = Args.upper(args.get(1));
        if (!sub.equals("GET")) {
            throw new CommandException("ERR CONFIG " + sub + " is not supported (settings come from the config file)");
        }
        if (args.size() < 3) {
            throw new CommandException("ERR wrong number of arguments for 'config|get' command");
        }
        // What CONFIG GET can see. Passwords are deliberately left out: anyone
        // who can run CONFIG GET could otherwise read them back.
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("dir", Path.of(config.dir()).toAbsolutePath().normalize().toString());
        settings.put("dbfilename", config.dbFilename());
        settings.put("port", Integer.toString(config.port()));
        settings.put("bind", String.join(" ", config.bind()));
        settings.put("protected-mode", config.protectedMode() ? "yes" : "no");
        settings.put("save", saveRules());
        settings.put("appendonly", "no");
        settings.put("maxmemory", Long.toString(config.maxMemory()));
        settings.put("maxmemory-policy", config.maxMemoryPolicy().configName());
        settings.put("maxmemory-samples", Integer.toString(config.maxMemorySamples()));
        settings.put("maxclients", Integer.toString(config.maxClients()));
        settings.put("timeout", Integer.toString(config.timeoutSeconds()));
        settings.put("proto-max-bulk-len", Integer.toString(config.protoMaxBulkLen()));
        settings.put("tls-port", Integer.toString(config.tlsPort()));

        Map<String, String> matched = new LinkedHashMap<>();
        for (int i = 2; i < args.size(); i++) {
            String pattern = Args.str(args, i).toLowerCase(Locale.ROOT);
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

    private String saveRules() {
        StringBuilder sb = new StringBuilder();
        for (Config.SaveRule rule : config.saveRules()) {
            if (!sb.isEmpty()) {
                sb.append(' ');
            }
            sb.append(rule.seconds()).append(' ').append(rule.changes());
        }
        return sb.toString();
    }

    // ---- INFO ----

    private byte[] info(Session session, List<byte[]> args) {
        List<String> wanted = new ArrayList<>();
        for (int i = 1; i < args.size(); i++) {
            wanted.add(Args.str(args, i).toLowerCase(Locale.ROOT));
        }
        boolean all = wanted.isEmpty() || wanted.contains("all") || wanted.contains("everything")
                || wanted.contains("default");

        StringBuilder out = new StringBuilder();
        if (all || wanted.contains("server")) {
            long uptime = (System.currentTimeMillis() - startedAt) / 1000;
            section(out, "Server", List.of(
                    "redis_version:" + VERSION,
                    "server_name:redis-java",
                    "redis_mode:standalone",
                    "os:" + System.getProperty("os.name") + " " + System.getProperty("os.arch"),
                    "process_id:" + ProcessHandle.current().pid(),
                    "java_version:" + Runtime.version(),
                    "tcp_port:" + state.port(),
                    "tls_port:" + state.tlsPort(),
                    "uptime_in_seconds:" + uptime,
                    "uptime_in_days:" + uptime / 86_400));
        }
        if (all || wanted.contains("clients")) {
            section(out, "Clients", List.of(
                    "connected_clients:" + state.sessions().size(),
                    "maxclients:" + config.maxClients(),
                    "watching_clients:" + state.sessions().stream().filter(s -> !s.watch().isEmpty()).count(),
                    "total_watched_keys:" + db.watchedKeys()));
        }
        if (all || wanted.contains("memory")) {
            Runtime rt = Runtime.getRuntime();
            long heapUsed = rt.totalMemory() - rt.freeMemory();
            section(out, "Memory", List.of(
                    "used_memory:" + db.usedMemory(),
                    "used_memory_human:" + human(db.usedMemory()),
                    "used_memory_estimate:keys and values plus a fixed per-key overhead",
                    "jvm_heap_used:" + heapUsed,
                    "jvm_heap_used_human:" + human(heapUsed),
                    "jvm_heap_max:" + rt.maxMemory(),
                    "jvm_heap_max_human:" + human(rt.maxMemory()),
                    "maxmemory:" + config.maxMemory(),
                    "maxmemory_human:" + human(config.maxMemory()),
                    "maxmemory_policy:" + config.maxMemoryPolicy().configName()));
        }
        if (all || wanted.contains("persistence")) {
            section(out, "Persistence", state.persistenceInfo());
        }
        if (all || wanted.contains("stats")) {
            Stats stats = state.stats();
            section(out, "Stats", List.of(
                    "total_connections_received:" + stats.connectionsReceived.get(),
                    "total_commands_processed:" + state.commandsProcessed(),
                    "rejected_connections:" + stats.rejectedConnections.get(),
                    "expired_keys:" + stats.expiredKeys.get(),
                    "evicted_keys:" + stats.evictedKeys.get(),
                    "keyspace_hits:" + state.keyspaceHitsAndMisses()[0],
                    "keyspace_misses:" + state.keyspaceHitsAndMisses()[1],
                    "acl_access_denied_auth:" + authFailures.get()));
        }
        if (all || wanted.contains("replication")) {
            section(out, "Replication", state.replicationInfo());
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

    private static String human(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        }
        String[] units = {"K", "M", "G", "T"};
        double value = bytes;
        int unit = -1;
        while (value >= 1024 && unit < units.length - 1) {
            value /= 1024;
            unit++;
        }
        return String.format(Locale.ROOT, "%.2f%s", value, units[unit]);
    }

    // ---- small ones ----

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
