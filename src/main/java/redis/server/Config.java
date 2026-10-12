package redis.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Logger;

import redis.protocol.RespReader;
import redis.replication.ReplicationMaster;
import redis.store.Eviction;

/**
 * Server settings, written as redis.conf-style directives. They can come
 * from a config file, from the command line ({@code --port 6380}), or both;
 * later ones win, as in Redis.
 *
 * Immutable once built: {@link #with} returns a changed copy.
 */
public final class Config {

    private static final Logger log = Logger.getLogger(Config.class.getName());

    public static final int DEFAULT_PORT = 6379;

    /** One "save &lt;seconds&gt; &lt;changes&gt;" rule: snapshot when that many writes happened in that time. */
    public record SaveRule(long seconds, long changes) {
    }

    /** Redis' defaults: after an hour with 1 change, 5 minutes with 100, a minute with 10,000. */
    public static final List<SaveRule> DEFAULT_SAVE_RULES = List.of(
            new SaveRule(3600, 1), new SaveRule(300, 100), new SaveRule(60, 10_000));

    /** Directives accepted for compatibility with existing redis.conf files, but with no effect here. */
    private static final Set<String> IGNORED = Set.of(
            "loglevel", "always-show-logo", "set-proc-title", "tcp-backlog", "tcp-keepalive", "supervised",
            "pidfile", "databases", "lazyfree-lazy-eviction", "lazyfree-lazy-expire", "lazyfree-lazy-server-del",
            "replica-read-only", "slave-read-only", "repl-diskless-sync", "rdbcompression", "rdbchecksum",
            "stop-writes-on-bgsave-error", "replica-serve-stale-data", "slave-serve-stale-data");

    private int port = DEFAULT_PORT;
    private List<String> bind = List.of();
    private boolean protectedMode = true;
    private String requirePass;
    private String masterAuth;
    private String masterHost;
    private int masterPort;
    private String dir = ".";
    private String dbFilename = "dump.rdb";
    private List<SaveRule> saveRules = DEFAULT_SAVE_RULES;
    private boolean saveRulesFromConfig;
    private long maxMemory;
    private Eviction.Policy maxMemoryPolicy = Eviction.Policy.NOEVICTION;
    private int maxMemorySamples = 5;
    private int maxClients = 10_000;
    private int timeoutSeconds;
    private int protoMaxBulkLen = RespReader.DEFAULT_MAX_BULK;
    private long replicaOutputLimit = ReplicationMaster.DEFAULT_OUTPUT_LIMIT;
    private int simulatedReplicationLagMs;
    private int tlsPort;
    private String tlsCertFile;
    private String tlsKeyFile;
    private String tlsCaCertFile;
    private String tlsAuthClients = "no";
    private boolean tlsReplication;

    private Config() {
    }

    private Config copy() {
        Config c = new Config();
        c.port = port;
        c.bind = bind;
        c.protectedMode = protectedMode;
        c.requirePass = requirePass;
        c.masterAuth = masterAuth;
        c.masterHost = masterHost;
        c.masterPort = masterPort;
        c.dir = dir;
        c.dbFilename = dbFilename;
        c.saveRules = saveRules;
        c.saveRulesFromConfig = saveRulesFromConfig;
        c.maxMemory = maxMemory;
        c.maxMemoryPolicy = maxMemoryPolicy;
        c.maxMemorySamples = maxMemorySamples;
        c.maxClients = maxClients;
        c.timeoutSeconds = timeoutSeconds;
        c.protoMaxBulkLen = protoMaxBulkLen;
        c.replicaOutputLimit = replicaOutputLimit;
        c.simulatedReplicationLagMs = simulatedReplicationLagMs;
        c.tlsPort = tlsPort;
        c.tlsCertFile = tlsCertFile;
        c.tlsKeyFile = tlsKeyFile;
        c.tlsCaCertFile = tlsCaCertFile;
        c.tlsAuthClients = tlsAuthClients;
        c.tlsReplication = tlsReplication;
        return c;
    }

    public static Config defaults() {
        return new Config();
    }

    // ---- building ----

    /**
     * Parses a command line the way redis-server does: an optional config file
     * first, then {@code --directive value...} pairs that override it.
     * {@code --replicaof "host port"} and {@code --replicaof host port} both work.
     */
    public static Config fromArgs(String[] args) {
        Config config = new Config();
        int i = 0;
        if (args.length > 0 && !args[0].startsWith("--")) {
            config = fromFile(Path.of(args[0]));
            i = 1;
        }
        while (i < args.length) {
            String flag = args[i];
            if (!flag.startsWith("--") || flag.length() == 2) {
                throw new IllegalArgumentException("expected --directive, got '" + flag + "'");
            }
            List<String> values = new ArrayList<>();
            i++;
            while (i < args.length && !args[i].startsWith("--")) {
                values.add(args[i++]);
            }
            config.apply(flag.substring(2), values);
        }
        config.validate();
        return config;
    }

    public static Config fromFile(Path file) {
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("can't read config file " + file + ": " + e.getMessage());
        }
        Config config = new Config();
        for (int n = 0; n < lines.size(); n++) {
            String line = lines.get(n).strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            try {
                List<String> words = split(line);
                config.apply(words.get(0), words.subList(1, words.size()));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(file + " line " + (n + 1) + ": " + e.getMessage());
            }
        }
        config.validate();
        return config;
    }

    /** A copy with one more directive applied, e.g. {@code with("requirepass", "secret")}. */
    public Config with(String directive, String... values) {
        Config c = copy();
        c.apply(directive, List.of(values));
        c.validate();
        return c;
    }

    public Config withPort(int port) {
        return with("port", Integer.toString(port));
    }

    public Config withMaster(String host, int port) {
        return with("replicaof", host, Integer.toString(port));
    }

    public Config withDir(String dir) {
        return with("dir", dir);
    }

    public Config withDbFilename(String name) {
        return with("dbfilename", name);
    }

    public Config withSimulatedReplicationLag(int millis) {
        return with("simulate-replication-lag", Integer.toString(millis));
    }

    private void apply(String rawName, List<String> v) {
        String name = rawName.toLowerCase(Locale.ROOT);
        switch (name) {
            case "port" -> port = (int) number(name, one(name, v), 0, 65535);
            case "bind" -> {
                if (v.isEmpty()) {
                    throw new IllegalArgumentException("bind needs at least one address");
                }
                bind = List.copyOf(v);
            }
            case "protected-mode" -> protectedMode = yesNo(name, one(name, v));
            case "requirepass" -> requirePass = emptyToNull(one(name, v));
            case "requirepass-file" -> requirePass = secretFromFile(name, one(name, v));
            case "masterauth" -> masterAuth = emptyToNull(one(name, v));
            case "masterauth-file" -> masterAuth = secretFromFile(name, one(name, v));
            case "replicaof", "slaveof" -> {
                List<String> parts = v.size() == 1 ? List.of(v.get(0).trim().split("\\s+")) : v;
                if (parts.size() != 2) {
                    throw new IllegalArgumentException(name + " expects <host> <port>, or \"no one\"");
                }
                if (parts.get(0).equalsIgnoreCase("no") && parts.get(1).equalsIgnoreCase("one")) {
                    masterHost = null;
                    masterPort = 0;
                } else {
                    masterHost = parts.get(0);
                    masterPort = (int) number(name, parts.get(1), 1, 65535);
                }
            }
            case "dir" -> dir = one(name, v);
            case "dbfilename" -> {
                String file = one(name, v);
                // A path here would let whoever writes the config direct snapshots anywhere.
                if (file.isEmpty() || file.contains("/") || file.contains("\\") || file.equals("..")) {
                    throw new IllegalArgumentException("dbfilename must be a plain file name, not a path");
                }
                dbFilename = file;
            }
            case "save" -> applySave(v);
            case "maxmemory" -> maxMemory = bytes(name, one(name, v));
            case "maxmemory-policy" -> maxMemoryPolicy = Eviction.Policy.parse(one(name, v));
            case "maxmemory-samples" -> maxMemorySamples = (int) number(name, one(name, v), 1, 64);
            case "maxclients" -> maxClients = (int) number(name, one(name, v), 1, 1_000_000);
            case "timeout" -> timeoutSeconds = (int) number(name, one(name, v), 0, Integer.MAX_VALUE / 1000);
            case "proto-max-bulk-len" -> protoMaxBulkLen = (int) Math.min(
                    Integer.MAX_VALUE - 16, Math.max(1024, bytes(name, one(name, v))));
            case "client-output-buffer-limit" -> applyOutputLimit(v);
            case "simulate-replication-lag" -> simulatedReplicationLagMs =
                    (int) number(name, one(name, v), 0, 3_600_000);
            case "tls-port" -> tlsPort = (int) number(name, one(name, v), 0, 65535);
            case "tls-cert-file" -> tlsCertFile = one(name, v);
            case "tls-key-file" -> tlsKeyFile = one(name, v);
            case "tls-ca-cert-file" -> tlsCaCertFile = one(name, v);
            case "tls-auth-clients" -> {
                String mode = one(name, v).toLowerCase(Locale.ROOT);
                if (!Set.of("yes", "no", "optional").contains(mode)) {
                    throw new IllegalArgumentException("tls-auth-clients must be yes, no or optional");
                }
                tlsAuthClients = mode;
            }
            case "tls-replication" -> tlsReplication = yesNo(name, one(name, v));
            case "appendonly" -> {
                if (yesNo(name, one(name, v))) {
                    throw new IllegalArgumentException("appendonly yes isn't supported; snapshots (save) are");
                }
            }
            case "daemonize" -> {
                if (yesNo(name, one(name, v))) {
                    throw new IllegalArgumentException("daemonize yes isn't supported; run it under a supervisor");
                }
            }
            default -> {
                if (!IGNORED.contains(name)) {
                    throw new IllegalArgumentException("unknown directive '" + rawName + "'");
                }
                log.info("ignoring directive '" + name + "', which has no effect here");
            }
        }
    }

    private void applySave(List<String> v) {
        if (v.size() == 1 && v.get(0).isEmpty() || v.isEmpty()) {
            saveRules = List.of();
            saveRulesFromConfig = true;
            return;
        }
        List<String> parts = v.size() == 1 ? List.of(v.get(0).trim().split("\\s+")) : v;
        if (parts.size() % 2 != 0) {
            throw new IllegalArgumentException("save expects pairs of <seconds> <changes>");
        }
        // Like Redis, the first save line in a config replaces the defaults and later ones add to it.
        List<SaveRule> rules = new ArrayList<>(saveRulesFromConfig ? saveRules : List.of());
        for (int i = 0; i < parts.size(); i += 2) {
            rules.add(new SaveRule(number("save", parts.get(i), 1, Long.MAX_VALUE),
                    number("save", parts.get(i + 1), 1, Long.MAX_VALUE)));
        }
        saveRules = List.copyOf(rules);
        saveRulesFromConfig = true;
    }

    private void applyOutputLimit(List<String> v) {
        if (v.size() != 4) {
            throw new IllegalArgumentException("client-output-buffer-limit expects <class> <hard> <soft> <seconds>");
        }
        String clientClass = v.get(0).toLowerCase(Locale.ROOT);
        switch (clientClass) {
            case "replica", "slave" -> replicaOutputLimit = bytes("client-output-buffer-limit", v.get(1));
            case "normal", "pubsub" -> log.info("client-output-buffer-limit for '" + clientClass + "' has no effect here");
            default -> throw new IllegalArgumentException("unknown client class '" + v.get(0) + "'");
        }
    }

    private void validate() {
        if ((tlsCertFile == null) != (tlsKeyFile == null)) {
            throw new IllegalArgumentException("tls-cert-file and tls-key-file go together");
        }
        if ((tlsPort > 0 || tlsReplication) && tlsCertFile == null && tlsCaCertFile == null) {
            throw new IllegalArgumentException("TLS needs tls-cert-file/tls-key-file (and tls-ca-cert-file to verify peers)");
        }
        if (tlsPort > 0 && tlsCertFile == null) {
            throw new IllegalArgumentException("tls-port needs tls-cert-file and tls-key-file");
        }
        if (!tlsAuthClients.equals("no") && tlsPort > 0 && tlsCaCertFile == null) {
            throw new IllegalArgumentException("tls-auth-clients " + tlsAuthClients + " needs tls-ca-cert-file");
        }
    }

    // ---- the settings ----

    public int port() {
        return port;
    }

    /** Addresses to listen on; empty means all interfaces. */
    public List<String> bind() {
        return bind;
    }

    public boolean protectedMode() {
        return protectedMode;
    }

    /** The password clients must AUTH with, or null when there is none. */
    public String requirePass() {
        return requirePass;
    }

    public String masterAuth() {
        return masterAuth;
    }

    public boolean isReplica() {
        return masterHost != null;
    }

    public String masterHost() {
        return masterHost;
    }

    public int masterPort() {
        return masterPort;
    }

    public String dir() {
        return dir;
    }

    public String dbFilename() {
        return dbFilename;
    }

    public Path rdbPath() {
        return Path.of(dir, dbFilename);
    }

    public List<SaveRule> saveRules() {
        return saveRules;
    }

    /** Bytes of data allowed before eviction (or OOM errors) start; 0 means no limit. */
    public long maxMemory() {
        return maxMemory;
    }

    public Eviction.Policy maxMemoryPolicy() {
        return maxMemoryPolicy;
    }

    public int maxMemorySamples() {
        return maxMemorySamples;
    }

    public int maxClients() {
        return maxClients;
    }

    /** Seconds an idle client may stay connected; 0 means forever. */
    public int timeoutSeconds() {
        return timeoutSeconds;
    }

    public int protoMaxBulkLen() {
        return protoMaxBulkLen;
    }

    public long replicaOutputLimit() {
        return replicaOutputLimit;
    }

    public int simulatedReplicationLagMs() {
        return simulatedReplicationLagMs;
    }

    public int tlsPort() {
        return tlsPort;
    }

    public String tlsCertFile() {
        return tlsCertFile;
    }

    public String tlsKeyFile() {
        return tlsKeyFile;
    }

    public String tlsCaCertFile() {
        return tlsCaCertFile;
    }

    /** yes (client certificates required), optional, or no. */
    public String tlsAuthClients() {
        return tlsAuthClients;
    }

    public boolean tlsReplication() {
        return tlsReplication;
    }

    // ---- parsing helpers ----

    /**
     * Splits a config line into words like Redis does: whitespace separates,
     * double quotes allow escapes (\n, \t, \", \\, \xHH), single quotes are literal.
     */
    static List<String> split(String line) {
        List<String> words = new ArrayList<>();
        int i = 0;
        while (i < line.length()) {
            while (i < line.length() && Character.isWhitespace(line.charAt(i))) {
                i++;
            }
            if (i >= line.length()) {
                break;
            }
            StringBuilder word = new StringBuilder();
            char quote = line.charAt(i);
            if (quote == '"' || quote == '\'') {
                i++;
                boolean closed = false;
                while (i < line.length()) {
                    char c = line.charAt(i++);
                    if (c == quote) {
                        closed = true;
                        break;
                    }
                    if (c == '\\' && quote == '"' && i < line.length()) {
                        char e = line.charAt(i++);
                        switch (e) {
                            case 'n' -> word.append('\n');
                            case 't' -> word.append('\t');
                            case 'r' -> word.append('\r');
                            case 'x' -> {
                                if (i + 2 > line.length()) {
                                    throw new IllegalArgumentException("bad \\x escape");
                                }
                                word.append((char) Integer.parseInt(line.substring(i, i + 2), 16));
                                i += 2;
                            }
                            default -> word.append(e);
                        }
                    } else if (c == '\\' && quote == '\'' && i < line.length() && line.charAt(i) == '\'') {
                        word.append('\'');
                        i++;
                    } else {
                        word.append(c);
                    }
                }
                if (!closed) {
                    throw new IllegalArgumentException("unbalanced quotes");
                }
                if (i < line.length() && !Character.isWhitespace(line.charAt(i))) {
                    throw new IllegalArgumentException("closing quote must be followed by a space");
                }
            } else {
                while (i < line.length() && !Character.isWhitespace(line.charAt(i))) {
                    word.append(line.charAt(i++));
                }
            }
            words.add(word.toString());
        }
        if (words.isEmpty()) {
            throw new IllegalArgumentException("empty directive");
        }
        return words;
    }

    private static String one(String name, List<String> v) {
        if (v.size() != 1) {
            throw new IllegalArgumentException(name + " takes exactly one value");
        }
        return v.get(0);
    }

    private static long number(String name, String text, long min, long max) {
        long n;
        try {
            n = Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + ": '" + text + "' isn't a number");
        }
        if (n < min || n > max) {
            throw new IllegalArgumentException(name + " must be between " + min + " and " + max);
        }
        return n;
    }

    private static boolean yesNo(String name, String text) {
        return switch (text.toLowerCase(Locale.ROOT)) {
            case "yes" -> true;
            case "no" -> false;
            default -> throw new IllegalArgumentException(name + " must be yes or no");
        };
    }

    /** Memory sizes as Redis writes them: 1k = 1000, 1kb = 1024, and so on up to g/gb. */
    static long bytes(String name, String text) {
        String t = text.toLowerCase(Locale.ROOT);
        long unit = 1;
        String digits = t;
        String[][] units = {{"gb", "1073741824"}, {"mb", "1048576"}, {"kb", "1024"},
                {"g", "1000000000"}, {"m", "1000000"}, {"k", "1000"}, {"b", "1"}};
        for (String[] u : units) {
            if (t.endsWith(u[0])) {
                unit = Long.parseLong(u[1]);
                digits = t.substring(0, t.length() - u[0].length());
                break;
            }
        }
        long n = number(name, digits, 0, Long.MAX_VALUE / unit);
        return n * unit;
    }

    private static String emptyToNull(String s) {
        return s.isEmpty() ? null : s;
    }

    /** Reads a secret from a file (say a Docker or Kubernetes secret), so it isn't on the command line. */
    private static String secretFromFile(String name, String path) {
        try {
            String secret = Files.readString(Path.of(path), StandardCharsets.UTF_8).strip();
            if (secret.isEmpty()) {
                throw new IllegalArgumentException(name + ": " + path + " is empty");
            }
            return secret;
        } catch (IOException e) {
            throw new IllegalArgumentException(name + ": can't read " + path + ": " + e.getMessage());
        }
    }
}
