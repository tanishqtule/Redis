package redis.server;

import java.nio.file.Path;

/** Startup options. Immutable once the server is running. */
public record Config(int port, String masterHost, int masterPort, String dir, String dbFilename) {

    public static final int DEFAULT_PORT = 6379;

    public static Config defaults() {
        return new Config(DEFAULT_PORT, null, 0, ".", "dump.rdb");
    }

    public boolean isReplica() {
        return masterHost != null;
    }

    public Path rdbPath() {
        return Path.of(dir, dbFilename);
    }

    public Config withPort(int port) {
        return new Config(port, masterHost, masterPort, dir, dbFilename);
    }

    public Config withMaster(String host, int port) {
        return new Config(this.port, host, port, dir, dbFilename);
    }

    public Config withDir(String dir) {
        return new Config(port, masterHost, masterPort, dir, dbFilename);
    }

    public Config withDbFilename(String dbFilename) {
        return new Config(port, masterHost, masterPort, dir, dbFilename);
    }

    /**
     * Parses redis-server style flags. {@code --replicaof} accepts both the
     * quoted form ("host port") and two separate arguments, like the real thing.
     */
    public static Config fromArgs(String[] args) {
        Config config = defaults();
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            switch (flag) {
                case "--port" -> config = config.withPort(parsePort(value(args, ++i, flag)));
                case "--dir" -> config = config.withDir(value(args, ++i, flag));
                case "--dbfilename" -> config = config.withDbFilename(value(args, ++i, flag));
                case "--replicaof" -> {
                    String[] parts = value(args, ++i, flag).trim().split("\\s+");
                    if (parts.length == 1) {
                        parts = new String[] {parts[0], value(args, ++i, flag)};
                    }
                    if (parts.length != 2) {
                        throw new IllegalArgumentException("--replicaof expects \"<host> <port>\"");
                    }
                    config = config.withMaster(parts[0], parsePort(parts[1]));
                }
                default -> throw new IllegalArgumentException("unknown option " + flag);
            }
        }
        return config;
    }

    private static String value(String[] args, int i, String flag) {
        if (i >= args.length) {
            throw new IllegalArgumentException(flag + " needs a value");
        }
        return args[i];
    }

    private static int parsePort(String s) {
        try {
            int port = Integer.parseInt(s);
            if (port < 0 || port > 65535) {
                throw new NumberFormatException();
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid port '" + s + "'");
        }
    }
}
