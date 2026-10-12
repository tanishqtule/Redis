package redis;

import redis.server.Config;
import redis.server.RedisServer;

public final class Main {

    public static void main(String[] args) throws Exception {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tT %4$-7s %5$s%6$s%n");
        Config config;
        try {
            config = Config.fromArgs(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println("usage: redis-java [config-file] [--directive value ...]");
            System.err.println("  e.g. redis-java --port 6380 --requirepass-file /run/secrets/redis_password");
            System.err.println("       redis-java /etc/redis-java.conf --replicaof \"10.0.0.5 6379\"");
            System.exit(1);
            return;
        }
        RedisServer server = new RedisServer(config);
        server.start();
        // SIGTERM (docker stop, systemctl stop) saves first when save rules are set, like Redis.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.shutdown(null), "shutdown"));
        server.awaitShutdown();
    }
}
