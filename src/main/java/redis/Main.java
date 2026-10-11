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
            System.err.println("usage: redis-java [--port N] [--replicaof \"host port\"] [--dir path] [--dbfilename name]");
            System.exit(1);
            return;
        }
        RedisServer server = new RedisServer(config);
        server.start();
        server.awaitShutdown();
    }
}
