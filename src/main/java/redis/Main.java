package redis;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import redis.server.Config;
import redis.server.RedisServer;
import sun.misc.Signal;

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
        handleStopSignals(server);
        server.awaitShutdown();
    }

    /**
     * SIGTERM (docker stop, systemctl stop) and Ctrl-C shut down the way
     * Redis does: save first when save rules are set, then exit 0. If that
     * save fails, log it and keep running rather than throw the data away.
     * A second Ctrl-C, after the first one's save failed or while it's still
     * running, means "I know, stop anyway" and exits at once without saving.
     *
     * The signals are handled here instead of in a shutdown hook because a
     * JVM that has started shutting down can't change its mind, and because
     * java.util.logging closes its handlers in a hook of its own, so whatever
     * a hook logs, a failed save included, is usually lost.
     */
    private static void handleStopSignals(RedisServer server) {
        Logger log = Logger.getLogger(Main.class.getName());
        AtomicBoolean stopping = new AtomicBoolean();
        AtomicBoolean interrupted = new AtomicBoolean();
        for (String name : new String[] {"TERM", "INT"}) {
            try {
                Signal.handle(new Signal(name), signal -> {
                    if (name.equals("INT") && interrupted.getAndSet(true)) {
                        log.warning("You insist... exiting now.");
                        Runtime.getRuntime().halt(1);
                    }
                    if (!stopping.compareAndSet(false, true)) {
                        return; // a shutdown is already under way
                    }
                    try {
                        log.info("Received SIG" + name + ", scheduling shutdown...");
                        if (server.shutdown(null)) {
                            System.exit(0);
                        }
                        log.warning("SIG" + name + " received but errors trying to shut down the server, check the logs for more information");
                    } finally {
                        // Even when shutting down threw, the next signal must get to try again.
                        stopping.set(false);
                    }
                });
            } catch (IllegalArgumentException unavailable) {
                // Not on this platform, or the JVM runs with -Xrs. The hook below still saves.
            }
        }
        // Any other way out of the JVM (System.exit, a signal we don't handle) still saves.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.shutdown(null), "shutdown"));
    }
}
