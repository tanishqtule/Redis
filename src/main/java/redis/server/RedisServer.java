package redis.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import redis.command.CommandTable;
import redis.command.ServerCommands;

public final class RedisServer implements AutoCloseable {

    private static final Logger log = Logger.getLogger(RedisServer.class.getName());

    private final Config config;
    private final ReentrantLock lock = new ReentrantLock();
    private final CommandTable commands = new CommandTable(lock);
    private final CountDownLatch stopped = new CountDownLatch(1);

    private ServerSocket listener;

    public RedisServer(Config config) {
        this.config = config;
        new ServerCommands().registerAll(commands);
    }

    public void start() throws IOException {
        listener = new ServerSocket();
        listener.setReuseAddress(true);
        listener.bind(new InetSocketAddress(config.port()));
        log.info("listening on port " + port());

        Thread.ofPlatform().name("accept").start(this::acceptLoop);
    }

    /** The bound port, which differs from the configured one when that was 0. */
    public int port() {
        return listener.getLocalPort();
    }

    public void awaitShutdown() throws InterruptedException {
        stopped.await();
    }

    @Override
    public void close() throws IOException {
        listener.close();
        stopped.countDown();
    }

    private void acceptLoop() {
        // One virtual thread per client: blocking socket code stays simple and
        // we don't run out of threads with thousands of idle connections.
        while (!listener.isClosed()) {
            try {
                Socket socket = listener.accept();
                Thread.ofVirtual().name("client-" + socket.getPort())
                        .start(new Connection(socket, commands, this::onDisconnect));
            } catch (IOException e) {
                if (!listener.isClosed()) {
                    log.warning("accept failed: " + e.getMessage());
                }
            }
        }
    }

    private void onDisconnect(Session session) {
    }
}
