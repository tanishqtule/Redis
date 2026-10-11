package redis.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import redis.command.Args;
import redis.command.CommandTable;
import redis.command.KeyCommands;
import redis.command.ServerCommands;
import redis.persistence.Rdb;
import redis.replication.MasterLink;
import redis.replication.ReplicationCommands;
import redis.replication.ReplicationMaster;
import redis.store.Keyspace;

public final class RedisServer implements AutoCloseable {

    private static final Logger log = Logger.getLogger(RedisServer.class.getName());

    private final Config config;
    private final ReentrantLock lock = new ReentrantLock();
    private final Keyspace db = new Keyspace();
    private final CommandTable commands;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final ScheduledExecutorService housekeeping = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "housekeeping");
        t.setDaemon(true);
        return t;
    });

    /** Exactly one of these is non-null, depending on the role. */
    private final ReplicationMaster replication;
    private final MasterLink masterLink;

    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private ServerSocket listener;

    public RedisServer(Config config) {
        this.config = config;
        this.commands = new CommandTable(lock, config.isReplica());

        if (config.isReplica()) {
            replication = null;
            masterLink = new MasterLink(config.masterHost(), config.masterPort(), this::port, lock, db, commands);
            db.setOwnsExpiry(false);
        } else {
            replication = new ReplicationMaster(lock);
            masterLink = null;
            commands.setReplicationSink(replication::propagate);
            // Replicas never expire keys on their own, so tell them explicitly.
            db.onExpire(key -> commands.propagate(List.of(Args.raw("DEL"), Args.raw(key))));
        }

        new ServerCommands(config, db, replication != null ? replication::info : masterLink::info)
                .registerAll(commands);
        new KeyCommands(db).registerAll(commands);
        new ReplicationCommands(replication, db).registerAll(commands);
    }

    public void start() throws IOException {
        if (!config.isReplica()) {
            loadSnapshot();
        }
        listener = new ServerSocket();
        listener.setReuseAddress(true);
        try {
            listener.bind(new InetSocketAddress(config.port()));
        } catch (IOException e) {
            listener.close();
            throw e;
        }
        log.info("listening on port " + port());

        Thread.ofPlatform().name("accept").start(this::acceptLoop);
        if (masterLink != null) {
            Thread.ofVirtual().name("master-link").start(masterLink);
        } else {
            housekeeping.scheduleWithFixedDelay(this::expireKeys, 100, 100, TimeUnit.MILLISECONDS);
        }
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
        housekeeping.shutdownNow();
        if (masterLink != null) {
            masterLink.stop();
        }
        listener.close();
        for (Socket client : clients) {
            client.close();
        }
        stopped.countDown();
    }

    private void acceptLoop() {
        // One virtual thread per client: blocking socket code stays simple and
        // we don't run out of threads with thousands of idle connections.
        while (!listener.isClosed()) {
            try {
                Socket socket = listener.accept();
                clients.add(socket);
                Thread.ofVirtual().name("client-" + socket.getPort())
                        .start(new Connection(socket, commands, session -> onDisconnect(socket, session)));
            } catch (IOException e) {
                if (!listener.isClosed()) {
                    log.warning("accept failed: " + e.getMessage());
                }
            }
        }
    }

    /** A replica skips this: whatever it has on disk is stale, the master will send a fresh copy. */
    private void loadSnapshot() throws IOException {
        Path path = config.rdbPath();
        if (!Files.exists(path)) {
            return;
        }
        long now = db.now();
        int[] loaded = {0};
        Rdb.read(Files.readAllBytes(path), (key, value, expireAt) -> {
            if (expireAt == Keyspace.NO_EXPIRY || expireAt > now) {
                db.set(key, value, expireAt);
                loaded[0]++;
            }
        });
        log.info("loaded " + loaded[0] + " keys from " + path);
    }

    private void expireKeys() {
        lock.lock();
        try {
            // Keep going while there's a backlog, but give clients a turn between batches.
            while (db.expireCycle(200) == 200) {
                lock.unlock();
                lock.lock();
            }
        } finally {
            lock.unlock();
        }
    }

    private void onDisconnect(Socket socket, Session session) {
        clients.remove(socket);
        if (replication != null && session.isReplica()) {
            replication.remove(session);
        }
    }
}
