package redis.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
import redis.protocol.Resp;
import redis.replication.MasterLink;
import redis.replication.ReplicationCommands;
import redis.replication.ReplicationMaster;
import redis.store.Keyspace;

public final class RedisServer implements AutoCloseable {

    private static final Logger log = Logger.getLogger(RedisServer.class.getName());

    private static final byte[] DENIED = Resp.error("DENIED redis-java is running in protected mode: no password is set,"
            + " so only connections from this machine (loopback) are accepted. Either set a password"
            + " (requirepass, and use AUTH), or if the network is trusted, restart with --protected-mode no.");
    private static final byte[] TOO_MANY_CLIENTS = Resp.error("ERR max number of clients reached");

    private final Config config;
    private final ReentrantLock lock = new ReentrantLock();
    private final Keyspace db = new Keyspace();
    private final CommandTable commands;
    private final Stats stats = new Stats();
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
    private final Map<Long, Session> sessions = new ConcurrentHashMap<>();
    private final List<ServerSocket> listeners = new ArrayList<>();
    private volatile long lastAcceptWarning;

    public RedisServer(Config config) {
        this.config = config;
        this.commands = new CommandTable(lock, config.isReplica());
        commands.setAuthRequired(config.requirePass() != null);

        if (config.isReplica()) {
            replication = null;
            masterLink = new MasterLink(config, this::port, lock, db, commands);
            db.setOwnsExpiry(false);
            commands.setLinearizableReadBarrier(masterLink::awaitLinearizable);
        } else {
            replication = new ReplicationMaster(lock, config.simulatedReplicationLagMs(), config.replicaOutputLimit());
            if (config.simulatedReplicationLagMs() > 0) {
                log.warning("simulating " + config.simulatedReplicationLagMs() + " ms of replication lag");
            }
            masterLink = null;
            commands.setReplicationSink(replication::propagate);
            // Replicas never expire keys on their own, so tell them explicitly.
            db.onExpire(key -> commands.propagate(List.of(Args.raw("DEL"), Args.raw(key))));
        }

        new ServerCommands(config, db, replication != null ? replication::info : masterLink::info)
                .registerAll(commands);
        new KeyCommands(db, config.protoMaxBulkLen()).registerAll(commands);
        new ReplicationCommands(replication, masterLink, db).registerAll(commands);
    }

    public void start() throws IOException {
        if (!config.isReplica()) {
            loadSnapshot();
        }
        try {
            if (config.bind().isEmpty()) {
                listeners.add(listen(null, config.port()));
            } else {
                for (String address : config.bind()) {
                    listeners.add(listen(address.equals("*") ? null : InetAddress.getByName(address), config.port()));
                }
            }
        } catch (IOException e) {
            for (ServerSocket listener : listeners) {
                listener.close();
            }
            throw e;
        }
        warnAboutExposure();

        for (ServerSocket listener : listeners) {
            Thread.ofPlatform().name("accept-" + listener.getLocalSocketAddress()).start(() -> acceptLoop(listener));
        }
        if (masterLink != null) {
            Thread.ofVirtual().name("master-link").start(masterLink);
        } else {
            housekeeping.scheduleWithFixedDelay(this::expireKeys, 100, 100, TimeUnit.MILLISECONDS);
        }
    }

    /** The bound port of the first listener, which differs from the configured one when that was 0. */
    public int port() {
        return listeners.get(0).getLocalPort();
    }

    public Stats stats() {
        return stats;
    }

    /** Connected clients, for CLIENT LIST and INFO. */
    public Collection<Session> sessions() {
        return sessions.values();
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
        for (ServerSocket listener : listeners) {
            listener.close();
        }
        for (Socket client : clients) {
            client.close();
        }
        stopped.countDown();
    }

    private ServerSocket listen(InetAddress address, int port) throws IOException {
        ServerSocket listener = new ServerSocket();
        listener.setReuseAddress(true);
        try {
            listener.bind(new InetSocketAddress(address, port));
        } catch (IOException e) {
            listener.close();
            throw new IOException("can't listen on " + (address == null ? "*" : address.getHostAddress()) + ":" + port
                    + ": " + e.getMessage(), e);
        }
        log.info("listening on " + listener.getLocalSocketAddress());
        return listener;
    }

    private void warnAboutExposure() {
        if (config.requirePass() != null) {
            return;
        }
        if (config.protectedMode()) {
            log.info("no password set: protected mode only accepts connections from this machine");
        } else {
            log.warning("no password and protected mode off: anyone who can reach this port can read and change"
                    + " all data. Only do this on a network you trust.");
        }
    }

    private void acceptLoop(ServerSocket listener) {
        // One virtual thread per client: blocking socket code stays simple and
        // we don't run out of threads with thousands of idle connections.
        while (!listener.isClosed()) {
            Socket socket;
            try {
                socket = listener.accept();
            } catch (IOException e) {
                if (!listener.isClosed()) {
                    // Typically out of file descriptors. Retrying at once would spin
                    // a core and write hundreds of thousands of log lines a second.
                    long now = System.currentTimeMillis();
                    if (now - lastAcceptWarning > 1000) {
                        lastAcceptWarning = now;
                        log.warning("accept failed: " + e.getMessage());
                    }
                    pause(100);
                }
                continue;
            }
            stats.connectionsReceived.incrementAndGet();
            if (config.protectedMode() && config.requirePass() == null && !socket.getInetAddress().isLoopbackAddress()) {
                reject(socket, DENIED);
            } else if (clients.size() >= config.maxClients()) {
                reject(socket, TOO_MANY_CLIENTS);
            } else {
                clients.add(socket);
                Thread.ofVirtual().name("client-" + socket.getPort()).start(
                        new Connection(socket, commands, config, this::onConnect, s -> onDisconnect(socket, s)));
            }
        }
    }

    /** Explains the refusal and hangs up. On its own thread, so a peer that doesn't read can't stall accepting. */
    private void reject(Socket socket, byte[] reason) {
        stats.rejectedConnections.incrementAndGet();
        Thread.ofVirtual().start(() -> {
            try (socket) {
                socket.setSoTimeout(1000);
                OutputStream out = socket.getOutputStream();
                out.write(reason);
                out.flush();
            } catch (IOException ignored) {
                // They're being turned away either way.
            }
        });
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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

    private void onConnect(Session session) {
        session.setAuthenticated(config.requirePass() == null);
        sessions.put(session.id(), session);
    }

    private void onDisconnect(Socket socket, Session session) {
        clients.remove(socket);
        sessions.remove(session.id());
        if (replication != null && session.isReplica()) {
            replication.remove(session);
        }
    }
}
