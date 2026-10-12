package redis.replication;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.logging.Logger;

import redis.command.Args;
import redis.command.CommandException;
import redis.command.CommandTable;
import redis.persistence.Rdb;
import redis.protocol.Resp;
import redis.protocol.RespReader;
import redis.server.Config;
import redis.server.Logs;
import redis.server.Session;
import redis.store.Keyspace;

/**
 * Replica side: connects to the master, does the handshake, loads the
 * snapshot, then applies the command stream. Reconnects with a full resync
 * whenever the link drops.
 */
public final class MasterLink implements Runnable {

    private static final Logger log = Logger.getLogger(MasterLink.class.getName());
    private static final long RETRY_DELAY_MS = 1000;
    private static final long ACK_INTERVAL_MS = 1000;
    private static final int LINEARIZABLE_READ_TIMEOUT_MS = 1000;

    private final String host;
    private final int port;
    private final String masterAuth;
    private final IntSupplier listeningPort;
    private final ReentrantLock commandLock;
    private final Keyspace db;
    private final CommandTable commands;

    /** Link states, named as Redis' ROLE command reports them. */
    private enum State {
        CONNECT, CONNECTING, HANDSHAKE, SYNC, CONNECTED;

        String label() {
            return name().toLowerCase();
        }
    }

    private volatile boolean running = true;
    private volatile State state = State.CONNECT;
    /**
     * How far into the master's replication stream we've applied, in the
     * master's own numbering: it starts from the offset in the FULLRESYNC
     * reply, not from zero. -1 until the first sync.
     */
    private volatile long offset = -1;
    /** The replication id from the last FULLRESYNC: which history {@code offset} counts in. */
    private volatile String masterReplId;
    private volatile Socket socket;

    private final ReadIndex readIndex;
    private final AtomicLong linearizableReads = new AtomicLong();

    private final Object progress = new Object();
    private volatile int waiters;

    public MasterLink(Config config, IntSupplier listeningPort,
                      ReentrantLock commandLock, Keyspace db, CommandTable commands) {
        this.host = config.masterHost();
        this.port = config.masterPort();
        this.masterAuth = config.masterAuth();
        this.listeningPort = listeningPort;
        this.commandLock = commandLock;
        this.db = db;
        this.commands = commands;
        this.readIndex = new ReadIndex(host, port, LINEARIZABLE_READ_TIMEOUT_MS, masterAuth);
    }

    @Override
    public void run() {
        while (running) {
            try {
                syncAndFollow();
            } catch (IOException | RuntimeException e) {
                // Whatever the master sent, the link must come back: a replica
                // that silently stops retrying is worse than one that keeps failing.
                if (running) {
                    log.warning("replication link to " + host + ":" + port + " failed: " + Logs.safe(e.toString()));
                }
            } finally {
                state = State.CONNECT;
            }
            if (running) {
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    public void stop() throws IOException {
        running = false;
        readIndex.close();
        Socket s = socket;
        if (s != null) {
            s.close();
        }
    }

    public boolean isLinkUp() {
        return state == State.CONNECTED;
    }

    public long offset() {
        return offset;
    }

    /**
     * Blocks until this replica has applied the master's stream up to
     * {@code target}, or the timeout passes (0 = no timeout). Returns the
     * offset reached, so the caller can tell which of the two happened.
     */
    public long awaitOffset(long target, long timeoutMillis) throws InterruptedException {
        long reached = offset;
        if (reached >= target) {
            return reached;
        }
        long deadline = Timeouts.deadlineAfter(timeoutMillis);
        synchronized (progress) {
            waiters++;
            try {
                while ((reached = offset) < target) {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        break;
                    }
                    progress.wait(remaining);
                }
            } finally {
                waiters--;
            }
        }
        return reached;
    }

    /**
     * Holds a read back until this replica has caught up with everything the
     * master had applied when the read arrived, which makes the read
     * linearizable: it can't return anything older than a write that
     * finished before it started.
     *
     * Costs one small request to the master (shared between concurrent
     * readers) plus however long the replica takes to catch up. Throws a
     * MASTERDOWN or TRYAGAIN error rather than ever answering stale.
     */
    public void awaitLinearizable() {
        if (!isLinkUp()) {
            throw new CommandException("MASTERDOWN Link with MASTER is down, so a linearizable read can't be served");
        }
        long started = System.currentTimeMillis();
        ReadIndex.Position index;
        try {
            index = readIndex.fetch(LINEARIZABLE_READ_TIMEOUT_MS);
        } catch (TimeoutException e) {
            throw new CommandException("MASTERDOWN master didn't report its offset within "
                    + LINEARIZABLE_READ_TIMEOUT_MS + " ms");
        } catch (IOException e) {
            throw new CommandException("MASTERDOWN couldn't ask the master for its offset: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandException("ERR interrupted while waiting for the master");
        }
        if (!index.replId().equals(masterReplId)) {
            // The master has a different history than the one we copied (it
            // restarted, say), so our offset says nothing about freshness.
            throw new CommandException("TRYAGAIN replica is still following an old replication history");
        }
        long remaining = Math.max(1, LINEARIZABLE_READ_TIMEOUT_MS - (System.currentTimeMillis() - started));
        long reached;
        try {
            reached = awaitOffset(index.offset(), remaining);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandException("ERR interrupted while waiting for the replication offset");
        }
        if (!index.replId().equals(masterReplId)) {
            throw new CommandException("TRYAGAIN replica resynced with a different history while waiting");
        }
        if (reached < index.offset()) {
            throw new CommandException("TRYAGAIN replica is at offset " + reached + " but the read needs "
                    + index.offset() + "; it didn't catch up within " + LINEARIZABLE_READ_TIMEOUT_MS + " ms");
        }
        linearizableReads.incrementAndGet();
    }

    public List<String> info() {
        List<String> lines = new ArrayList<>();
        lines.add("role:slave");
        lines.add("master_host:" + host);
        lines.add("master_port:" + port);
        lines.add("master_link_status:" + (isLinkUp() ? "up" : "down"));
        lines.add("slave_repl_offset:" + offset);
        lines.add("connected_slaves:0");
        lines.add("linearizable_reads:" + linearizableReads.get());
        lines.add("read_index_round_trips:" + readIndex.roundTrips());
        return lines;
    }

    /** ROLE reply, in the same shape Redis uses: slave, host, port, link state, offset. */
    public byte[] role() {
        return Resp.array(List.of(
                Resp.bulk("slave"),
                Resp.bulk(host),
                Resp.integer(port),
                Resp.bulk(state.label()),
                Resp.integer(offset)));
    }

    private void syncAndFollow() throws IOException {
        try (Socket s = new Socket()) {
            socket = s;
            state = State.CONNECTING;
            s.connect(new InetSocketAddress(host, port), 5000);
            s.setTcpNoDelay(true);
            RespReader reader = new RespReader(s.getInputStream());
            OutputStream out = new BufferedOutputStream(s.getOutputStream());

            state = State.HANDSHAKE;
            long startOffset = handshake(reader, out);
            state = State.SYNC;
            loadSnapshot(reader.readRdbPayload());
            advanceTo(startOffset);
            state = State.CONNECTED;
            log.info("synced with master " + host + ":" + port + " at offset " + startOffset);

            Thread acker = Thread.ofVirtual().name("replica-acker").start(() -> ackPeriodically(out));
            try {
                follow(reader, out);
            } finally {
                acker.interrupt();
            }
        }
    }

    /** Returns the offset the master says the snapshot corresponds to. */
    private long handshake(RespReader reader, OutputStream out) throws IOException {
        if (masterAuth != null) {
            send(out, Resp.command("AUTH", masterAuth));
            reader.readSimpleString();
        }
        send(out, Resp.command("PING"));
        reader.readSimpleString();
        send(out, Resp.command("REPLCONF", "listening-port", Integer.toString(listeningPort.getAsInt())));
        reader.readSimpleString();
        send(out, Resp.command("REPLCONF", "capa", "psync2"));
        reader.readSimpleString();
        send(out, Resp.command("PSYNC", "?", "-1"));
        String reply = reader.readSimpleString();
        String[] parts = reply.split(" ");
        if (parts.length != 3 || !parts[0].equals("FULLRESYNC")) {
            throw new IOException("expected FULLRESYNC <replid> <offset>, master said: " + reply);
        }
        masterReplId = parts[1];
        try {
            return Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            throw new IOException("bad offset in FULLRESYNC reply: " + reply);
        }
    }

    private void loadSnapshot(byte[] snapshot) throws IOException {
        commandLock.lock();
        try {
            db.clear();
            Rdb.read(snapshot, db::set);
        } finally {
            commandLock.unlock();
        }
    }

    private void follow(RespReader reader, OutputStream out) throws IOException {
        Session fromMaster = new Session(host + ":" + port, OutputStream.nullOutputStream(), true);
        while (running) {
            long before = reader.consumed();
            List<byte[]> args = reader.readCommand();
            if (args == null) {
                throw new IOException("master closed the connection");
            }
            if (isGetAck(args)) {
                // The reply covers what we'd applied *before* this GETACK.
                send(out, ack());
            } else if (!args.isEmpty()) {
                // Replies to the master's stream are thrown away, as in Redis.
                commands.dispatch(fromMaster, args);
            }
            advanceTo(offset + reader.consumed() - before);
        }
    }

    private void advanceTo(long newOffset) {
        offset = newOffset;
        // Only pay for the monitor when someone is actually waiting (WAITOFFSET, linearizable reads).
        if (waiters > 0) {
            synchronized (progress) {
                progress.notifyAll();
            }
        }
    }

    // Real masters drop replicas that stay quiet for repl-timeout (60 s), so
    // report progress every second like a Redis replica does.
    private void ackPeriodically(OutputStream out) {
        try {
            while (true) {
                Thread.sleep(ACK_INTERVAL_MS);
                send(out, ack());
            }
        } catch (InterruptedException | IOException e) {
            // Link is going away; syncAndFollow deals with it.
        }
    }

    private byte[] ack() {
        return Resp.command("REPLCONF", "ACK", Long.toString(offset));
    }

    private static boolean isGetAck(List<byte[]> args) {
        return args.size() >= 2
                && Args.upper(args.get(0)).equals("REPLCONF")
                && Args.upper(args.get(1)).equals("GETACK");
    }

    private static void send(OutputStream out, byte[] bytes) throws IOException {
        synchronized (out) {
            out.write(bytes);
            out.flush();
        }
    }
}
