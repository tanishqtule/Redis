package redis.replication;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.logging.Logger;

import redis.command.Args;
import redis.command.CommandTable;
import redis.persistence.Rdb;
import redis.protocol.Resp;
import redis.protocol.RespReader;
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

    private final String host;
    private final int port;
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
    private volatile Socket socket;

    public MasterLink(String host, int port, IntSupplier listeningPort,
                      ReentrantLock commandLock, Keyspace db, CommandTable commands) {
        this.host = host;
        this.port = port;
        this.listeningPort = listeningPort;
        this.commandLock = commandLock;
        this.db = db;
        this.commands = commands;
    }

    @Override
    public void run() {
        while (running) {
            try {
                syncAndFollow();
            } catch (IOException e) {
                if (running) {
                    log.warning("replication link to " + host + ":" + port + " failed: " + e.getMessage());
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

    public List<String> info() {
        List<String> lines = new ArrayList<>();
        lines.add("role:slave");
        lines.add("master_host:" + host);
        lines.add("master_port:" + port);
        lines.add("master_link_status:" + (isLinkUp() ? "up" : "down"));
        lines.add("slave_repl_offset:" + offset);
        lines.add("connected_slaves:0");
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
