package redis.server;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import redis.command.CommandTable;
import redis.protocol.ProtocolException;
import redis.protocol.Resp;
import redis.protocol.RespReader;

/** Read-dispatch-reply loop for one client. Runs on its own virtual thread. */
final class Connection implements Runnable {

    private static final Logger log = Logger.getLogger(Connection.class.getName());

    /** Before AUTH a client can't send more than this, so it can't make us buffer much. */
    private static final int UNAUTHENTICATED_MAX_ARGS = 10;
    private static final int UNAUTHENTICATED_MAX_BULK = 16 * 1024;
    private static final int MAX_ARGS = 1024 * 1024;

    private final Socket socket;
    private final CommandTable commands;
    private final Config config;
    private final Consumer<Session> onOpen;
    private final Consumer<Session> onClose;

    Connection(Socket socket, CommandTable commands, Config config, Consumer<Session> onOpen, Consumer<Session> onClose) {
        this.socket = socket;
        this.commands = commands;
        this.config = config;
        this.onOpen = onOpen;
        this.onClose = onClose;
    }

    @Override
    public void run() {
        Session session = null;
        try (socket) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(config.timeoutSeconds() * 1000);
            RespReader reader = new RespReader(socket.getInputStream());
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());
            session = new Session(socket.getRemoteSocketAddress().toString(), out, false);
            session.setCloser(this::closeQuietly);
            onOpen.accept(session);

            while (!session.closeRequested()) {
                if (session.authenticated() || config.requirePass() == null) {
                    reader.setLimits(MAX_ARGS, config.protoMaxBulkLen(), false);
                } else {
                    reader.setLimits(UNAUTHENTICATED_MAX_ARGS, UNAUTHENTICATED_MAX_BULK, true);
                }
                List<byte[]> args;
                try {
                    args = reader.readCommand();
                } catch (ProtocolException e) {
                    if (!session.isReplica()) {
                        out.write(Resp.error("ERR Protocol error: " + e.getMessage()));
                        out.flush();
                    }
                    break;
                }
                if (args == null) {
                    break;
                }
                if (args.isEmpty()) {
                    continue;
                }

                session.commandStarted(args.get(0));
                byte[] reply = commands.dispatch(session, args);
                if (session.isReplica()) {
                    // The replication stream is long-lived and the replica acks on
                    // its own schedule, so the idle timeout doesn't apply to it.
                    socket.setSoTimeout(0);
                    // The replication writer owns the socket's output from here on.
                    continue;
                }
                if (reply != null) {
                    out.write(reply);
                }
                // With pipelining, hold replies until we've drained what the client already sent.
                if (!reader.hasBufferedInput() || session.closeRequested()) {
                    out.flush();
                }
            }
        } catch (SocketTimeoutException e) {
            log.log(Level.FINE, "closing idle connection");
        } catch (IOException e) {
            log.log(Level.FINE, "connection dropped", e);
        } finally {
            if (session != null) {
                onClose.accept(session);
            }
        }
    }

    private void closeQuietly() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Already closed or broken; either way it's gone.
        }
    }
}
