package redis.server;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
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

    private final Socket socket;
    private final CommandTable commands;
    private final Consumer<Session> onClose;

    Connection(Socket socket, CommandTable commands, Consumer<Session> onClose) {
        this.socket = socket;
        this.commands = commands;
        this.onClose = onClose;
    }

    @Override
    public void run() {
        Session session = null;
        try (socket) {
            socket.setTcpNoDelay(true);
            RespReader reader = new RespReader(socket.getInputStream());
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());
            session = new Session(socket.getRemoteSocketAddress().toString(), out, false);

            while (!session.closeRequested()) {
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

                byte[] reply = commands.dispatch(session, args);
                if (session.isReplica()) {
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
        } catch (IOException e) {
            log.log(Level.FINE, "connection dropped", e);
        } finally {
            if (session != null) {
                onClose.accept(session);
            }
        }
    }
}
