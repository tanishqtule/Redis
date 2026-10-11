package redis.protocol;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A minimal blocking client: send a command, read the reply. Used by
 * replicas to ask their master for its offset, by the consistency lab, and
 * by the tests.
 *
 * Bulk strings are decoded as UTF-8 Strings, which suits all of those uses;
 * error replies come back as {@link RespError} values instead of exceptions.
 */
public final class RespClient implements AutoCloseable {

    private final Socket socket;
    private final RespReader reader;
    private final OutputStream out;

    public RespClient(int port) throws IOException {
        this("127.0.0.1", port, 10_000);
    }

    public RespClient(String host, int port, int timeoutMillis) throws IOException {
        socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            socket.setTcpNoDelay(true);
            reader = new RespReader(socket.getInputStream());
            out = new BufferedOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }

    public Object call(String... args) throws IOException {
        send(args);
        return read();
    }

    public void send(String... args) throws IOException {
        out.write(Resp.command(args));
        out.flush();
    }

    public void sendRaw(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    public Object read() throws IOException {
        return decode(reader.readReply());
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    private static Object decode(Object reply) {
        if (reply instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (reply instanceof List<?> items) {
            List<Object> decoded = new ArrayList<>(items.size());
            for (Object item : items) {
                decoded.add(decode(item));
            }
            return decoded;
        }
        return reply;
    }
}
