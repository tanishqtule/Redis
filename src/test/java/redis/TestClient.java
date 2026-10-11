package redis;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import redis.protocol.Resp;

/**
 * Bare-bones blocking client for tests. Replies come back as plain Java
 * values: String, Long, List, null, or {@link Err}.
 */
public final class TestClient implements AutoCloseable {

    public record Err(String message) {
    }

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    public TestClient(int port) throws IOException {
        socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(10_000);
        in = new BufferedInputStream(socket.getInputStream());
        out = socket.getOutputStream();
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
        int type = in.read();
        String line = line();
        return switch (type) {
            case '+' -> line;
            case '-' -> new Err(line);
            case ':' -> Long.parseLong(line);
            case '$' -> {
                int len = Integer.parseInt(line);
                if (len < 0) {
                    yield null;
                }
                byte[] data = in.readNBytes(len);
                in.readNBytes(2);
                yield new String(data, StandardCharsets.UTF_8);
            }
            case '*' -> {
                int len = Integer.parseInt(line);
                if (len < 0) {
                    yield null;
                }
                List<Object> items = new ArrayList<>();
                for (int i = 0; i < len; i++) {
                    items.add(read());
                }
                yield items;
            }
            default -> throw new IOException("unexpected reply type " + type);
        };
    }

    private String line() throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != '\r') {
            if (b == -1) {
                throw new IOException("connection closed");
            }
            sb.append((char) b);
        }
        in.read();
        return sb.toString();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
