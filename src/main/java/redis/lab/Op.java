package redis.lab;

/**
 * One operation as a client saw it: what it asked, what came back, and when
 * it was sent and answered (System.nanoTime). Linearizability is judged on
 * exactly this: the real-time window and the result, nothing else.
 *
 * @param value  the value written by SET, otherwise null
 * @param result GET: the value read or null; SET: "OK"; INCR: the new count (Long)
 */
public record Op(int client, Kind kind, String key, String value, Object result, long call, long ret) {

    public enum Kind { GET, SET, INCR }

    public boolean isRead() {
        return kind == Kind.GET;
    }

    @Override
    public String toString() {
        return switch (kind) {
            case GET -> "GET " + key + " -> " + quote(result);
            case SET -> "SET " + key + " " + quote(value);
            case INCR -> "INCR " + key + " -> " + result;
        };
    }

    static String quote(Object v) {
        return v == null ? "(nil)" : "\"" + v + "\"";
    }
}
