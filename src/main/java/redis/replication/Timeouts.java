package redis.replication;

/** Turning a client-supplied timeout into a deadline without overflowing. */
final class Timeouts {

    private Timeouts() {
    }

    /** 0 means wait forever, as in Redis; so does any timeout too large to add to the clock. */
    static long deadlineAfter(long timeoutMillis) {
        long now = System.currentTimeMillis();
        if (timeoutMillis == 0 || timeoutMillis > Long.MAX_VALUE - now) {
            return Long.MAX_VALUE;
        }
        return now + timeoutMillis;
    }
}
