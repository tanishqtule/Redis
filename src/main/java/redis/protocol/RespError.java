package redis.protocol;

/** An error reply, e.g. "ERR syntax error". Returned as a value rather than thrown, like other replies. */
public record RespError(String message) {

    /** The error code: the first word, such as ERR, READONLY or MASTERDOWN. */
    public String code() {
        int space = message.indexOf(' ');
        return space < 0 ? message : message.substring(0, space);
    }
}
