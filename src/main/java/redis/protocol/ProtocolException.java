package redis.protocol;

import java.io.IOException;

/** The peer sent something that isn't valid RESP. The connection can't be trusted after this. */
public class ProtocolException extends IOException {
    public ProtocolException(String message) {
        super(message);
    }
}
