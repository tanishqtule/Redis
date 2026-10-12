package redis.server;

/** Helpers for putting text that came off the network into log lines. */
public final class Logs {

    private static final int MAX = 300;

    private Logs() {
    }

    /**
     * Replaces control characters and cuts long text, so a peer can't forge
     * extra log lines or flood the log through an error message it controls.
     */
    public static String safe(String text) {
        if (text == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(Math.min(text.length(), MAX + 3));
        for (int i = 0; i < text.length() && sb.length() < MAX; i++) {
            char c = text.charAt(i);
            sb.append(c < 0x20 || c == 0x7f ? '?' : c);
        }
        if (text.length() > MAX) {
            sb.append("...");
        }
        return sb.toString();
    }
}
