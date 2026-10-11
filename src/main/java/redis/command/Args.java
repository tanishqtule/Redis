package redis.command;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Small helpers for pulling typed values out of raw arguments.
 *
 * Keys are kept as Strings decoded with ISO-8859-1. That charset maps every
 * byte to exactly one char and back, so keys stay binary-safe while still
 * being cheap to hash and compare.
 */
public final class Args {

    private Args() {
    }

    public static String str(byte[] raw) {
        return new String(raw, StandardCharsets.ISO_8859_1);
    }

    public static String str(List<byte[]> args, int i) {
        return str(args.get(i));
    }

    public static byte[] raw(String s) {
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    /** Command names and option keywords are case-insensitive. ASCII-only upper-casing is enough. */
    public static String upper(byte[] raw) {
        char[] chars = new char[raw.length];
        for (int i = 0; i < raw.length; i++) {
            char c = (char) (raw[i] & 0xff);
            chars[i] = c >= 'a' && c <= 'z' ? (char) (c - 32) : c;
        }
        return new String(chars);
    }

    public static long parseLong(byte[] raw) {
        String s = str(raw);
        // Long.parseLong would also accept "+5"; Redis doesn't.
        if (s.isEmpty() || s.charAt(0) == '+' || s.length() > 20) {
            throw CommandException.notInteger();
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw CommandException.notInteger();
        }
    }
}
