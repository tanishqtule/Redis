package redis.store;

/**
 * Redis-style glob matching for KEYS: {@code *}, {@code ?}, {@code [abc]},
 * {@code [^a-z]} and backslash escapes.
 */
public final class Glob {

    private Glob() {
    }

    public static boolean matches(String pattern, String text) {
        return match(pattern, 0, text, 0);
    }

    private static boolean match(String p, int pi, String s, int si) {
        while (pi < p.length()) {
            char c = p.charAt(pi);
            if (c == '*') {
                while (pi + 1 < p.length() && p.charAt(pi + 1) == '*') {
                    pi++;
                }
                if (pi + 1 == p.length()) {
                    return true;
                }
                for (int k = si; k <= s.length(); k++) {
                    if (match(p, pi + 1, s, k)) {
                        return true;
                    }
                }
                return false;
            }
            if (si >= s.length()) {
                return false;
            }
            if (c == '?') {
                pi++;
                si++;
            } else if (c == '[') {
                pi = matchClass(p, pi + 1, s.charAt(si));
                if (pi < 0) {
                    return false;
                }
                si++;
            } else {
                if (c == '\\' && pi + 1 < p.length()) {
                    c = p.charAt(++pi);
                }
                if (s.charAt(si) != c) {
                    return false;
                }
                pi++;
                si++;
            }
        }
        return si == s.length();
    }

    /** Returns the pattern index just past the class, or -1 if {@code ch} isn't in it. */
    private static int matchClass(String p, int pi, char ch) {
        boolean negate = pi < p.length() && p.charAt(pi) == '^';
        if (negate) {
            pi++;
        }
        boolean found = false;
        while (pi < p.length() && p.charAt(pi) != ']') {
            char c = p.charAt(pi);
            if (c == '\\' && pi + 1 < p.length()) {
                found |= p.charAt(pi + 1) == ch;
                pi += 2;
            } else if (pi + 2 < p.length() && p.charAt(pi + 1) == '-' && p.charAt(pi + 2) != ']') {
                char lo = c;
                char hi = p.charAt(pi + 2);
                if (lo > hi) {
                    char t = lo;
                    lo = hi;
                    hi = t;
                }
                found |= ch >= lo && ch <= hi;
                pi += 3;
            } else {
                found |= c == ch;
                pi++;
            }
        }
        if (found == negate) {
            return -1;
        }
        // An unterminated class just runs to the end of the pattern, like in Redis.
        return pi < p.length() ? pi + 1 : pi;
    }
}
