package redis.store;

/**
 * Redis-style glob matching for KEYS, SCAN and CONFIG GET: {@code *},
 * {@code ?}, {@code [abc]}, {@code [^a-z]} and backslash escapes.
 *
 * Every element except {@code *} matches exactly one character, so when a
 * match fails it's enough to go back to the most recent star and let it
 * swallow one more character. Earlier stars never need revisiting, which
 * bounds the work at pattern length times text length. A recursive matcher
 * that retries every star is exponential: a 29-character pattern against a
 * 40-character key hung the server for minutes, the same class of bug as
 * Redis' CVE-2022-36021.
 */
public final class Glob {

    private Glob() {
    }

    public static boolean matches(String pattern, String text) {
        int p = 0;
        int s = 0;
        int starP = -1;
        int starS = -1;
        while (s < text.length()) {
            if (p < pattern.length()) {
                if (pattern.charAt(p) == '*') {
                    starP = p++;
                    starS = s;
                    continue;
                }
                int next = matchOne(pattern, p, text.charAt(s));
                if (next >= 0) {
                    p = next;
                    s++;
                    continue;
                }
            }
            if (starP < 0) {
                return false;
            }
            // Let the last star take one more character and try again from there.
            p = starP + 1;
            s = ++starS;
        }
        while (p < pattern.length() && pattern.charAt(p) == '*') {
            p++;
        }
        return p == pattern.length();
    }

    /** If the element at {@code p} matches {@code ch}, the index of the next element; otherwise -1. */
    private static int matchOne(String pattern, int p, char ch) {
        char c = pattern.charAt(p);
        if (c == '?') {
            return p + 1;
        }
        if (c == '[') {
            return matchClass(pattern, p + 1, ch);
        }
        if (c == '\\' && p + 1 < pattern.length()) {
            return pattern.charAt(p + 1) == ch ? p + 2 : -1;
        }
        return c == ch ? p + 1 : -1;
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
