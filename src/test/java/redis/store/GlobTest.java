package redis.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Random;

import org.junit.jupiter.api.Test;

class GlobTest {

    @Test
    void star() {
        assertTrue(Glob.matches("*", ""));
        assertTrue(Glob.matches("*", "anything"));
        assertTrue(Glob.matches("user:*", "user:42"));
        assertTrue(Glob.matches("*:42", "user:42"));
        assertTrue(Glob.matches("a*b*c", "aXXbYYc"));
        assertFalse(Glob.matches("user:*", "session:1"));
    }

    @Test
    void questionMark() {
        assertTrue(Glob.matches("h?llo", "hello"));
        assertFalse(Glob.matches("h?llo", "hllo"));
    }

    @Test
    void characterClasses() {
        assertTrue(Glob.matches("h[ae]llo", "hallo"));
        assertFalse(Glob.matches("h[ae]llo", "hillo"));
        assertTrue(Glob.matches("h[^e]llo", "hallo"));
        assertFalse(Glob.matches("h[^e]llo", "hello"));
        assertTrue(Glob.matches("key[0-9]", "key7"));
        assertFalse(Glob.matches("key[0-9]", "keyx"));
    }

    @Test
    void unterminatedClassesAndTrailingBackslash() {
        assertTrue(Glob.matches("a[bc", "ab"));
        assertFalse(Glob.matches("[", "a"));
        assertTrue(Glob.matches("a\\", "a\\"));
    }

    /** The pattern that hung the old recursive matcher for minutes. */
    @Test
    void pathologicalPatternsFinishQuickly() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            assertFalse(Glob.matches("*a".repeat(14) + "b", "a".repeat(40)));
            assertFalse(Glob.matches("*a".repeat(2000) + "b", "a".repeat(5000)));
            assertTrue(Glob.matches("*a".repeat(2000), "a".repeat(5000)));
        });
    }

    /**
     * The new matcher against the old recursive one, which is correct but
     * exponential, on lots of small random inputs where it's still fast.
     */
    @Test
    void agreesWithTheOldRecursiveMatcher() {
        Random random = new Random(99);
        String patternChars = "ab*?[]^-\\";
        String textChars = "ab-*[]^\\";
        for (int i = 0; i < 200_000; i++) {
            String pattern = randomString(random, patternChars, random.nextInt(8));
            String text = randomString(random, textChars, random.nextInt(8));
            assertEquals(Reference.matches(pattern, text), Glob.matches(pattern, text),
                    "pattern " + pattern + " text " + text);
        }
    }

    private static String randomString(Random random, String alphabet, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return sb.toString();
    }

    /** The original backtracking implementation, kept here only as a reference. */
    private static final class Reference {
        static boolean matches(String pattern, String text) {
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
            return pi < p.length() ? pi + 1 : pi;
        }
    }

    @Test
    void escapes() {
        assertTrue(Glob.matches("a\\*b", "a*b"));
        assertFalse(Glob.matches("a\\*b", "aXb"));
    }
}
