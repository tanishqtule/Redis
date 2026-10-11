package redis.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void escapes() {
        assertTrue(Glob.matches("a\\*b", "a*b"));
        assertFalse(Glob.matches("a\\*b", "aXb"));
    }
}
