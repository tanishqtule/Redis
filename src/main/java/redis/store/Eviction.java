package redis.store;

import java.util.Locale;

/** What to do when stored data passes maxmemory. */
public final class Eviction {

    private Eviction() {
    }

    public enum Policy {
        /** Refuse commands that would add data, with an OOM error. Redis' default. */
        NOEVICTION("noeviction", false),
        ALLKEYS_LRU("allkeys-lru", false),
        ALLKEYS_RANDOM("allkeys-random", false),
        /** Only keys with a TTL are candidates. */
        VOLATILE_LRU("volatile-lru", true),
        VOLATILE_RANDOM("volatile-random", true),
        /** Keys closest to expiring go first. */
        VOLATILE_TTL("volatile-ttl", true);

        private final String configName;
        private final boolean volatileOnly;

        Policy(String configName, boolean volatileOnly) {
            this.configName = configName;
            this.volatileOnly = volatileOnly;
        }

        public String configName() {
            return configName;
        }

        public boolean volatileOnly() {
            return volatileOnly;
        }

        public static Policy parse(String name) {
            String wanted = name.toLowerCase(Locale.ROOT);
            for (Policy p : values()) {
                if (p.configName.equals(wanted)) {
                    return p;
                }
            }
            throw new IllegalArgumentException("unknown maxmemory-policy '" + name
                    + "' (supported: noeviction, allkeys-lru, allkeys-random, volatile-lru, volatile-random, volatile-ttl)");
        }
    }
}
