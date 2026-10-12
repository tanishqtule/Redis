package redis.server;

import java.util.concurrent.atomic.AtomicLong;

/** Counters for INFO stats. All of them only ever go up. */
public final class Stats {

    public final AtomicLong connectionsReceived = new AtomicLong();
    public final AtomicLong rejectedConnections = new AtomicLong();
    public final AtomicLong expiredKeys = new AtomicLong();
    public final AtomicLong evictedKeys = new AtomicLong();
}
