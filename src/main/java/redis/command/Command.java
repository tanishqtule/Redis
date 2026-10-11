package redis.command;

import java.util.List;
import java.util.Set;

import redis.server.Session;

/**
 * A registered command.
 *
 * @param arity same convention as Redis' COMMAND output: N means exactly N
 *              arguments including the name, -N means at least N
 */
public record Command(String name, int arity, Set<Flag> flags, Handler handler) {

    public enum Flag {
        /** Modifies the keyspace: refused on replicas, propagated by masters. */
        WRITE,
        /** Reads the keyspace: a replica serving linearizable reads checks it's caught up first. */
        READ,
        /** Runs immediately even inside MULTI (MULTI, EXEC, DISCARD, ...). */
        TX_CONTROL,
        /** May block for a while, so it must not hold the global lock. */
        BLOCKING
    }

    @FunctionalInterface
    public interface Handler {
        /** Returns the encoded reply, or null when nothing should be sent back. */
        byte[] handle(Session session, List<byte[]> args);
    }

    public boolean has(Flag flag) {
        return flags.contains(flag);
    }

    public boolean acceptsArgCount(int n) {
        return arity >= 0 ? n == arity : n >= -arity;
    }
}
