package redis.command;

import static redis.command.Command.Flag.DENY_OOM;
import static redis.command.Command.Flag.READ;
import static redis.command.Command.Flag.WRITE;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import redis.protocol.Resp;
import redis.server.Session;
import redis.store.Glob;
import redis.store.Keyspace;

/** String commands plus the generic key commands (DEL, EXPIRE, TTL, KEYS, ...). */
public final class KeyCommands {

    private static final Set<Command.Flag> W = Set.of(WRITE);
    /** Writes that can add data, refused or evicting under maxmemory. */
    private static final Set<Command.Flag> WM = Set.of(WRITE, DENY_OOM);
    private static final Set<Command.Flag> R = Set.of(READ);

    private final Keyspace db;
    private final int maxValueSize;
    private final Random random = new Random();

    /** @param maxValueSize the largest string APPEND may build (Redis' proto-max-bulk-len) */
    public KeyCommands(Keyspace db, int maxValueSize) {
        this.db = db;
        this.maxValueSize = maxValueSize;
    }

    public void registerAll(CommandTable table) {
        table.register("GET", 2, R, (s, a) -> Resp.bulk(read(s, Args.str(a, 1))));
        table.register("SET", -3, WM, this::set);
        table.register("MGET", -2, R, this::mget);
        table.register("MSET", -3, WM, this::mset);
        table.register("SETNX", 3, WM, (s, a) -> Resp.integer(setIfAbsent(a) ? 1 : 0));
        table.register("SETEX", 4, WM, (s, a) -> setWithTtl(a, 1000, "setex"));
        table.register("PSETEX", 4, WM, (s, a) -> setWithTtl(a, 1, "psetex"));
        table.register("GETSET", 3, WM, this::getSet);
        table.register("GETDEL", 2, W, this::getDel);
        table.register("APPEND", 3, WM, this::append);
        table.register("STRLEN", 2, R, (s, a) -> Resp.integer(lengthOf(read(s, Args.str(a, 1)))));
        table.register("INCR", 2, WM, (s, a) -> incrementBy(a, 1));
        table.register("DECR", 2, WM, (s, a) -> incrementBy(a, -1));
        table.register("INCRBY", 3, WM, (s, a) -> incrementBy(a, Args.parseLong(a.get(2))));
        table.register("DECRBY", 3, WM, (s, a) -> incrementBy(a, negate(Args.parseLong(a.get(2)))));

        table.register("DEL", -2, W, this::del);
        table.register("EXISTS", -2, R, this::exists);
        table.register("TYPE", 2, R, (s, a) -> Resp.simple(db.exists(Args.str(a, 1)) ? "string" : "none"));
        table.register("KEYS", 2, R, this::keys);
        table.register("SCAN", -2, R, this::scan);
        table.register("RANDOMKEY", 1, R, (s, a) -> {
            String key = db.randomKey(random);
            return key == null ? Resp.NULL_BULK : Resp.bulk(Args.raw(key));
        });
        table.register("DBSIZE", 1, R, (s, a) -> Resp.integer(db.size()));
        table.register("FLUSHALL", -1, W, this::flushAll);

        table.register("EXPIRE", 3, W, (s, a) -> expire(a, 1000));
        table.register("PEXPIRE", 3, W, (s, a) -> expire(a, 1));
        table.register("TTL", 2, R, (s, a) -> ttl(a, 1000));
        table.register("PTTL", 2, R, (s, a) -> ttl(a, 1));
        table.register("PERSIST", 2, W, (s, a) -> Resp.integer(db.persist(Args.str(a, 1)) ? 1 : 0));
    }

    /** SET key value [NX | XX] [GET] [EX s | PX ms | EXAT s | PXAT ms | KEEPTTL] */
    private byte[] set(Session session, List<byte[]> args) {
        String key = Args.str(args, 1);
        byte[] value = args.get(2);
        boolean nx = false;
        boolean xx = false;
        boolean returnOld = false;
        boolean keepTtl = false;
        long expireAt = Keyspace.NO_EXPIRY;
        boolean expirySet = false;

        for (int i = 3; i < args.size(); i++) {
            String opt = Args.upper(args.get(i));
            switch (opt) {
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "GET" -> returnOld = true;
                case "KEEPTTL" -> keepTtl = true;
                case "EX", "PX", "EXAT", "PXAT" -> {
                    if (expirySet || i + 1 >= args.size()) {
                        throw CommandException.syntax();
                    }
                    expireAt = deadline(opt, Args.parseLong(args.get(++i)));
                    expirySet = true;
                }
                default -> throw CommandException.syntax();
            }
        }
        if ((nx && xx) || (keepTtl && expirySet)) {
            throw CommandException.syntax();
        }

        Keyspace.Entry current = db.lookup(key);
        boolean allowed = nx ? current == null : !xx || current != null;
        if (allowed) {
            if (keepTtl && current != null) {
                expireAt = current.expireAt();
            }
            db.set(key, value, expireAt);
        }
        if (returnOld) {
            return Resp.bulk(current == null ? null : current.value());
        }
        return allowed ? Resp.OK : Resp.NULL_BULK;
    }

    private long deadline(String unit, long n) {
        boolean relative = unit.equals("EX") || unit.equals("PX");
        long multiplier = unit.startsWith("E") ? 1000 : 1;
        if (n <= 0 || n > Long.MAX_VALUE / multiplier) {
            throw new CommandException("ERR invalid expire time in 'set' command");
        }
        long ms = n * multiplier;
        if (!relative) {
            return ms;
        }
        long now = db.now();
        if (ms > Long.MAX_VALUE - now) {
            throw new CommandException("ERR invalid expire time in 'set' command");
        }
        return now + ms;
    }

    private byte[] mget(Session session, List<byte[]> args) {
        List<byte[]> values = new ArrayList<>(args.size() - 1);
        for (int i = 1; i < args.size(); i++) {
            values.add(Resp.bulk(read(session, Args.str(args, i))));
        }
        return Resp.array(values);
    }

    private byte[] mset(Session session, List<byte[]> args) {
        if (args.size() % 2 == 0) {
            throw new CommandException("ERR wrong number of arguments for 'mset' command");
        }
        for (int i = 1; i < args.size(); i += 2) {
            db.set(Args.str(args, i), args.get(i + 1), Keyspace.NO_EXPIRY);
        }
        return Resp.OK;
    }

    private boolean setIfAbsent(List<byte[]> args) {
        String key = Args.str(args, 1);
        if (db.exists(key)) {
            return false;
        }
        db.set(key, args.get(2), Keyspace.NO_EXPIRY);
        return true;
    }

    /** SETEX key seconds value / PSETEX key milliseconds value. */
    private byte[] setWithTtl(List<byte[]> args, long unitMs, String name) {
        long n = Args.parseLong(args.get(2));
        long at;
        try {
            if (n <= 0) {
                throw new ArithmeticException();
            }
            at = Math.addExact(db.now(), Math.multiplyExact(n, unitMs));
        } catch (ArithmeticException e) {
            throw new CommandException("ERR invalid expire time in '" + name + "' command");
        }
        db.set(Args.str(args, 1), args.get(3), at);
        return Resp.OK;
    }

    private byte[] getSet(Session session, List<byte[]> args) {
        String key = Args.str(args, 1);
        byte[] old = read(session, key);
        db.set(key, args.get(2), Keyspace.NO_EXPIRY);
        return Resp.bulk(old);
    }

    private byte[] getDel(Session session, List<byte[]> args) {
        String key = Args.str(args, 1);
        byte[] value = read(session, key);
        if (value != null) {
            db.delete(key);
        }
        return Resp.bulk(value);
    }

    /** SCAN cursor [MATCH pattern] [COUNT count] [TYPE type] */
    private byte[] scan(Session session, List<byte[]> args) {
        long cursor;
        try {
            cursor = Long.parseUnsignedLong(Args.str(args, 1));
        } catch (NumberFormatException e) {
            throw new CommandException("ERR invalid cursor");
        }
        String pattern = null;
        int count = 10;
        String type = null;
        for (int i = 2; i < args.size(); i += 2) {
            if (i + 1 >= args.size()) {
                throw CommandException.syntax();
            }
            switch (Args.upper(args.get(i))) {
                case "MATCH" -> pattern = Args.str(args, i + 1);
                case "COUNT" -> {
                    long n = Args.parseLong(args.get(i + 1));
                    if (n < 1) {
                        throw CommandException.syntax();
                    }
                    count = (int) Math.min(n, 1_000_000);
                }
                case "TYPE" -> type = Args.str(args, i + 1).toLowerCase(java.util.Locale.ROOT);
                default -> throw CommandException.syntax();
            }
        }
        if (cursor < 0) {
            // A cursor past 2^63 can't have come from us; treat it as the end.
            return Resp.array(List.of(Resp.bulk("0"), Resp.EMPTY_ARRAY));
        }
        String glob = pattern;
        boolean onlyStrings = type == null || type.equals("string");
        Keyspace.ScanPage page = db.scan(cursor, count, k -> onlyStrings && (glob == null || Glob.matches(glob, k)));
        List<byte[]> keys = new ArrayList<>(page.keys().size());
        for (String key : page.keys()) {
            keys.add(Resp.bulk(Args.raw(key)));
        }
        return Resp.array(List.of(Resp.bulk(Long.toString(page.nextCursor())), Resp.array(keys)));
    }

    private byte[] append(Session session, List<byte[]> args) {
        String key = Args.str(args, 1);
        Keyspace.Entry current = db.lookup(key);
        byte[] suffix = args.get(2);
        byte[] value;
        long expireAt = Keyspace.NO_EXPIRY;
        if (current == null) {
            value = suffix;
        } else {
            if ((long) current.value().length + suffix.length > maxValueSize) {
                throw new CommandException("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
            }
            value = new byte[current.value().length + suffix.length];
            System.arraycopy(current.value(), 0, value, 0, current.value().length);
            System.arraycopy(suffix, 0, value, current.value().length, suffix.length);
            expireAt = current.expireAt();
        }
        db.set(key, value, expireAt);
        return Resp.integer(value.length);
    }

    private byte[] incrementBy(List<byte[]> args, long delta) {
        String key = Args.str(args, 1);
        Keyspace.Entry current = db.lookup(key);
        long value = current == null ? 0 : Args.parseLong(current.value());
        long updated;
        try {
            updated = Math.addExact(value, delta);
        } catch (ArithmeticException e) {
            throw new CommandException("ERR increment or decrement would overflow");
        }
        // INCR keeps an existing TTL; it only changes the value.
        long expireAt = current == null ? Keyspace.NO_EXPIRY : current.expireAt();
        db.set(key, Args.raw(Long.toString(updated)), expireAt);
        return Resp.integer(updated);
    }

    private static long negate(long n) {
        if (n == Long.MIN_VALUE) {
            throw new CommandException("ERR decrement would overflow");
        }
        return -n;
    }

    private byte[] del(Session session, List<byte[]> args) {
        int removed = 0;
        for (int i = 1; i < args.size(); i++) {
            if (db.delete(Args.str(args, i))) {
                removed++;
            }
        }
        return Resp.integer(removed);
    }

    private byte[] exists(Session session, List<byte[]> args) {
        // Repeated keys count repeatedly; that's what Redis does too.
        int found = 0;
        for (int i = 1; i < args.size(); i++) {
            if (db.exists(Args.str(args, i))) {
                found++;
            }
        }
        return Resp.integer(found);
    }

    private byte[] keys(Session session, List<byte[]> args) {
        String pattern = Args.str(args, 1);
        List<byte[]> out = new ArrayList<>();
        for (String key : db.keys(k -> Glob.matches(pattern, k))) {
            out.add(Resp.bulk(Args.raw(key)));
        }
        return Resp.array(out);
    }

    private byte[] flushAll(Session session, List<byte[]> args) {
        // FLUSHALL [ASYNC | SYNC]; both mean the same thing here.
        if (args.size() > 2 || args.size() == 2 && !Set.of("ASYNC", "SYNC").contains(Args.upper(args.get(1)))) {
            throw CommandException.syntax();
        }
        db.clear();
        return Resp.OK;
    }

    private byte[] expire(List<byte[]> args, long unitMs) {
        String key = Args.str(args, 1);
        long n = Args.parseLong(args.get(2));
        long at;
        try {
            at = Math.addExact(db.now(), Math.multiplyExact(n, unitMs));
        } catch (ArithmeticException e) {
            throw new CommandException("ERR invalid expire time in '" + (unitMs == 1000 ? "expire" : "pexpire") + "' command");
        }
        return Resp.integer(db.setExpireAt(key, at) ? 1 : 0);
    }

    private byte[] ttl(List<byte[]> args, long unitMs) {
        long at = db.expireAt(Args.str(args, 1));
        if (at < 0) {
            return Resp.integer(at);
        }
        long remaining = Math.max(0, at - db.now());
        // Round to the nearest second like Redis does, so a fresh EX 10 shows 10, not 9.
        return Resp.integer(unitMs == 1 ? remaining : (remaining + 500) / 1000);
    }

    /** A lookup that counts towards INFO's keyspace hits and misses. */
    private byte[] read(Session session, String key) {
        byte[] value = db.get(key);
        session.countLookup(value != null);
        return value;
    }

    private static int lengthOf(byte[] value) {
        return value == null ? 0 : value.length;
    }
}
