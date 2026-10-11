# redis-java

A Redis-compatible server written in plain Java 21, no frameworks. It speaks
RESP, so `redis-cli`, `redis-benchmark` and ordinary Redis client libraries
can talk to it.

It started from the "Build Your Own Redis" challenge on
[CodeCrafters](https://codecrafters.io/challenges/redis) and the
[Build Your Own Redis in Java](https://www.youtube.com/watch?v=04-lBU8k-5g&list=PLUtdCvEWBvma4DTjQdH5qCV4NIxqhOfDo)
series by Reinventing D Wheel, then went further on correctness. The things
that were changed and why are written up in [NOTES.md](NOTES.md).

## What works

- **Strings:** `GET`, `SET` (with `NX`, `XX`, `GET`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`), `MGET`, `APPEND`, `STRLEN`,
  `INCR`, `DECR`, `INCRBY`, `DECRBY`
- **Keys:** `DEL`, `EXISTS`, `TYPE`, `KEYS` (full glob syntax), `DBSIZE`, `FLUSHALL`,
  `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`
- **Transactions:** `MULTI`, `EXEC`, `DISCARD`, with Redis' rules for queue-time and run-time errors
- **Persistence:** loads an RDB file at startup and writes one with `SAVE`. It reads dumps made by real
  Redis, including LZF-compressed and integer-encoded values, and checks the CRC-64 trailer.
- **Replication:** `--replicaof`, full resync with a real snapshot, command propagation, `WAIT`,
  automatic reconnect. It interoperates with real Redis in both directions.
- **Server:** `PING`, `ECHO`, `INFO`, `CONFIG GET`, `SELECT 0`, `QUIT`, pipelining, inline commands (so `nc` works)

Expired keys are removed lazily on access and by a background sweep every 100 ms.

## Running it

```sh
mvn package
java -jar target/redis-java.jar                       # port 6379, dump.rdb in the current directory
java -jar target/redis-java.jar --port 6380 --replicaof "localhost 6379"
java -jar target/redis-java.jar --dir /var/data --dbfilename snapshot.rdb
```

Then:

```sh
redis-cli set greeting hello
redis-cli get greeting
```

With Docker, `docker compose up` starts a master on 6379 and a replica on 6380.

## Tests

```sh
mvn test
```

The tests drive real servers over sockets rather than calling handlers
directly. They cover pipelining, commands split across TCP packets,
concurrent `INCR`, 200 idle connections, transactions, expiry, save and
restart, and master/replica setups including a master restart.

Two tests use outside references:

- `RdbTest` parses a dump written by redis-server 7.0.15 (`src/test/resources`).
- If `redis-server` is installed, `RdbTest` also starts one and checks that it loads a dump we wrote.
  Otherwise that test is skipped.

## How it's put together

```
protocol/     RESP parsing (streaming, byte-exact) and reply encoding
server/       accept loop, one virtual thread per connection, per-connection state
command/      command table and dispatch, MULTI/EXEC, the commands themselves
store/        the keyspace, expiry, glob matching
persistence/  RDB reader/writer, LZF, CRC-64
replication/  master side (replica tracking, WAIT) and replica side (the link to the master)
```

The central decision: commands run one at a time under a single lock, while
socket reads and writes happen in parallel on virtual threads. That gives the
same guarantee real Redis gets from being single-threaded. Every command is
atomic, `INCR` can't lose updates, and the replication stream has exactly
the order in which commands ran. A finer-grained design would be faster under
contention, but much harder to get right, and Redis itself shows a single
execution thread goes a long way.

## Performance

`redis-benchmark` with 50 clients on a 4-core VM, after warming up the JVM:

| | SET | GET | SET, pipeline 16 | GET, pipeline 16 |
|---|---|---|---|---|
| redis-java | 69k/s | 67k/s | 584k/s | 784k/s |
| redis-server 7.0.15 | 85k/s | 80k/s | 1.03M/s | 1.30M/s |

These numbers come from one run on a shared VM. Read them as "the same order
of magnitude as Redis, and clearly slower when pipelining", not as precise
figures.

## Limitations

- Only string values. No lists, hashes, sets, sorted sets or streams yet; an RDB file containing them is
  rejected at load time rather than silently half-loaded.
- One database (`SELECT 0` only). No `WATCH`, pub/sub, AOF, auth or cluster mode.
- `PSYNC` always does a full resync; there's no backlog for partial resyncs.
- A replica that falls far behind is buffered in memory with no limit, unlike Redis' output-buffer limits.
- Relative expiries (`SET ... EX`) are sent to replicas as-is, so a replica's deadline can trail the
  master's by the propagation delay. Redis rewrites these to absolute times.
