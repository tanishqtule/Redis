# redis-java

A Redis-compatible server written in plain Java 21, no frameworks. It speaks
RESP, so `redis-cli`, `redis-benchmark` and ordinary Redis client libraries
can talk to it.

It started from the "Build Your Own Redis" challenge on
[CodeCrafters](https://codecrafters.io/challenges/redis) and the
[Build Your Own Redis in Java](https://www.youtube.com/watch?v=04-lBU8k-5g&list=PLUtdCvEWBvma4DTjQdH5qCV4NIxqhOfDo)
series by Reinventing D Wheel, then went further in two directions:

- **Correctness.** It handles what real clients do (pipelining, packets split mid-command,
  thousands of connections, binary values) and stays compatible with real Redis. It loads Redis'
  RDB files, and replicates to and from a real `redis-server`. It has had a security review, and
  it can be left running: passwords, TLS, automatic snapshots, `maxmemory` eviction.
- **Consistency you can choose and check.** Replicas can serve *linearizable* reads on request,
  something Redis replicas don't offer. A built-in checker measures what consistency a
  deployment actually gives, for this server or for real Redis.

What was changed compared to the series, and why, is in [NOTES.md](NOTES.md).

## What works

- **Strings:** `GET`, `SET` (with `NX`, `XX`, `GET`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`), `MGET`, `MSET`,
  `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY`
- **Keys:** `DEL`, `EXISTS`, `TYPE`, `KEYS` (full glob syntax), `SCAN` (`MATCH`, `COUNT`, `TYPE`),
  `RANDOMKEY`, `DBSIZE`, `FLUSHALL`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`
- **Transactions:** `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`, with Redis' rules for queue-time and
  run-time errors. `WATCH` was checked against a real Redis 7.0 on 31 scenarios, with identical replies.
- **Persistence:** save rules (`save 3600 1 300 100 60 10000` by default), `SAVE`, `BGSAVE`, `LASTSAVE`,
  `SHUTDOWN [SAVE|NOSAVE]`, and a save on SIGTERM. It reads dumps made by real Redis, including
  LZF-compressed and integer-encoded values, and checks the CRC-64 trailer.
- **Memory limit:** `maxmemory` with `noeviction`, `allkeys-lru`, `allkeys-random`, `volatile-lru`,
  `volatile-random` and `volatile-ttl`, sampled the way Redis does it
- **Replication:** `--replicaof`, full resync with a real snapshot, command propagation, `WAIT`, `ROLE`,
  automatic reconnect, output-buffer limits for slow replicas. It interoperates with real Redis in both
  directions, over TLS too.
- **Replica consistency:** `CLIENT CONSISTENCY LINEARIZABLE`, `WAITOFFSET` (see below)
- **Security:** `AUTH`, `requirepass`, protected mode, `bind`, `maxclients`, idle `timeout`, TLS 1.2/1.3
  (optionally with client certificates). See [SECURITY.md](SECURITY.md).
- **Server:** `PING`, `ECHO`, `INFO` (server, clients, memory, persistence, stats, replication,
  keyspace), `CONFIG GET`, `CLIENT ID/SETNAME/GETNAME/SETINFO/LIST/KILL`, `TIME`, `COMMAND`, `SELECT 0`,
  `QUIT`, pipelining, inline commands (so `nc` works)

Expired keys are removed lazily on access and by a background sweep every 100 ms.

Checked with a real client library as well: redis-py 5.0.8 ran its usual calls (pipelines,
`transaction()` with `WATCH` under contention, `scan_iter`, `client_list`, `info`) against it
without a failure. Commands for types that don't exist here, like `HSET`, fail with a clear
`unknown command` error.

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

Settings use Redis' directive names, either in a config file in `redis.conf`
format or as `--directive value` arguments, which override the file:

```sh
java -jar target/redis-java.jar /etc/redis-java.conf --port 6380
```

`port`, `bind`, `protected-mode`, `requirepass`, `masterauth`, `replicaof`,
`dir`, `dbfilename`, `save`, `maxmemory`, `maxmemory-policy`,
`maxmemory-samples`, `maxclients`, `timeout`, `proto-max-bulk-len`,
`client-output-buffer-limit replica` and the `tls-*` settings mean what they
mean in Redis. `requirepass-file` and `masterauth-file` read the secret from a
file. A directive that isn't supported is an error at startup rather than
silently ignored; harmless ones (`loglevel`, `appendonly no`, ...) are
accepted.

**From another machine.** Without a password the server only answers
connections from the same machine (protected mode, as in Redis). For anything
else, set one:

```sh
openssl rand -base64 32 > password.txt
java -jar target/redis-java.jar --requirepass-file password.txt --bind 10.0.0.5
redis-cli -h 10.0.0.5 -a "$(cat password.txt)" ping
```

Add `--tls-port 6380 --tls-cert-file server.crt --tls-key-file server.key`
for TLS. The checklist in [SECURITY.md](SECURITY.md) covers the rest.

**Stopping it.** `SHUTDOWN`, SIGTERM and Ctrl-C save first when save rules are
set, then exit 0. If that save fails, the server logs why and keeps running
rather than lose data; a second Ctrl-C exits without saving.

**Memory.** `maxmemory` counts an estimate of what keys and values cost on
the heap, so keep it well under `-Xmx`; the server warns at startup if it
isn't. Running out of heap mid-command can leave the keyspace half-updated,
so run the JVM with `-XX:+ExitOnOutOfMemoryError` and let it restart from the
last snapshot.

### Docker

```sh
openssl rand -base64 32 > redis_password.txt
chmod 644 redis_password.txt   # the container runs as an unprivileged user
docker compose up -d
redis-cli -p 6379 -a "$(cat redis_password.txt)" set hello world
redis-cli -p 6380 -a "$(cat redis_password.txt)" get hello
```

That starts a master on 6379 and a replica on 6380, both password-protected
and published on 127.0.0.1 only. `docker stop` saves before exiting (the
compose file allows 60 seconds for it) and the image has a health check.

## Reading from replicas without stale data

A Redis replica applies the master's writes a little after the master
acknowledges them, so a read sent to a replica right after a write can
return the old value. That isn't rare. In the lab runs below, a client
writing to a real Redis 7.0.15 master and then reading the same key from its
replica got a provably stale answer in every run, on a quiet machine with
master and replica on the same host.

This server offers two ways around it, both on the replica.

**`CLIENT CONSISTENCY LINEARIZABLE`** switches a connection so that every
read is linearizable: it can't return anything older than a write that
finished before the read started, on any connection. Any Redis client can use
it by sending the command once after connecting. Before each read the replica:

1. asks the master for its replication position (id and offset, from `INFO replication`);
2. waits until it has applied the master's stream up to that offset;
3. answers from its own data.

This is the ReadIndex technique from Raft, which etcd and TiKV use for
follower reads, applied to Redis replication offsets. It works when the
master is a real Redis server too. When the replica can't prove it's caught
up (master unreachable, master restarted with a new history, timeout), the
read fails with `MASTERDOWN` or `TRYAGAIN` rather than answering stale.
Concurrent reads share requests to the master. In the lab runs, 2,400
reads took 739 to 823 requests.

The price is latency: a round trip to the master plus any wait for the
replica to catch up. Median read latency went from about 0.25 ms to about
0.9 ms in the lab runs.

**`WAITOFFSET offset timeout`** is the low-level version for read-your-writes
without contacting the master at read time. After a write, the client takes
the master's offset from `ROLE` (element 2). Before reading from a replica,
it calls `WAITOFFSET` with that offset. The reply is the offset the replica
reached; anything lower means the timeout passed first.

```
master>  SET cart:42 "3 items"
master>  ROLE                      -> 1) "master" 2) (integer) 18734 ...
replica> WAITOFFSET 18734 100      -> (integer) 18734
replica> GET cart:42               -> "3 items"
```

Why the linearizable mode is correct, and what it assumes, is argued in
[NOTES.md](NOTES.md#linearizable-reads-from-replicas).

## The consistency lab

`redis.lab.ConsistencyLab` runs concurrent clients against a deployment,
records every operation with its timing, and checks the history for
linearizability. It works against real Redis as well.

```sh
java -cp target/redis-java.jar redis.lab.ConsistencyLab --master localhost:6379 --replica localhost:6380
```

```
scenario                                      ops  linearizable?        stale   read p50  read p99
master only                                  3582  yes                      0    0.38 ms   5.88 ms
replica, default reads                       3582  NO                     605    0.29 ms   6.72 ms
replica, CLIENT CONSISTENCY LINEARIZABLE     3582  yes                      0    1.26 ms   8.78 ms

replica, default reads: GET lab:29f7...:reg0 -> (nil) [1.108 to 1.602 ms, client 0] missed
  SET lab:29f7...:reg0 "c0-0" [0.000 to 1.104 ms, client 0], which had already finished
```

Each client writes to the master, then half the time reads back the key it
just wrote. "Stale" counts reads proven older than a write that had already
finished. The checker is the Wing and Gong search with Lowe's memoization
(the algorithm in Knossos and Porcupine), run per key. Before that, a quick
pass explains the obvious violations in words.

Five runs per setup, 8 clients, 2,400 reads each, everything on one 4-core VM:

| master + replica | master reads | default replica reads | linearizable replica reads |
|---|---|---|---|
| redis-java + redis-java | linearizable, 5/5 | **not linearizable, 5/5** (279–472 stale) | linearizable, 5/5 |
| redis-server 7.0.15 + redis-server 7.0.15 | linearizable, 5/5 | **not linearizable, 5/5** (8–122 stale) | not supported |

A redis-java replica following a real Redis master was checked less: one lab
run, plus a separate loop of 2,000 write-then-read pairs. Linearizable reads
were never stale in either; default reads in that loop were stale 79 times.

Two honest caveats. First, a passing run shows that run's history was
linearizable; it isn't a proof that every run will be. Second, a redis-java
replica serves noticeably more stale default reads than a Redis replica in
the same setup. Why hasn't been pinned down: a dedicated platform thread for
the replication link made no measurable difference (see NOTES.md).

## Tests

```sh
mvn test
```

The tests drive real servers over sockets rather than calling handlers
directly. They cover:
- pipelining, commands split across TCP packets, concurrent `INCR` and 200 idle connections;
- transactions and `WATCH` (including 8 clients doing check-and-set increments that must not lose one),
  expiry, `SCAN` while keys come and go, eviction under `maxmemory`;
- save rules, `BGSAVE`, `SHUTDOWN`, and SIGTERM sent to a server in its own JVM, including one whose save
  fails, and a check that a shutdown keeps every write it acknowledged;
- master/replica setups, including a master restart, a replica that joins late, passwords and TLS;
- stale and linearizable replica reads under simulated replication lag;
- the security fixes: hostile input to the parser, the glob matcher and the RDB loader (fuzzed), fake
  replicas, hostile masters, and each limit in [SECURITY.md](SECURITY.md).

Outside references keep the tests honest:

- `RdbTest` parses a dump written by redis-server 7.0.15 (`src/test/resources`). If `redis-server`
  is installed, it also checks that a real server loads a dump we wrote.
- `LinearizabilityBruteForceTest` compares the checker with a brute-force search on 12,000 random
  histories, using a model written separately.
- `ConsistencyLabTest` runs the lab end to end: with 50 ms of simulated lag, default replica reads
  must fail the check and linearizable ones must pass.

`--simulate-replication-lag <ms>` on a master delays everything it sends to
replicas. It exists for experiments like these.

## How it's put together

```
protocol/     RESP parsing (streaming, byte-exact, bounded), reply encoding, a small client
server/       config, accept loop, one virtual thread per connection, TLS, per-connection state
command/      command table and dispatch, MULTI/EXEC/WATCH, the commands themselves
store/        the keyspace, expiry, SCAN positions (SipHash), eviction, glob matching
persistence/  RDB reader/writer, LZF, CRC-64, save rules and shutdown
replication/  master side (replica tracking, WAIT) and replica side (link to the master, read index)
lab/          linearizability checker and the consistency lab
```

The central decision: commands run one at a time under a single lock, while
socket reads and writes happen in parallel on virtual threads. That gives the
same guarantee real Redis gets from being single-threaded. Every command is
atomic, `INCR` can't lose updates, and the replication stream has exactly
the order in which commands ran. A finer-grained design would be faster under
contention, but much harder to get right, and Redis itself shows a single
execution thread goes a long way.

## Performance

`redis-benchmark` with 50 clients on a 4-core VM, after warming up the JVM.
Both servers ran with their default settings, alternating, three rounds each;
the table shows the medians.

| | SET | GET | SET, pipeline 16 | GET, pipeline 16 |
|---|---|---|---|---|
| redis-java | 64k/s | 65k/s | 400k/s | 762k/s |
| redis-server 7.0.15 | 88k/s | 88k/s | 1.06M/s | 1.27M/s |

Without pipelining, both spend most of their time on network round trips, and
redis-java manages about three quarters of Redis' rate. With pipelining the
gap opens up: Redis is about 2.6 times faster on SET and 1.7 times on GET.

The security work and the practical features cost nothing measurable without
pipelining and on pipelined GET. Pipelined SET got about 12% slower (449k/s
to 396k/s, same method, same session). That's the bookkeeping each write now
does for save rules, `maxmemory`, LRU and `INFO`. Taking all of it out brings
the old number back; taking out the two shared counters one at a time made no
measurable difference, so it looks like many small costs rather than one slow
piece.

This is a shared VM, and numbers from different days differ by more than
that 12%. An earlier version of this table, measured on another day, showed
584k/s for pipelined SET. Compare runs from the same session only.

## Limitations

- Only string values. No lists, hashes, sets, sorted sets or streams yet; an RDB file containing them is
  rejected at load time rather than silently half-loaded.
- One database (`SELECT 0` only). No pub/sub, Lua scripting, AOF, ACL users or cluster mode.
- `PSYNC` always does a full resync; there's no backlog for partial resyncs.
- `maxmemory` is an estimate of heap use, not a measurement (see Running it).
- Relative expiries (`SET ... EX`) are sent to replicas as-is, so a replica's deadline can trail the
  master's by the propagation delay. Redis rewrites these to absolute times.
- Inline commands (the `nc`/telnet form) are split on spaces; quotes aren't understood.
- Linearizable replica reads assume the master doesn't lose writes it acknowledged. There is no
  failover here, but a master restarted without its data breaks that assumption, just as it would
  for reads from the master itself.
