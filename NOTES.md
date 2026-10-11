# Notes: what changed compared to the series, and why

The YouTube series ([playlist](https://www.youtube.com/watch?v=04-lBU8k-5g&list=PLUtdCvEWBvma4DTjQdH5qCV4NIxqhOfDo),
[source](https://dev.azure.com/ChaitanyaDSharma/Codecrafters/_git/codecrafters-redis-java-yt)) is
a good walkthrough of the CodeCrafters stages. Its code is written to pass the
CodeCrafters tests, but it breaks when used the way a real client uses Redis.

The rows marked *ran it* were reproduced by building that code (with the Java
target lowered from 23 to 21, otherwise unchanged) and sending it raw RESP
from small Python scripts, on a fresh instance per check. The rest come from
reading the code.

## Problems found in the original, and what this version does instead

| What happens | Cause | Here |
|---|---|---|
| *Ran it:* on a 4-core machine, the 4th open connection never gets a reply | Each client runs as a blocking loop on `CompletableFuture.runAsync`, which uses the common ForkJoin pool (cores − 1 threads) | One virtual thread per connection; a test opens 200 idle connections and then expects a new one to get `PONG` |
| *Ran it:* CPU sits at ~270% after a few clients disconnect | `while (socket.isConnected())` stays true after the peer closes, and `read()` returning -1 isn't checked, so the loop spins | `null` from the reader ends the loop |
| *Ran it:* 2 × 1000 concurrent `INCR` ended at 1996 | INCR reads the value, then writes it outside any lock | All commands run under one lock |
| *Ran it:* `ping` (lowercase) or any unknown command gets no reply and the client hangs | Exact-match `switch` with no default reply | Case-insensitive lookup; unknown commands and wrong arity return proper errors |
| *Ran it:* a command split across two TCP writes never gets a reply | Each `read()` is parsed as if it held whole commands | Streaming parser that reads exactly what RESP says to read |
| *Ran it:* after `SET u é`, `GET u` returns `$2\r\né\r\r\n`, a malformed reply that desyncs the client | Bytes are decoded to UTF-8 first, then lengths are counted in chars | Bulk lengths are counted in bytes; values are kept as `byte[]` |
| *Ran it:* `redis-benchmark` can't run at all | It sends `CONFIG GET` first, which goes unanswered | `CONFIG GET` is implemented |
| Writes can reach replicas out of order | Each propagation is a separate `runAsync` task | Writes are queued per replica while the command lock is held, and sent by one writer thread |
| A replica that joins late misses existing data | `FULLRESYNC` always sends a hard-coded empty RDB | Sends a snapshot of the current data, taken under the same lock as the start offset |
| `WAIT` burns a core while it waits | Busy loop on a shared counter | Waits on a monitor that each `ACK` signals |
| The replica can misread values containing `*` | The replication stream is split by scanning for `*` bytes | The same RESP parser as everywhere else, with exact byte counting for the offset |

Smaller things: `DEL` inside a transaction returned `+OK` or an error for a
missing key, where Redis returns an integer. `SET ... PX` inside `MULTI`
dropped the expiry. Keys only expired when read, so unread keys stayed in
memory forever. Spring was pulled in only for dependency injection.

## Design choices worth knowing about

**One lock instead of fine-grained locking.** This is the main decision.
Redis gets atomicity and a single total order from running commands on one
thread. Taking one lock around command execution gives the same thing, while
socket I/O still runs in parallel. A per-key lock design would do better under
heavy contention. But `MULTI/EXEC`, `KEYS` and replication ordering would each
need special care. At this size, being obviously correct is worth more than
the extra throughput. Where the remaining gap to real Redis comes from
hasn't been profiled yet.

**Keys are Strings decoded as ISO-8859-1.** That charset maps each byte to
exactly one char and back, so keys stay binary-safe but still work as normal
`HashMap` keys. Values stay as `byte[]`.

**Expiry uses a min-heap with lazy deletion.** Overwriting a key leaves its
old deadline in the heap, and that entry is skipped when it comes up. The heap
is rebuilt if dead entries pile up (a test sets the same key with a TTL
100,000 times to check this).

**Replicas don't expire keys themselves.** They hide expired keys from reads
and wait for the master's `DEL`, which is what Redis does too. Otherwise a
replica and its master could disagree about whether a key exists.

**Replicas send `REPLCONF ACK` every second.** A real Redis master drops
replicas that go quiet for `repl-timeout`. Checked against redis-server 7.0.15
with `repl-timeout 3`. CodeCrafters' replica tests may not expect these
unprompted ACKs. If you submit this there, that's the first thing to look at.

## Linearizable reads from replicas

### Why it's correct

Take a read R that starts at time t0 on a replica in linearizable mode.

1. The replica asks the master for its position, the replication id and
   offset O. The request is sent after t0, so the master answers at some
   moment tm ≥ t0, while holding its command lock. A write is applied before
   it's acknowledged, so O covers every write that finished before R started.
2. The replica waits until it has applied the stream up to some O' ≥ O,
   then runs R. R sees exactly the master's state at offset O'.
3. Place R at the moment max(tm, when the master applied O'). That moment is
   no earlier than t0, because tm ≥ t0. It's no later than R's reply, because
   the replica can't apply O' before the master did, and tm came before the
   replica's wait. At that moment the master's state is what R returned. If
   the master applied O' after tm, O' had just become current. If it applied
   O' before tm, then O' ≤ O, so O' = O, which was current at tm.

Writes go at the moment the master applies them and master reads where they
run. Every point then lies on one sequence, the master's own order, so the
history is linearizable.

Two details matter in practice:

- **Transactions.** A master streams a MULTI/EXEC as one block while holding
  its lock, so an offset it reports never points into the middle of a block.
  A replica that has reached such an offset has applied whole transactions.
- **Which history.** Offsets only compare within one replication history. If
  the master restarted, its offsets start again, and an old replica could hold
  data the new master doesn't have at a *higher* offset. So the read index
  carries the replication id, and the replica checks it against the id from
  its last full sync, before and after waiting. That's why it uses
  `INFO replication` rather than `ROLE`, which doesn't report the id.

### What it assumes

The master doesn't lose writes it has acknowledged: no failover, and no
restart without the data. If that's broken, reads from the master itself
aren't linearizable either. The replica also asks the same host and port it
replicates from, so a load balancer in between would void the argument.

### Sharing requests to the master

Concurrent reads can share one request, with one rule: a read may only join a
request sent *after* it arrived. A request already in flight might have been
answered before the read started, and then step 1 fails. So a read that
arrives during a request waits for the next one, which starts as soon as the
current one returns. In the lab runs, 2,400 reads made 739 to 823 requests.

### Prior work

The idea is Raft's ReadIndex, used for follower reads in etcd, TiKV and
Apache Ozone ([openraft's description](https://docs.rs/crate/openraft/0.10.0-alpha.31/source/src/docs/protocol/read.md),
[TiKV/TiDB follower read](https://github.com/pingcap/docs/pull/22000/files),
[Ozone](https://ozone.apache.org/docs/next/administrator-guide/configuration/high-availability/om-follower-read)).
Redis' own `WAIT` runs on the master and only counts acknowledgements; it
doesn't help a reader on a replica ([WAIT docs](https://redis.io/docs/latest/commands/wait/)).

The checker follows Lowe's "Testing for linearizability"
([paper](https://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/paper.pdf)),
the Wing and Gong search plus memoization, as implemented in Knossos and
[Porcupine](https://pkg.go.dev/github.com/anishathalye/porcupine). Checking
per key relies on linearizability being compositional; Horn and Kroening's
[P-compositionality](https://arxiv.org/pdf/1504.00204) paper is the
reference for that speed-up.

## Bugs found while building the second round

- **Replica offsets started from zero.** After a full sync, a replica counted
  its offset from 0. Redis, and the master side here, count from the offset
  in the FULLRESYNC reply. A replica that attached after the master had any
  writes acked numbers that were too small, so `WAIT` never counted it. The
  first round's tests missed it because every replica in them attached at
  offset 0. It was found while working out the read-index argument above.
  A regression test now attaches a replica late.
- **The checker crashed on a non-numeric read of a counter.** The
  differential test against brute force found it within seconds. A correct
  server never produces such a read, but a checker has to cope with exactly
  the histories broken servers produce.

## An experiment that didn't work out

With a real Redis master, a redis-java replica serves noticeably more stale
default reads than a Redis replica does: 153 to 840 out of 2,400 over 18
runs, against 8 to 122 over 5 runs. The guess was that the replication link
competes with client connections for the virtual-thread carriers and wakes
up late. Moving it to a dedicated platform thread and measuring nine runs of
each gave means of 460 (virtual) and 445 (platform), with heavy overlap. No
effect, so the change was reverted. The cause is still open. Next things to
look at: the lock handoff between the link thread and client threads, and
whether a read simply races the stream on a busy 4-core VM.

## Verified against real Redis

These were run by hand against redis-server 7.0.15, not in CI:

- A real `redis-server --replicaof` this server: it loaded our snapshot,
  applied streamed `SET`/`INCR`/`DEL`/`MULTI…EXEC`, and `WAIT 1` returned 1.
  Both sides reported offset 347.
- This server as a replica of a real master: it loaded Redis' RDB (version 10, with LZF), handled the
  `SELECT` a real master sends first, applied writes and expiry `DEL`s, stayed connected well past
  `repl-timeout`, and both sides reported offset 248.
- Late joiners, both directions: a redis-java replica that synced from a real master at offset 974,
  and a real replica that synced from this server at offset 981. `WAIT` counted both, and master and
  replica reported the same offset afterwards.
- `ROLE` replies are byte-for-byte the same shape as Redis' on master and replica.
- Linearizable reads from a redis-java replica of a real master: 0 stale out of 2,000 write-then-read
  pairs, where default reads were stale 79 times.
