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

## Verified against real Redis

These were run by hand against redis-server 7.0.15, not in CI:

- A real `redis-server --replicaof` this server: it loaded our snapshot,
  applied streamed `SET`/`INCR`/`DEL`/`MULTI…EXEC`, and `WAIT 1` returned 1.
  Both sides reported offset 347.
- This server as a replica of a real master: it loaded Redis' RDB (version 10, with LZF), handled the
  `SELECT` a real master sends first, applied writes and expiry `DEL`s, stayed connected well past
  `repl-timeout`, and both sides reported offset 248.
