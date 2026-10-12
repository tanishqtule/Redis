# Security

## Reporting a vulnerability

Please report it privately through the repository's **Security** tab
("Report a vulnerability"), not in a public issue. Include what you sent to
the server and what happened; a script that reproduces it is ideal.

## What you get by default

- **Protected mode.** With no password set, only connections from the same
  machine (loopback) are accepted. Everyone else gets a `-DENIED` error that
  explains how to fix it. This is Redis' behaviour too.
- **Passwords.** `requirepass` (or `requirepass-file`, so the secret stays
  out of the process list and shell history) makes every command except
  `AUTH` and `QUIT` answer `NOAUTH` until the connection authenticates. The
  comparison is constant-time. Failed attempts are logged, at most once every
  10 seconds.
- **Limits before authentication.** Until a connection has authenticated, a
  request may have at most 10 arguments of at most 16 KB each, as in Redis
  (the fix for CVE-2021-32675). An anonymous client can't make the server
  allocate much.
- **Bounded everything.** Request size (`proto-max-bulk-len`, 512 MB),
  connections (`maxclients`, 10,000), replica output buffers
  (`client-output-buffer-limit replica`, 256 MB), and error messages that
  echo client input (128 characters).
- **Snapshots** are written to a temp file, fsynced, renamed into place and
  created with mode 0600.
- **No `CONFIG SET`.** The classic Redis attack (point `dir` and `dbfilename`
  at `~/.ssh` or a cron directory, then `SAVE`) has nothing to work with.
  `dbfilename` can't contain a path at all.
- **The Docker image** runs as an unprivileged user, and the compose file
  publishes ports on 127.0.0.1 only, with the password coming from a secret
  file.

## Before exposing it to a network

1. Set a long random password: `openssl rand -base64 32 > password.txt`,
   then `--requirepass-file password.txt`. There is no rate limit on `AUTH`
   (Redis has none either), so a short password can be guessed.
2. Bind to the interface clients use (`--bind 10.0.0.5`), not every interface.
3. Use TLS if the network isn't yours: `--tls-port`, `--tls-cert-file`,
   `--tls-key-file`, plus `--tls-replication yes` and `--tls-ca-cert-file` on
   replicas so they check who they're talking to. Unlike Redis,
   `tls-auth-clients` defaults to `no`: clients prove who they are with the
   password. Set it to `yes` (and give `tls-ca-cert-file`) to require client
   certificates as well. To serve TLS only, add `--port 0`.
4. Give replicas `masterauth-file` with the master's password.
5. Set `timeout` if idle clients should be disconnected.
6. Keep the data directory private; the snapshot contains everything.

## Security review, October 2026

The whole server was reviewed for ways an outsider, or a client with
limited intentions, could read data, take the server down, or make it
misbehave. Each finding below was first reproduced with a proof of concept
against the running server, and the same proof of concept was run again
after the fix. Where the last column names a test, that test keeps the
problem from coming back.

| | Finding | Before | After | Guarded by |
|---|---|---|---|---|
| Critical | Anyone who could reach the port could run any command. `PSYNC` from another host handed over the whole dataset as an RDB file. | Full dump sent to a non-loopback client | `-DENIED` without a password; `-NOAUTH` with one | `AccessControlTest` |
| High | `KEYS` with a pattern like `*a*a*a…*ab` made the recursive glob matcher backtrack exponentially (the bug class of CVE-2022-36021). | Server unresponsive for over 120 s | 0.008 s | `GlobTest.pathologicalPatternsFinishQuickly`, plus 200,000 random patterns compared with the old matcher |
| High | A client that sent `PSYNC` and then stopped reading made the master block on the socket while holding the command lock. | Every other client froze | Snapshot sent by the replica's own writer thread, outside the lock | `ReplicationSafetyTest.psyncFromAClientThatStoppedReadingDoesNotFreezeTheServer` |
| High | A fake replica that never read its stream made the master buffer it forever. | Heap grew until `OutOfMemoryError` | Disconnected when its buffer passes 256 MB (and `PSYNC` needs the password now) | `ReplicationSafetyTest.replicaThatFallsTooFarBehindIsDisconnected` |
| Medium | The parser trusted declared lengths: `*1048576` made it allocate room for a million arguments before any arrived. | 48 `OutOfMemoryError`s from 60 connections sending 10 bytes each | None; allocation grows with the data that actually arrives | `RespReaderHardeningTest` |
| Medium | Running out of file descriptors made the accept loop spin. | 573,143 log lines in about 6 seconds | `maxclients` turns extra clients away; failed accepts back off (8 log lines) | `AccessControlTest.maxclientsTurnsAwayTheExtraConnection` for the limit; the back-off only by the proof of concept |
| Medium | A hostile master (or anything on the path to it) could send a snapshot that crashed the replication thread for good. An LZF header that "expands" 3 bytes to 2 GB was one way. | Replication dead until restart | Rejected before allocating; the replica keeps reconnecting (8 retries in 8 s in the proof of concept) | `ReplicationSafetyTest.replicaKeepsRetryingAfterAHostileSnapshot`, `RdbFuzzTest` |
| Low | Unknown-command errors quoted the arguments in full. | 2 MB reply to a 2 MB request | 192 bytes | `CommandHardeningTest.unknownCommandErrorsStaySmall` |
| Low | Deeply nested replies from a master overflowed the stack. | `StackOverflowError` | Rejected beyond depth 64 | `RespReaderHardeningTest.deeplyNestedRepliesAreRejected` |
| Low | `APPEND` could grow a value without limit. | Unbounded | Capped at `proto-max-bulk-len` | `CommandHardeningTest.appendCannotGrowPastTheValueLimit` |
| Low | `SAVE` serialized the whole dataset while holding the command lock. | Server paused for the whole save | Only references are copied under the lock | Only the proof of concept: a timing test would be flaky |

Smaller fixes from the same review: a second `PSYNC` on one connection is
refused; integers must be written canonically (`+1` or `01` used to be
accepted); `FLUSHALL` rejects arguments it doesn't know instead of
flushing anyway; huge `EXPIRE` values are rejected instead of wrapping into
the past; client-supplied text is cleaned before it reaches the log, so a
client can't forge log lines; and TLS was added so passwords and data don't
have to cross the network in clear text.

### Found later, while making it practical

- **Shutdown could lose acknowledged writes.** The final save took its
  snapshot, then other clients kept running commands until the connections
  closed. A test writing a counter during `SHUTDOWN SAVE` saw up to a
  thousand acknowledged values missing from the dump, 3 runs out of 3. The
  command lock is now held from the snapshot until every connection is
  closed. (`AutoSaveTest.shutdownKeepsEveryAcknowledgedWrite`)
- **SIGTERM lost its own logs**, a failed final save included, and exited
  with 143 even after a good save. Signals are now handled the way Redis
  handles them: exit 0 after a good save, keep running if the save fails,
  and a second Ctrl-C exits at once. (`AutoSaveTest`, in a separate JVM)
- **A client accepted while the server was closing** could be served after
  shutdown. The accept loop now closes it.

### What isn't covered

- **One password, no users.** Every authenticated client can do everything,
  `FLUSHALL` and `SHUTDOWN` included. There are no ACLs.
- **Authenticated clients can still slow everyone down.** `KEYS *` on a big
  keyspace or a huge `MGET` holds the command lock while it runs, as in Redis.
- **`maxmemory` is an estimate** of what the keys cost on the JVM heap. The
  real heap can run out first. The Docker image runs the JVM with
  `-XX:+ExitOnOutOfMemoryError`, so it restarts from the last snapshot rather
  than carry on in an unknown state. Do the same outside Docker.
- **Replicas trust their master.** `masterauth` proves the replica to the
  master, not the other way round. With `tls-replication yes` and a CA file,
  the replica checks the master's certificate, including its host name.
- **TLS:** no certificate revocation checks, no reloading certificates
  without a restart, and the private key must be an unencrypted PKCS#8 file
  protected by file permissions.
- **Snapshots aren't encrypted.** They're created with mode 0600.
- **This review was done once, by one reviewer** (an AI assistant, working
  with proofs of concept and tests). It found real problems, which is a
  reason to expect it missed others. It isn't a substitute for an
  independent audit.
