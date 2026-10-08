# neo4rs 0.8.0 cancellation safety

Source: crates.io neo4rs 0.8.0 (MIT package license). Upstream authors, license
declaration, README and runtime source notices are retained. The published
archive does not include the license files linked from its README.
Runtime changes are limited to `src/connection.rs`, `src/graph.rs`, `src/pool.rs`,
`src/txn.rs`, `src/stream.rs`, `src/messages.rs`, and `src/types/duration.rs`.
The package manifest omits upstream example/integration
test targets, retaining the development dependencies needed by unit tests.

A cancelled Bolt send or receive can leave an unread response on a pooled
connection. Upstream recycling sends RESET and accepts the first SUCCESS, which
may belong to the cancelled query. Later queries can then receive empty or
misattributed results.

Track an outstanding response from before the first write until its terminal
response is fully parsed. Records do not end a PULL response. Recycling rejects
connections with unfinished messages, causing the pool to close and replace them.
Fully consumed successes and server failures retain ordinary RESET behavior.

Deadpool returns dropped objects directly to its idle queue; recycling occurs
only on a later checkout. A connection lease now removes dirty sockets from the
pool immediately on drop using `Object::take`. Explicit transactions also mark
their lease non-returnable from BEGIN until COMMIT or ROLLBACK is acknowledged.
Canceled transactions therefore close even between messages, without requiring
another checkout to release server resources. A lost COMMIT acknowledgement
still has unknown outcome; closing the socket does not prove rollback.

`Graph::start_txn_with_timeout` adds a positive server `tx_timeout` in BEGIN.
Local TCP regressions verify this metadata and immediate peer EOF while Graph
remains alive for a dropped open transaction, a timed-out PULL followed by
rejected ROLLBACK, and a timed-out COMMIT. A successfully acknowledged ROLLBACK
retains its clean pooled connection.
A disposable-Neo4j regression holds a write lock, times out a later PULL,
attempts rollback, and verifies a separate graph can acquire the lock while
the original Graph remains alive and performs no checkout. This test deliberately
uses BEGIN without a server timeout to isolate immediate lease closure.

PULL preserves `Error::Neo4j` for server FAILURE messages and preserves original
transport errors. BEGIN, COMMIT, ROLLBACK, and RESET also retain typed server
failures rather than converting them to `UnexpectedMessage`. DISCARD already
preserved these through its existing query error conversion. A fake Bolt-server
matrix verifies constraint-error codes survive BEGIN, PULL, DISCARD, COMMIT,
and ROLLBACK so adapters can distinguish known server rejection from an
ambiguous transport/protocol acknowledgement failure.

Sending another message while a response is pending is also rejected. In
particular, an adapter's ROLLBACK after timed-out transaction I/O must not read
the old request's SUCCESS and accidentally clear the pending flag. Driver
message sequences consume a terminal response before sending their next message;
they do not pipeline requests. Local TCP regression tests cover interrupted
PULL followed by ROLLBACK/RESET and ordinary sequential response handling.

`Graph::run_once` and `Graph::execute_once` bypass upstream's internal retry loop
so the adapter owns attempt limits and write ambiguity. Existing `run` and
`execute` behavior is preserved for other callers. A local fake Bolt server
returns a retryable failure to verify both once-only APIs return that failure.

`BoltDuration::as_seconds_f64` preserves signed duration components without
converting through unsigned `std::time::Duration`; the adapter tests negative
duration decoding.

Regression coverage: the Neo4j adapter's live cancellation tests, including
repeated pool reuse with known query results. Remove this patch only after an
upstream release provides equivalent cancellation safety and passes those tests.

`IGNORED` is decoded as a terminal Bolt response. If ROLLBACK is ignored after
an earlier FAILURE, the transaction sends RESET and returns its connection only
after RESET succeeds. A wire regression verifies the next query reuses that same
socket; interrupted messages still force immediate socket closure.

## Distribution license

This vendored dependency is distributed under its Apache-2.0 licensing option. The full license text is in LICENSE-APACHE. Upstream author metadata and existing source notices are retained.
