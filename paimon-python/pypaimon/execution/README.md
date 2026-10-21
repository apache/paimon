<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Python and Native execution backends

`execution` contains the Python and Native implementations of corresponding
Paimon operations. A module belongs here because it implements one of those
backends, not merely because it performs work at runtime.

Keep this boundary when adding or moving code:

* `execution/python` implements Paimon scanning, file reading, merging, writing,
  updates, commits and indexes in Python.
* `execution/native` adapts the corresponding operations to `pypaimon_rust`.
  General Native execution algorithms belong in Rust Core.
* Public contracts, request validation, backend selection, Arrow/Pandas output
  adapters, shared input conversion and resource helpers stay in their owning
  `read`, `write`, `globalindex` or `table` modules.
* Ray and Torch worker scheduling, dataset/sink adapters, prefetch and shuffle
  stay with their framework integrations. They call the public table APIs and
  do not need duplicate Paimon backend implementations.
* Match Paimon capabilities, not filenames: Python reader/index helper modules
  can support a corresponding Rust operation without a same-named Rust file.
  Format/Object file readers have Rust Core counterparts and remain backends.

```text
pypaimon/
  read/                         Public read/scan APIs, Arrow output and explain
    read_dispatch.py            Read/scan selection across the two backends
    read_support.py             Shared output, schema and reader ownership
    read_explain.py             ExplainResult construction and rendering
    datasource/                 Public Ray/Torch framework adapters
  write/                        Public write/update/commit APIs and shared inputs
    backend_dispatch.py         Preparation, backend selection and commit coordination
    write_dispatch.py           Writer coordination and safe pre-write switching
    update_dispatch.py          Update/upsert/delete/MERGE coordination
    input_adapter.py            Pandas transport and distributed input conversion
    merge_condition.py          Shared MERGE condition parsing/evaluation
    merge_transform.py          Shared Arrow assignments and callable inputs
    ray_datasink.py              Ray worker writes and coordinator commits
  globalindex/                  Public index contracts and backend coordination
    index_dispatch.py           Index selection and Vindex resource ownership
    vindex_input.py             Python SDK/Native callback streams and ownership
  table/source/
    search_dispatch.py          Search selection and transported-plan coordination
  table/system/                 SystemSplit metadata and common Arrow output
  ray/
    vector_index_build.py       Ray index tasks, shard workers and failure draining
  execution/
    python/
      read/                     File scanners/readers and merge execution
        search/                 Vector/text/hybrid search and Python factories
      write/                    File writers, committers and update execution
      globalindex/              Index file codecs, readers, writers and build algorithms
      merge_engine_dispatch.py  Python merge-function selection
    native/
      runtime.py                Lazy SDK discovery and process safety
      table.py                  Rust table/configuration bridge
      read/                     Rust scan/read/search and system metadata adapters
      write/                    Rust write/commit/update/index adapters
```

## Public APIs and dependencies

Corresponding Paimon APIs keep configuration, validation and explicit delegation;
they do not inherit either backend. Public method signatures, defaults, class
paths and supported subclass/instance hooks remain stable. Shared input/result
contracts include schemas, predicates, plans, splits and commit messages.

Neither backend imports the other. Native adapters do not construct or retain
Python executors. Python implementations do not load the optional Native SDK
just because it is installed. Planning, reading, writing and committing retain
independent options; shared plans and commit messages connect those stages.
Explicit access to an existing Python scanner/router/committer property selects
that implementation. Ordinary configuration and metadata inspection do not.

Framework adapters keep their actual implementation in their original modules,
including their public class identities, serialization and worker closures.
Local and Native MERGE use shared `write` helpers without importing the public
Ray package. Public Ray helper entry points delegate to those same helpers.
System-table output applies common Arrow filtering/projection to `SystemSplit`
results from either metadata provider without creating a Python file reader.

## Corresponding execution APIs

Compare the operation's inputs, results and lifecycle when changing a backend.
The public facade and the Native bridge normalize Python objects and Rust FFI
types at that boundary.

| Operation | Python execution | Native execution / Rust Core |
| --- | --- | --- |
| Batch scan | `PythonTableScanMixin` | `NativeTableScanMixin` / `TableScan` |
| Split read | `PythonTableReadMixin` | `NativeTableReadMixin` / `TableRead` |
| Stream scan | `AsyncStreamingTableScan` | `StreamTableScan` / `StreamTableScan` |
| Write | `PythonTableWriteMixin` | `NativeTableWrite` / `TableWrite` |
| Commit | `PythonTableCommit` | `NativeTableCommit` / `TableCommit` |
| Row ID / predicate update, delete and key upsert | Python update executors | Native update adapters / `TableUpdate` |
| Shard update | `ShardTableUpdator` | `NativeShardTableUpdator` / `ShardTableUpdater` |
| Index build, drop and search | Python index/search executors | Native index/search adapters / Core index builders and readers |

Both commit adapters expose `commit(messages, commit_identifier,
snapshot_properties=None)`, `abort(messages)`,
`truncate_table(commit_identifier)`,
`truncate_partitions(partitions, commit_identifier)` and `close()`.
Message conversion happens during preparation; publication uses the selected
executor's prepared messages. Batch/stream binding details stay in the Native
adapter, and overwrite details stay in each backend.
Public stream write, update and commit entry points normalize integer-like
commit identifiers to Java `long` / Rust `i64` before execution starts.
Write-column selections reject empty selections and duplicate names before
replacing the current configuration. Index build/drop requests bind partition
predicates to the partition layout and reject predicates containing
non-partition fields.

Both stream scans expose `plan()`, `checkpoint()`,
`restore(next_snapshot_id=None)`, `watermark()`,
`notify_checkpoint_complete(next_snapshot)`, `stream()`, `stream_sync()` and
`close()`. A plan call checks available snapshots once. Consumer progress is
persisted explicitly or after the caller resumes a yielded plan. Cancelling a
yielded plan preserves its unacknowledged consumer position. Snapshot gaps must
preserve the configured changelog sequence.

Both shard updaters expose `arrow_reader()`, `update_by_arrow_batch(data)`,
`prepare_commit()` and `close()`. They track actual visible row IDs, require a
complete read/update cycle before preparation and preserve prepared files when
closed. This matters when deletion vectors hide physical rows.

Python mixins retain public instance hooks while Native adapters own Rust
handles. Private helper structure follows each implementation's algorithms.

The operation protocols do not imply identical backend capabilities. Python
Data Evolution reads still reject nested ROW field projections supported by
Rust. `Plan.stats()` is optional: ordinary Python planning returns no counters,
while Native planning always records lightweight counters. Python's explicit
`scan_with_stats()` / explain pass collects additional bookkeeping and can
change planning concurrency, so it is not enabled by ordinary `plan()` calls.

## Failure and resource boundaries

Only preparation can select a fallback backend. Reads fail without replaying
through another backend after iteration begins. A writer cannot switch after
accepting a mutation. Update, MERGE and commit publication calls execute outside
preparation fallback handlers, because an exception may follow a successful
write or publication. Preserve prepared files when an outcome is uncertain;
explicit `TableCommit.abort` remains for known abandoned, uncommitted files.

Close retained readers on completion, cancellation and failures. Cleanup errors
must not replace a primary read/conversion failure. Ray adapters close their
worker readers, and Torch prefetch producers observe cancellation while waiting
on a full queue. Vindex initialization/close support callback reentry without
publishing a candidate cancelled during initialization.

Tests cover process-isolated backend imports, public hooks and serialization,
empty results versus unsupported operations, source cleanup, fallback boundaries
and prevention of write replay or accidental deletion. Packaging tests check
that backend modules and framework integrations ship at their intended paths.
