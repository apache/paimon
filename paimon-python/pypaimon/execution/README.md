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

# Execution implementations

PyPaimon's public builders and table operations remain in `pypaimon.read`,
`pypaimon.write` and `pypaimon.table`. Their signatures and configuration are
independent of the implementation that executes an operation.

```text
pypaimon/
  read/                       Public read/scan APIs and shared read contracts
  write/                      Public write/commit/update APIs and shared inputs
  execution/
    dispatch.py               Preparation and safe backend selection
    read_dispatch.py          Batch/stream planning and read selection
    read_support.py           Shared Arrow output, resource and dataset adapters
    read_explain.py           Explain planning and counter aggregation
    search_dispatch.py        Search selection and lazy transported-plan reads
    write_dispatch.py         Writer selection, diagnostics and safe switching
    update_dispatch.py        Update, delete, upsert and MERGE coordination
    index_dispatch.py         Index build/drop selection
    python/
      read/                   Python scanning, file readers and merge execution
                              Format/object file I/O and system-table filtering
        search/               Vector, text, hybrid search and candidate filtering
      write/                  Python writers, committers and update execution
                              Format-table writing and explicit cleanup
      globalindex/            Python index readers, writers, build and evaluation
        ray_build.py          Driver coordination and Python shard workers
      merge_engine_dispatch.py Python merge-function selection for read and write
    native/
      runtime.py              Lazy runtime discovery and process safety
      table.py                Rust table/configuration bridge
      read/                   Native scan/read/search adapters
      write/                  Native write/commit/update/index adapters
    vindex_input.py           Shared vector-index streams and I/O callbacks
```

## Dependencies

Both implementations consume shared requests, predicates, schemas, plans, splits,
commit messages and input adapters. These contracts stay outside either backend.
Neither implementation imports the other. Public APIs retain configuration and
validation and delegate execution without inheriting either backend. Explicit
delegates retain the public object as the request context so subclass and instance
overrides continue to run. Python scanners, readers, writers and committers
are created only when Python execution is selected. Direct access to a Python
scanner, writer router or committer through an existing public property explicitly
selects that implementation. Attribute inspection and configuration getters do
not select Python. Public index entry points, interfaces, options, results and
shared metadata remain in `globalindex`; shared search plans and splits remain in
`table/source`. Python index file codecs, compression and reader/writer helpers
live in `execution/python/globalindex`. Vector-index stream ownership and I/O
callbacks are shared by both backends through `execution/vindex_input.py`.
Public Format, Object and System table classes keep their existing paths and
delegate file enumeration, decoding and filtering to Python read execution.
Native system-table planning uses `execution/native/read/system_table.py`.
Scan authorization and request validation remain shared; Python file-scanner
pruning, sharding and slicing execute under `execution/python/read`.

Native adapters convert requests and results and invoke `pypaimon_rust`. General
execution algorithms belong in Rust Core. The optional extension is loaded only
when a Native operation is requested. Importing the Python implementation does
not require or initialize it.

Python schema evolution uses Python cast rules and builds Arrow read schemas
directly, including runtime child nullability. It does not select a cast engine
based on whether the Native extension is installed.
The shared Arrow write validator also checks nested nullability directly in
Python. It never probes the Native extension; visible NULLs in required children
are rejected, while values hidden by NULL parents retain their existing meaning.
Table metadata and FileIO classes load Python row-key extractors and BLOB writers
only when their corresponding Python operation is invoked.

Ray and Daft input grouping share bucket arithmetic, binary-row size estimation
and fixed/dynamic hash routing in `write/bucket_planning.py` and
`write/input_routing.py`. These operators hold input configuration and partition
counts, with no file scanner, reader, writer or index maintainer. Native postpone
and dynamic bucket coordinators load existing buckets from Rust's complete file
plan, pinned to the restore snapshot and partition. Dynamic assignment still
requires a complete HASH index for existing data buckets. Python coordinators
use their Python metadata loader. Selection finishes before assigning input
keys. Python's Arrow grouping capability probe runs only when its grouping
implementation is used.

## Selection and failure boundaries

Planning, reading, writing and committing retain their independent options. A
Native plan can feed a Python reader, and Python writers can prepare messages for
a Native committer. Transportable `Plan`, `Split` and `CommitMessage` contracts
connect these stages.

`dispatch.py` owns preparation fallbacks for writers, committers and updates.
The writer coordinator in `write_dispatch.py` holds the selected executor and
any safe preparation fallback. Native writers only adapt inputs/results and
invoke Rust; they neither construct nor retain a Python writer. Update and index
coordination likewise lives outside both backends. Python index builders own
their construction methods; the public index builder no longer exposes private
Python writer hooks, and Ray workers receive the Python executor directly.
MERGE selects Python only for an explicit unsupported result, because preparing
it may consume user input. Native read/scan adapters keep their existing
capability checks and preparation boundaries. An empty successful result is distinct from an
unsupported operation. Fork safety errors always propagate.

Reader iteration errors end that read. Once a writer has accepted its first
mutation, it cannot switch to a Python writer. Actual update, MERGE and snapshot
publication calls run outside preparation fallback handlers. A failure may occur
after a write or publication succeeded, so execution is never replayed through
another implementation and prepared files are preserved.

The shared search coordinator keeps a lazy Python factory for transported plans
or Ray split workers. Native search adapters contain only Rust handles and plan
metadata; they do not construct or retain a Python executor. Creating a Native
scan or reader, executing its Native plan, and
serializing it do not construct that Python executor. Search execution errors
propagate without a second search through Python. Explicit split reads and Ray
split workers select their Python factory when invoked, not when their methods
or optional attributes are inspected.
Readers capture query vectors and options at construction for both the live
Native plan and a later transported-plan read.

Disabling Native options does not import Native adapters. Fork safety checks in
shared error handling inspect only an already loaded SDK, so Python streaming
prefetch failures cannot load Native execution. Independent stage options still
apply: a Python index builder may use a Native reader selected through the public
read API without importing that reader's implementation itself.

## Tests

Existing tests continue through the public APIs. Backend-specific tests patch
the implementation module where an operation executes. `execution_dispatch_test`
checks backend dependencies, lazy imports, empty results, preparation failures,
fork errors, public updater types and prevention of write replay.
The lazy read/search and execution lifecycle tests also reject Python executor
construction on Native paths. Global index boundary tests verify those imports
in fresh processes and preserve the exported public index contracts.
Strict import guards also exercise complete Python write/update/commit/index
workflows and Native read/scan/stream preparation, rejecting imports from the
other backend rather than only checking constructor mocks.
