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
    python/
      read/                   Python scanning, file readers and merge execution
                              Format/object file I/O and system-table filtering
        search/               Vector, text, hybrid search and candidate filtering
      write/                  Python writers, committers and update execution
                              Format-table writing and explicit cleanup
      globalindex/            Python index readers, writers, build and evaluation
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
select an executor when needed. Python scanners, readers, writers and committers
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
MERGE selects Python only for an explicit unsupported result, because preparing
it may consume user input. Native read/scan adapters keep their existing
capability checks and preparation boundaries. An empty successful result is distinct from an
unsupported operation. Fork safety errors always propagate.

Reader iteration errors end that read. Once a writer has accepted its first
mutation, it cannot switch to a Python writer. Actual update, MERGE and snapshot
publication calls run outside preparation fallback handlers. A failure may occur
after a write or publication succeeded, so execution is never replayed through
another implementation and prepared files are preserved.

Native search keeps a lazy Python factory for transported plans or Ray split
workers. Creating a Native scan or reader, executing its Native plan, and
serializing it do not construct that Python executor. Search execution errors
propagate without a second search through Python. Explicit split reads and Ray
split workers select their Python factory when invoked, not when their methods
or optional attributes are inspected.

## Tests

Existing tests continue through the public APIs. Backend-specific tests patch
the implementation module where an operation executes. `execution_dispatch_test`
checks backend dependencies, lazy imports, empty results, preparation failures,
fork errors, public updater types and prevention of write replay.
The lazy read/search and execution lifecycle tests also reject Python executor
construction on Native paths. Global index boundary tests verify those imports
in fresh processes and preserve the exported public index contracts.
