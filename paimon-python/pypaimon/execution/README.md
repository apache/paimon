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
        search/               Vector, text, hybrid search and candidate filtering
      write/                  Python writers, committers and update execution
      globalindex/            Python index readers, writers, build and evaluation
    native/
      runtime.py              Lazy runtime discovery and process safety
      table.py                Rust table/configuration bridge
      read/                   Native scan/read/search adapters
      write/                  Native write/commit/update/index adapters
```

## Dependencies

Both implementations consume shared requests, predicates, schemas, plans, splits,
commit messages and input adapters. These contracts stay outside either backend.
Neither implementation imports the other. Public APIs retain configuration and
select an executor when needed. Python scanners, readers, writers and committers
are created only when Python execution is selected. Direct access to a Python
scanner or committer through an existing public property explicitly selects that
implementation. Shared index interfaces, options, results and wire formats remain
in `globalindex`; shared search plans and splits remain in `table/source`.

Native adapters convert requests and results and invoke `pypaimon_rust`. General
execution algorithms belong in Rust Core. The optional extension is loaded only
when a Native operation is requested. Importing the Python implementation does
not require or initialize it.

Ray input grouping shares bucket arithmetic, binary-row size estimation and
hash routing in `write/bucket_planning.py` and `write/input_routing.py`. These
operators hold input configuration and partition counts, with no file scanner,
reader or writer. The Native postpone coordinator loads existing bucket counts
from Rust's complete file plan; the Python coordinator uses its Python metadata
loader. This selection finishes before Ray materializes or writes input.

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
propagate without a second search through Python.

## Tests

Existing tests continue through the public APIs. Backend-specific tests patch
the implementation module where an operation executes. `execution_dispatch_test`
checks backend dependencies, lazy imports, empty results, preparation failures,
fork errors, public updater types and prevention of write replay.
The lazy read/search and execution lifecycle tests also reject Python executor
construction on Native paths. Global index boundary tests verify those imports
in fresh processes and preserve old serialized class paths.
