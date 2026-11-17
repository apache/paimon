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

# Native index configuration

Rust owns index configuration parsing for native reads, writes, builds, and
searches. Python forwards the effective table options in the schema and the
explicit query or route options in the corresponding builder call. It preserves
option keys and field JSON, including canonical declarations, legacy PK
properties, and opaque implementation parameters.

The common FFI encoding converts scalar values to strings, booleans to lowercase
strings, and collections to JSON. A None table option represents an unset value.
Python preserves effective authorization and write-policy values at the generic
table boundary.

Global-index builds retain schema defaults separately from explicit build options,
so an alias in the build request can override a different alias in the schema.
A None build value removes that schema default. Typed build parallelism retains
Python's integer conversion before crossing the string-only ABI; other index
parameters are interpreted in Rust.

## Resolution contract

* The shared resolver returns the selected implementation together with its
  merged options. Java exposes `type()` and `options()`; Python exposes
  `index_type` and `options`. Rust uses the same configuration semantics.
* Support `global-index.<type>.index-columns`, singular `index-column`,
  type-level parameters such as `ivf-pq.dimension`, and short keys in
  `fields.<column>.index-options`. Retain legacy PK declarations, field types,
  field JSON, metrics, and DLF flat field parameters.
* Configuration is interpreted in Rust, including normalization, defaults,
  option precedence, per-column scalar settings, and full-text analyzers.
  Python does not flatten field JSON or convert canonical declarations into
  synthetic legacy PK declarations for native execution.
* PK declarations control source-backed index maintenance. Data Evolution
  declarations supply implementation and parameter defaults while retaining
  explicit global-index builds. Rust owns this table-mode distinction.
* Read valid existing index entries before choosing the implementation. Persisted
  metadata determines the algorithm for indexed data; use configuration for
  writes or raw fallback when no valid index metadata is available.
* Query and route options remain separate from table defaults. An unspecified
  metric stays unspecified in query options so persisted metadata can provide
  it. Each hybrid route retains its own options and column-specific analyzer.
* Preserve configuration through scan/read reconstruction, captured routes,
  build shards, and distributed worker serialization. Typed metadata fields
  such as `RawVectorSearchSplit.index_type` retain their existing meaning.
* The external type-hint aliases `index_type`, `index-type`, `vector.index-type`,
  and `fields.<column>.index-type` are unsupported. Python forwards options
  without interpreting or removing these keys; Rust must ignore them as type
  selectors. Legitimate API argument and metadata names remain supported.
* SDK configuration and execution errors propagate. A failed native operation
  must not trigger a second execution against a potentially newer snapshot.

## Verification

`native_index_options_test.py` checks lossless schema transport for reads and
writes, legacy PK/DLF declarations, per-field options, and generic encoding.
`lazy_search_execution_test.py` checks single, batch, hybrid, captured, and
serialized requests using standard builders and unmodified query options.
`global_index_build_test.py` checks that builds retain table and user options.

Real native integration in `native_search_plan_test.py` requires the corresponding
Rust shared configuration resolver. Cover raw queries without index files,
metadata precedence, independent hybrid columns/analyzers, and PK/DE maintenance
semantics. The Rust implementation is maintained separately.
