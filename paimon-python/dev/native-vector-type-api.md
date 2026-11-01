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

# Native vector implementation API

The Python adapter expects the following method on all three
`pypaimon_rust.datafusion` builders: `VectorSearchBuilder`,
`BatchVectorSearchBuilder`, and `HybridSearchBuilder`.

```python
def with_vector_index_type(self, column: str, index_type: str) -> Self:
    ...
```

Expose equivalent typed state in the Rust core builders, rather than encoding
it as a table or query option. The Python adapter already calls this method;
the Rust implementation and binding are a separate change.

* `column` is the schema column name. `index_type` is a normalized implementation
  identifier such as `ivf-pq`, `ivf-flat`, or `diskann`.
* The method mutates builder configuration and returns the same builder, like
  `with_vector_column`. Calling it must not load a snapshot, plan or execute a
  search. Repeating it for a column replaces that column's previous value.
* Single and batch queries use the entry for the selected vector column.
  Hybrid queries use the entry for each vector route's column, independently of
  route order. Full-text routes do not consume it. Configuration may be supplied
  before routes are added.
* Persisted index metadata continues to determine the implementation for indexed
  ranges. The explicit value supplies the implementation for a raw search when
  no vector index metadata is available, matching the Java planner. An absent
  value leaves existing metadata/default behavior in place.
* Keep this state through scan/read construction, batch reader reconstruction,
  captured hybrid route builders, and distributed worker serialization.
  `RawVectorSearchSplit.index_type` remains a typed transport field.
* This API sets only the implementation. Resolved vector parameters still use
  `with_options`/route options; an unspecified metric must remain unspecified so
  persisted index metadata can provide it.
* Native code must stop interpreting `index_type`, `index-type`,
  `vector.index-type`, and `fields.<column>.index-type` as option aliases.
  Legitimate API argument names and index metadata fields named `index_type`
  remain supported.

Python resolves the implementation once through `VectorIndexOptions`, using
`global-index.<type>.index-columns` (or singular `index-column`) and the retained
legacy PK key `fields.<column>.pk-vector.index.type`. It strips the four removed
aliases before transferring schema or query options to older SDKs.

When an implementation is configured and this method is unavailable, Python
returns to its own execution path before planning, reading, executing, or
capturing hybrid routes. SDK configuration and execution errors propagate;
there is no retry after execution. Queries with no configured implementation
can continue to use older SDKs and persisted index metadata.

## Rust verification

Cover single, batch, hybrid, scan/read, captured routes, and serialized workers
with both canonical and legacy PK declarations. Test raw searches without index
files with and without a metric, metadata taking precedence over the explicit
value, and independent columns in a hybrid query. The Python adapter contract
and old-SDK fallback are covered in `lazy_search_execution_test.py`; native
integration coverage is in `native_search_plan_test.py` and requires this API.
