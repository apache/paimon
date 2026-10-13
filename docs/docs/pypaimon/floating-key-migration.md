---
title: "Floating primary-key upgrades"
---

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

# Floating primary-key upgrades

Older Python writers could produce unordered FLOAT/DOUBLE keys, duplicate NaN keys, or discard a signed-zero key. Upgrading the reader alone does not repair these files.

## Upgrade requirements

PyPaimon requires `python.floating-key-order=java-v1` when the primary keys, after excluding partition columns (`trimmed primary keys`), contain FLOAT/DOUBLE, including in composite keys. Floating columns used only as sequence fields, value columns or partition keys do not trigger this requirement. Updated Python catalogs mark new tables; loading or altering an existing table does not add the option. Without it, Python rejects planning, reading and writing while allowing metadata inspection.

**Unmarked Java-created tables also require acknowledgement before Python access.** Missing acknowledgement does not imply corruption. Java readers and writers are unaffected; `java-v1` names the ordering contract, not a Java runtime version.

Whichever upgrade procedure you choose:

1. Stop old Python writers and keep them stopped after cutover; the option does not fence them.
2. Verify `null < -Infinity < ... < -0.0 < +0.0 < ... < +Infinity < NaN`, with all NaNs equal. Each file must have unique keys and valid bounds. Cross-file versions of a key are normal.
3. Include the snapshots, tags and branches you will read. Checking the latest snapshot alone does not certify the table's history. Rebuild incompatible data from physical records or a trusted source; keys already discarded by an old writer require another source.
4. Validate the resulting data and follow-up writes before switching consumers, including deletes and late events when using `sequence.field`. Rebuilding only live rows can discard deletion markers and allow older events to recreate deleted keys. Retain the source and plan checkpoints and historical access. After new destination writes, rollback requires reconciling those writes.

You can acknowledge data that already meets these requirements, such as verified Java-only history, without rewriting. Otherwise, rebuild into a new table from a trusted source or through an offline procedure that reads physical records without relying on the incompatible file order. Choose and validate a procedure for your table's merge semantics and ingestion pipeline; PyPaimon does not provide a migration tool.

After verification, acknowledge an existing table and reload it:

```python
from pypaimon.schema.schema_change import SchemaChange

catalog.alter_table("default.orders", [
    SchemaChange.set_option("python.floating-key-order", "java-v1")
])
table = catalog.get_table("default.orders")
```

Setting this option does not inspect or repair data. Runtime overrides such as `table.copy(...)` carry the same acknowledgement responsibility.
