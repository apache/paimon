---
title: "Data Types"
description: "Use these mappings when defining a `pyarrow.Schema` for a Paimon table."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

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

# Data Types

Use these mappings when defining a `pyarrow.Schema` for a Paimon table. See [Catalogs and Tables](./catalogs#create-table) for schema creation and [BLOB Storage](./blob) for payload reads.

## Scalar Types

| Python Native Type  | PyArrow Type                           | Paimon Type                       |
|:--------------------|:---------------------------------------|:----------------------------------|
| `int`               | `pyarrow.int8()`                       | `TINYINT`                         |
| `int`               | `pyarrow.int16()`                      | `SMALLINT`                        |
| `int`               | `pyarrow.int32()`                      | `INT`                             |
| `int`               | `pyarrow.int64()`                      | `BIGINT`                          |
| `float`             | `pyarrow.float32()`                    | `FLOAT`                           |
| `float`             | `pyarrow.float64()`                    | `DOUBLE`                          |
| `bool`              | `pyarrow.bool_()`                      | `BOOLEAN`                         |
| `str`               | `pyarrow.string()`                     | `STRING`, `CHAR(n)`, `VARCHAR(n)` |
| `bytes`             | `pyarrow.binary()`                     | `BYTES`, `VARBINARY(n)`           |
| `bytes`             | `pyarrow.binary(length)`               | `BINARY(length)`                  |
| `bytes`             | `pyarrow.large_binary()`               | `BLOB`                            |
| `decimal.Decimal`   | `pyarrow.decimal128(precision, scale)` | `DECIMAL(precision, scale)`       |
| `datetime.datetime` | `pyarrow.timestamp(unit, tz=None)`     | `TIMESTAMP(p)` — unit: `'s'` p=0, `'ms'` p=1–3, `'us'` p=4–6, `'ns'` p=7–9 |
| `datetime.datetime` | `pyarrow.timestamp(unit, tz='UTC')`    | `TIMESTAMP_LTZ(p)` — same unit/p mapping as above                             |
| `datetime.date`     | `pyarrow.date32()`                     | `DATE`                            |
| `datetime.time`     | `pyarrow.time32('ms')`                 | `TIME(p)`                         |

## Complex Types

| Python Native Type | PyArrow Type                          | Paimon Type            |
|:-------------------|:--------------------------------------|:-----------------------|
| `list`             | `pyarrow.list_(element_type)`         | `ARRAY<element_type>`  |
| `dict`             | `pyarrow.map_(key_type, value_type)`  | `MAP<key, value>`      |
| `dict`             | `pyarrow.struct([field, ...])`        | `ROW<field ...>`       |

## VARIANT Type

`VARIANT` stores semi-structured, schema-flexible data (JSON objects, arrays, and primitives)
in the [Parquet Variant binary encoding](https://github.com/apache/parquet-format/blob/master/VariantEncoding).

pypaimon exposes VARIANT columns as Arrow `struct<value: binary NOT NULL, metadata: binary NOT NULL>` and
provides `GenericVariant` for encoding and decoding.

Paimon supports two Parquet storage layouts for VARIANT:

- **Plain VARIANT** — the standard two-field struct (`value` + `metadata`). Default for all writes.
- **Shredded VARIANT** — typed sub-columns are stored alongside overflow bytes, enabling column-skipping
  inside the Parquet file. Controlled by the `variant.shreddingSchema` table option.

<Tabs groupId="variant-read-write">

<TabItem value="plain-variant" label="Plain VARIANT">

**Read**

A VARIANT column arrives as `struct<value: binary, metadata: binary>` in every Arrow batch.
Use `GenericVariant.from_arrow_struct` to decode each row:

```python
from pypaimon.data.generic_variant import GenericVariant

read_builder = table.new_read_builder()
result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())

for record in result.to_pylist():
    if (payload := record["payload"]) is not None:
        gv = GenericVariant.from_arrow_struct(payload)
        print(gv.to_python())   # decode to Python dict / list / scalar
```

`from_arrow_struct` is a lightweight operation — it only wraps the two raw byte arrays without
parsing them. Actual variant binary decoding is deferred to `to_python()`.

**Write**

Build `GenericVariant` values and convert them to an Arrow column with `to_arrow_array`:

```python
import pyarrow as pa
from pypaimon.data.generic_variant import GenericVariant

gv1 = GenericVariant.from_python({'city': 'Beijing', 'age': 30})
gv2 = GenericVariant.from_python({'tags': [1, 2, 3], 'active': True})
# None represents SQL NULL

data = pa.table({
    'id':      pa.array([1, 2, 3], type=pa.int32()),
    'payload': GenericVariant.to_arrow_array([gv1, gv2, None]),
})

write_builder = table.new_batch_write_builder()
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()
table_write.write_arrow(data)
table_commit.commit(table_write.prepare_commit())
table_write.close()
table_commit.close()
```

</TabItem>

<TabItem value="shredded-variant" label="Shredded VARIANT">

In shredded mode the VARIANT column is physically split inside Parquet into a three-field group:

```
payload (GROUP)
├── metadata   BYTE_ARRAY          -- key dictionary (always present)
├── value      BYTE_ARRAY OPTIONAL -- overflow bytes for un-shredded fields
└── typed_value (GROUP) OPTIONAL
    ├── age    (GROUP)
    │   ├── value        BYTE_ARRAY OPTIONAL
    │   └── typed_value  INT64 OPTIONAL
    └── city   (GROUP)
        ├── value        BYTE_ARRAY OPTIONAL
        └── typed_value  BYTE_ARRAY OPTIONAL
```

**Read — automatic reassembly**

When pypaimon reads a Parquet file that contains shredded VARIANT columns (whether written by Paimon Java
or by pypaimon with shredding enabled), it **automatically detects and reassembles** them back to the
standard `struct<value, metadata>` form before returning any batch. No code changes are needed on the
read side:

```python
from pypaimon.data.generic_variant import GenericVariant

# Works identically for both shredded and plain Parquet files
read_builder = table.new_read_builder()
result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())

for record in result.to_pylist():
    if (payload := record["payload"]) is not None:
        gv = GenericVariant.from_arrow_struct(payload)   # same API as plain VARIANT
        print(gv.to_python())
```

Reassembly (reconstructing the variant binary from `typed_value` sub-columns and overflow bytes)
happens inside `FormatPyArrowReader.read_arrow_batch()` — that is, **at batch read time**, before
the Arrow data is returned to the caller. Note: When sub-field projection is active
(`with_variant_sub_fields`), reassembly is skipped entirely and only the requested typed
sub-columns are decoded.

**Write — shredding mode**

Set the `variant.shreddingSchema` table option to a JSON-encoded `ROW` type that describes which
sub-fields of which VARIANT columns to shred. The top-level fields map VARIANT column names to their
sub-field schemas:

```python
import json

shredding_schema = json.dumps({
    "type": "ROW",
    "fields": [
        {
            "id": 0,
            "name": "payload",          # VARIANT column name in the table
            "type": {
                "type": "ROW",
                "fields": [             # sub-fields to extract as typed columns
                    {"id": 0, "name": "age",  "type": "BIGINT"},
                    {"id": 1, "name": "city", "type": "VARCHAR"},
                ]
            }
        }
    ]
})

# Pass the option when creating the table
schema = Schema.from_pyarrow_schema(
    pa_schema,
    options={'variant.shreddingSchema': shredding_schema}
)
catalog.create_table('db.events', schema, ignore_if_exists=True)
```

Once the option is set, each `write_arrow` call transparently converts VARIANT columns to the shredded
Parquet layout. The read path — including Java Paimon and other engines — can then exploit the typed
sub-columns for column-skipping via sub-field projection.

Fields not listed in `variant.shreddingSchema` are stored in the overflow `value` bytes and remain
fully accessible on the read path.

Supported Paimon type strings for shredded sub-fields: `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`,
`BIGINT`, `FLOAT`, `DOUBLE`, `VARCHAR`, `BINARY`, `VARBINARY`, `DECIMAL(p,s)`, `ARRAY`, and nested
`ROW` types for recursive object shredding.

</TabItem>

</Tabs>

## VARIANT Path Updates

Read existing paths as Arrow arrays, use Arrow compute, and replace them
without decoding unrelated fields:

```python
import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.data import variant_get, variant_replace, variant_set

current = variant_get(payload, '$.velocity.y', pa.float64())
updated_payload = variant_replace(
    payload, '$.velocity.y', pc.negate(current))
```

The Arrow type must match the stored VARIANT type; these APIs do not cast
values. Exact extraction supports scalar types and nested struct, list, and
string-keyed map types. Replacement supports scalar types. Missing paths read
as NULL and remain unchanged unless `strict=True` is specified. Pass mappings
to process multiple paths in one pass.

`variant_set` upserts paths: existing paths are replaced like
`variant_replace`, and a missing final key is inserted when its parent path
exists and is an OBJECT:

```python
updated_payload = variant_set(payload, {
    '$.velocity.y': pc.negate(current),
    '$.processed': pa.scalar(True, type=pa.bool_()),
})
```

Values may be a `pa.Scalar` (broadcast to every row) or a `pa.Array` /
`pa.ChunkedArray` with one value per row; Arrow NULL values are stored as
VARIANT NULL and SQL NULL rows are preserved. `variant_set` raises
`ValueError` when an intermediate path is missing, when the parent of a
missing key is not an OBJECT, or for a missing array index — it never
creates intermediate objects or extends arrays.


**`GenericVariant` API:**

| Method | Description |
|:-------|:------------|
| `GenericVariant.from_python(obj)` | Build from a Python object (`dict`, `list`, `int`, `str`, …) |
| `GenericVariant.from_arrow_struct({"value": b"...", "metadata": b"..."})` | Wrap raw bytes from an Arrow VARIANT struct row (read path) |
| `GenericVariant.to_arrow_array([gv1, gv2, None, ...])` | Convert a list of `GenericVariant` (or `None`) to a `pa.StructArray` for writing |
| `gv.to_python()` | Decode to native Python (`dict`, `list`, `int`, `str`, `None`, …) |
| `gv.value()` | Return raw value bytes |
| `gv.metadata()` | Return raw metadata bytes |

**Limitations:**

- `VARIANT` is only supported with Parquet file format. ORC and Avro are not supported.
- `VARIANT` cannot be used as a primary key or partition key.
