################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Pre-repartition a Ray Dataset by (partition, bucket) before writing
to a Paimon table.

Without this, Ray's default round-robin block distribution scatters rows
that share the same (partition, bucket) across many Ray tasks. Each
task then opens its own writer and emits its own data file, producing
``partitions × buckets × ray_tasks`` files instead of the
``partitions × buckets`` the writer would naturally produce.

For HASH_FIXED tables we group rows by ``(partition_keys..., bucket)``
so every distinct group lands in a single Ray task. ``bucket`` is
computed using the same ``FixedBucketRowKeyExtractor`` the writer
uses, so the bucket assignment seen by the groupby is byte-equivalent
to the writer's. HASH_FIXED writes are always pre-clustered; no user
opt-in is required.

For any other bucket mode the dataset is returned unchanged.
"""

import uuid
from typing import TYPE_CHECKING, List

import pyarrow as pa

from pypaimon.table.bucket_mode import BucketMode

if TYPE_CHECKING:
    import ray.data

    from pypaimon.table.table import Table

# Default transient column name. A collision-safe variant is picked at
# runtime by ``_pick_bucket_col_name`` so user tables that happen to
# contain a column with this name still work correctly.
BUCKET_KEY_COL = "__paimon_bucket__"


def _pick_bucket_col_name(existing_names) -> str:
    """Return a bucket column name guaranteed not to collide with
    ``existing_names``. Falls back to a UUID suffix on collision."""
    if BUCKET_KEY_COL not in existing_names:
        return BUCKET_KEY_COL
    while True:
        candidate = "__paimon_bucket_{}_".format(uuid.uuid4().hex[:8])
        if candidate not in existing_names:
            return candidate


def maybe_apply_repartition(
        dataset: "ray.data.Dataset",
        table: "Table",
) -> "ray.data.Dataset":
    """Cluster rows by ``(partition_keys..., bucket)`` for HASH_FIXED tables.

    For any other bucket mode the dataset is returned unchanged.
    HASH_FIXED writes are always pre-clustered, with no user opt-in
    required.
    """
    if table.bucket_mode() != BucketMode.HASH_FIXED:
        return dataset

    partition_keys = list(table.table_schema.partition_keys or [])
    extractor = table.create_row_key_extractor()
    col_names = set(f.name for f in table.table_schema.fields)
    bucket_col = _pick_bucket_col_name(col_names)
    bucket_udf = _make_bucket_udf(extractor, bucket_col)

    ds_with_bucket = dataset.map_batches(
        bucket_udf, batch_format="pyarrow", zero_copy_batch=True,
    )
    group_keys: List[str] = partition_keys + [bucket_col]
    grouped = ds_with_bucket.groupby(group_keys)
    regrouped = grouped.map_groups(_identity_batch, batch_format="pyarrow")
    return regrouped.drop_columns([bucket_col])


def _identity_batch(batch: pa.Table) -> pa.Table:
    # Some Ray versions promote ``string`` to ``large_string`` (and
    # ``binary`` to ``large_binary``) while materialising blocks for
    # ``groupby().map_groups``. Paimon's writer compares schemas with a
    # strict ``!=`` and rejects the large variants, so coerce them back
    # to the regular types here. Other Arrow types pass through.
    return _coerce_large_string_types(batch)


def _coerce_large_string_types(batch: pa.Table) -> pa.Table:
    needs_cast = False
    fields = []
    for field in batch.schema:
        if pa.types.is_large_string(field.type):
            fields.append(field.with_type(pa.string()))
            needs_cast = True
        elif pa.types.is_large_binary(field.type):
            fields.append(field.with_type(pa.binary()))
            needs_cast = True
        else:
            fields.append(field)
    return batch.cast(pa.schema(fields)) if needs_cast else batch


def _make_bucket_udf(extractor, bucket_col):
    """Build a map_batches UDF that appends a transient bucket column.

    The bucket value comes from ``extract_partition_bucket_batch`` so it
    matches the writer's bucket assignment for the same row exactly.
    """
    def _udf(batch: pa.Table) -> pa.Table:
        if batch.num_rows == 0:
            return batch.append_column(
                bucket_col, pa.array([], type=pa.int32())
            )
        record_batch = batch.combine_chunks().to_batches()[0]
        _, buckets = extractor.extract_partition_bucket_batch(record_batch)
        return batch.append_column(
            bucket_col, pa.array(buckets, type=pa.int32())
        )

    return _udf
