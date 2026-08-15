# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Distributed predicate updates with Arrow batch transforms."""

from typing import Callable, Dict, List, Optional

import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.ray.process_row_id_ranges import process_row_id_ranges
from pypaimon.ray.update_by_row_id import (
    _update_by_row_id,
    _validate_update_target,
)
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.special_fields import SpecialFields

__all__ = ["update_by_predicate"]


def update_by_predicate(
    target: str,
    predicate: Optional[Predicate],
    transform: Callable[[pa.Table], pa.Table],
    catalog_options: Dict[str, str],
    *,
    read_columns: List[str],
    update_cols: List[str],
    rows_per_commit: int,
    batch_size: Optional[int] = None,
    num_partitions: Optional[int] = None,
    ray_remote_args: Optional[Dict] = None,
) -> Dict[str, int]:
    """Update matching rows using an Arrow batch transform.

    ``transform`` receives ``read_columns`` and must return ``update_cols``.
    Row-id file groups are processed in bounded, sequential commits.
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory

    table = CatalogFactory.create(catalog_options).get_table(target)
    _validate(table, transform, read_columns, update_cols, batch_size)
    read_columns = list(dict.fromkeys(read_columns))
    update_cols = list(dict.fromkeys(update_cols))
    _validate_update_target(
        table, target, update_cols, operation="update_by_predicate"
    )

    row_id = SpecialFields.ROW_ID.name
    target_schema = PyarrowFieldParser.from_paimon_schema(
        table.table_schema.fields
    )
    update_schema = pa.schema(
        [pa.field(row_id, pa.int64())]
        + [target_schema.field(name) for name in update_cols]
    )
    total_updated = 0

    def process_ranges(ranges):
        nonlocal total_updated

        current = CatalogFactory.create(catalog_options).get_table(target)
        read_builder = current.new_read_builder().with_projection(
            read_columns + [row_id]
        )
        if predicate is not None:
            read_builder.with_filter(predicate)
        plan = read_builder.new_scan().with_row_ranges(ranges).plan()
        splits = plan.splits()
        source = read_builder.new_read().to_ray(splits)

        transform_args = {"batch_format": "pyarrow"}
        if batch_size is not None:
            transform_args["batch_size"] = batch_size
        updates = source.map_batches(
            _apply_transform,
            fn_kwargs={
                "transform": transform,
                "read_columns": read_columns,
                "update_cols": update_cols,
                "update_schema": update_schema,
            },
            **transform_args,
        )
        result = _update_by_row_id(
            target,
            updates,
            catalog_options,
            update_cols=update_cols,
            num_partitions=num_partitions,
            ray_remote_args=ray_remote_args,
            source_schema=update_schema,
            base_snapshot_id=plan.snapshot_id,
        )
        total_updated += result["num_updated"]

    process_row_id_ranges(
        target,
        catalog_options,
        rows_per_commit=rows_per_commit,
        processor=process_ranges,
    )
    return {"num_updated": total_updated}


def _apply_transform(
    batch, *, transform, read_columns, update_cols, update_schema
):
    transformed = transform(batch.select(read_columns))
    if not isinstance(transformed, pa.Table):
        raise TypeError("transform must return a pyarrow.Table.")
    missing = [
        name for name in update_cols if name not in transformed.column_names
    ]
    if missing:
        raise ValueError(
            "transform result is missing update columns {}.".format(missing)
        )
    if transformed.num_rows != batch.num_rows:
        raise ValueError(
            "transform must preserve the input row count: {} != {}.".format(
                transformed.num_rows, batch.num_rows
            )
        )
    result = pa.Table.from_arrays(
        [batch[SpecialFields.ROW_ID.name]]
        + [transformed[name] for name in update_cols],
        names=[SpecialFields.ROW_ID.name] + update_cols,
    )
    return result.cast(update_schema)


def _validate(table, transform, read_columns, update_cols, batch_size):
    if not callable(transform):
        raise ValueError("transform must be callable.")
    if not read_columns:
        raise ValueError("read_columns must be non-empty.")
    if not update_cols:
        raise ValueError("update_cols must be non-empty.")
    dependencies = set(read_columns) - set(update_cols)
    if dependencies:
        raise ValueError(
            "read_columns must also be updated to preserve conflict "
            "detection: {}.".format(sorted(dependencies))
        )
    for name in list(read_columns) + list(update_cols):
        if name not in table.field_names:
            raise ValueError("Column '{}' is not in table schema.".format(name))
    if batch_size is not None and (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size <= 0
    ):
        raise ValueError("batch_size must be a positive integer.")
