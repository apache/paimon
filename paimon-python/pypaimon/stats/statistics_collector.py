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

import logging
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.stats.col_stats import ColStats
from pypaimon.stats.statistics import Statistics

logger = logging.getLogger(__name__)


def _is_binary_like(arrow_type: pa.DataType) -> bool:
    return (pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)
            or pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type)
            or pa.types.is_fixed_size_binary(arrow_type))


def _is_binary_only(arrow_type: pa.DataType) -> bool:
    """Binary (not string) — min/max not meaningful for Java interop."""
    return pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type)


def _skip_min_max(arrow_type: pa.DataType) -> bool:
    """String and binary types do not get min/max (matches Java/Spark hasMinMax)."""
    return _is_binary_like(arrow_type)


def _is_analyzable_type(arrow_type: pa.DataType) -> bool:
    """Return True if the type supports column statistics (matches Spark's gate)."""
    return not (pa.types.is_struct(arrow_type) or pa.types.is_list(arrow_type) or
                pa.types.is_large_list(arrow_type) or pa.types.is_map(arrow_type) or
                pa.types.is_nested(arrow_type))


def _scalar_to_str(scalar: Any) -> Optional[str]:
    """Convert a pyarrow scalar to string in Java-compatible format.

    Java serialization:
    - DATE: epoch days (int)
    - TIME: millis of day (int)
    - TIMESTAMP: micros since epoch (long) for TIMESTAMP, or formatted string
    - Numeric types: str(value)
    - Binary: hex string
    """
    if scalar is None or not scalar.is_valid:
        return None
    val = scalar.as_py()
    if val is None:
        return None
    if isinstance(val, datetime):
        # Timestamp as formatted string (matches Java TimestampSerializer.serializeToString)
        if val.microsecond:
            return val.strftime("%Y-%m-%d %H:%M:%S.") + f"{val.microsecond:06d}".rstrip('0')
        return val.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(val, date):
        # Date as epoch days (matches Java IntSerializer for DATE)
        from datetime import date as date_cls
        epoch = date_cls(1970, 1, 1)
        return str((val - epoch).days)
    if isinstance(val, time):
        # Time as millis of day (matches Java IntSerializer for TIME)
        millis = (val.hour * 3600 + val.minute * 60 + val.second) * 1000 + val.microsecond // 1000
        return str(millis)
    if isinstance(val, bytes):
        return val.hex()
    if isinstance(val, Decimal):
        return str(val)
    return str(val)


class StatisticsCollector:
    """Collects column statistics by reading merged table data via the read path."""

    def __init__(self, table):
        self._table = table

    def collect(self, columns: Optional[List[str]] = None) -> Statistics:
        """Compute statistics by reading the full merged table.

        Args:
            columns: Column names to compute stats for. If None, all columns.

        Returns:
            A Statistics object ready to commit.
        """
        snapshot = self._table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return Statistics(
                snapshot_id=0, schema_id=self._table.table_schema.id,
                merged_record_count=0, merged_record_size=0, col_stats={},
            )

        schema = self._table.table_schema
        all_fields = schema.fields
        field_name_to_id = {f.name: f.id for f in all_fields}

        target_columns: List[str]
        if columns:
            for col in columns:
                if col not in field_name_to_id:
                    raise ValueError(f"Column '{col}' not found in table schema. "
                                     f"Available: {list(field_name_to_id.keys())}")
            target_columns = columns
        else:
            target_columns = [f.name for f in all_fields]

        # Read merged data
        read_builder = self._table.new_read_builder()
        scan = read_builder.new_scan()
        plan = scan.plan()
        splits = plan.splits()

        if not splits:
            return Statistics(
                snapshot_id=snapshot.id,
                schema_id=schema.id,
                merged_record_count=0,
                merged_record_size=0,
                col_stats={},
            )

        read = read_builder.new_read()
        accumulators: Dict[str, _ColumnAccumulator] = {}
        total_rows = 0
        total_size = 0

        arrow_table = read.to_arrow(splits)
        total_rows = arrow_table.num_rows
        total_size = arrow_table.nbytes

        for col_name in target_columns:
            if col_name not in arrow_table.column_names:
                continue
            col_array = arrow_table.column(col_name)
            if not _is_analyzable_type(col_array.type):
                logger.debug("Skipping column '%s' with unsupported type %s", col_name, col_array.type)
                continue
            acc = _ColumnAccumulator(col_name)
            acc.add_array(col_array)
            accumulators[col_name] = acc

        # Build ColStats
        col_stats: Dict[str, ColStats] = {}
        for col_name, acc in accumulators.items():
            col_id = field_name_to_id[col_name]
            col_stats[col_name] = acc.finalize(col_id)

        return Statistics(
            snapshot_id=snapshot.id,
            schema_id=schema.id,
            merged_record_count=total_rows,
            merged_record_size=total_size,
            col_stats=col_stats,
        )


class _ColumnAccumulator:
    """Accumulates statistics for a single column."""

    def __init__(self, name: str):
        self.name = name
        self._null_count = 0
        self._min_scalar = None
        self._max_scalar = None
        self._distinct_values: Optional[Set] = set()
        self._total_len = 0
        self._max_len_value = 0
        self._row_count = 0
        self._is_binary_like = False
        self._skip_min_max = False
        self._too_many_distinct = False

    def add_array(self, array: pa.ChunkedArray):
        self._is_binary_like = _is_binary_like(array.type)
        self._skip_min_max = _skip_min_max(array.type)

        for chunk in array.chunks:
            self._null_count += chunk.null_count
            self._row_count += len(chunk)

            valid = pc.drop_null(chunk)
            if len(valid) == 0:
                continue

            # min/max (skip for string and binary — matches Java/Spark hasMinMax)
            if not self._skip_min_max:
                chunk_min = pc.min(valid)
                chunk_max = pc.max(valid)
                if self._min_scalar is None or (chunk_min.is_valid and chunk_min < self._min_scalar):
                    self._min_scalar = chunk_min
                if self._max_scalar is None or (chunk_max.is_valid and chunk_max > self._max_scalar):
                    self._max_scalar = chunk_max

            # distinct count (exact up to 10M values, then give up)
            if not self._too_many_distinct and self._distinct_values is not None:
                unique_vals = pc.unique(valid)
                for val in unique_vals:
                    self._distinct_values.add(val.as_py())
                if len(self._distinct_values) > 10_000_000:
                    self._too_many_distinct = True
                    self._distinct_values = None

            # binary/string length stats
            if self._is_binary_like:
                lengths = pc.binary_length(valid)
                self._total_len += pc.sum(lengths).as_py() or 0
                chunk_max_len = pc.max(lengths)
                if chunk_max_len.is_valid:
                    self._max_len_value = max(self._max_len_value, chunk_max_len.as_py())

    def finalize(self, col_id: int) -> ColStats:
        distinct_count = None
        if self._distinct_values is not None:
            distinct_count = len(self._distinct_values)

        avg_len = None
        max_len = None
        if self._is_binary_like:
            non_null_count = self._row_count - self._null_count
            if non_null_count > 0:
                avg_len = self._total_len // non_null_count
            else:
                avg_len = 0
            max_len = self._max_len_value

        return ColStats(
            col_id=col_id,
            distinct_count=distinct_count,
            min=_scalar_to_str(self._min_scalar),
            max=_scalar_to_str(self._max_scalar),
            null_count=self._null_count,
            avg_len=avg_len,
            max_len=max_len,
        )
