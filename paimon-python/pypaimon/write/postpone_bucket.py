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

from typing import Dict, Tuple

from pypaimon.schema.data_types import (
    ArrayType,
    AtomicType,
    MapType,
    MultisetType,
    RowType,
    VectorType,
)
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.table.row.generic_row import _parse_type_precision_scale


def _ceil_div(dividend, divisor):
    return (dividend + divisor - 1) // divisor


def _round_to_word(size):
    return _ceil_div(size, 8) * 8


class _BinaryRowSizeEstimator:
    """Calculates the Java internal binary size without serializing rows."""

    @classmethod
    def row_size(cls, values, fields):
        size = ((len(fields) + 71) // 64) * 8 + len(fields) * 8
        for value, field in zip(values, fields):
            size += cls._variable_size(value, field.type)
        return size

    @classmethod
    def _variable_size(cls, value, data_type):
        if isinstance(data_type, AtomicType):
            return cls._atomic_variable_size(value, data_type)
        if value is None:
            return 0
        if isinstance(data_type, ArrayType):
            return _round_to_word(cls._array_size(value, data_type.element))
        if isinstance(data_type, VectorType):
            return _round_to_word(
                4 + data_type.length * cls._primitive_width(data_type.element)
            )
        if isinstance(data_type, MapType):
            keys, values = cls._map_values(value)
            return _round_to_word(
                4
                + cls._array_size(keys, data_type.key)
                + cls._array_size(values, data_type.value)
            )
        if isinstance(data_type, MultisetType):
            keys, values = cls._map_values(value)
            return _round_to_word(
                4
                + cls._array_size(keys, data_type.element)
                + cls._array_size(values, AtomicType("INT", False))
            )
        if isinstance(data_type, RowType):
            return _round_to_word(
                cls.row_size(cls._row_values(value, data_type), data_type.fields)
            )
        raise ValueError("Unsupported data type: {}".format(data_type))

    @classmethod
    def _atomic_variable_size(cls, value, data_type):
        type_name = data_type.type.upper()
        if type_name.startswith(("DECIMAL", "NUMERIC")):
            precision, _ = _parse_type_precision_scale(data_type)
            return 16 if precision > 18 else 0
        if type_name.startswith("TIMESTAMP"):
            precision, _ = _parse_type_precision_scale(data_type)
            return 8 if precision > 3 else 0
        if value is None:
            return 0
        if type_name == "VARIANT":
            value_bytes, metadata = cls._variant_bytes(value)
            return _round_to_word(4 + len(value_bytes) + len(metadata))
        if type_name == "BLOB":
            value = value if isinstance(value, (bytes, bytearray)) else value.to_data()
            return cls._binary_size(value)
        if type_name.startswith(("CHAR", "VARCHAR", "STRING")):
            return cls._binary_size(str(value).encode("utf-8"))
        if type_name.startswith(("BINARY", "VARBINARY", "BYTES")):
            return cls._binary_size(value)
        return 0

    @classmethod
    def _array_size(cls, values, element_type):
        values = list(values)
        header_size = 4 + ((len(values) + 31) // 32) * 4
        size = _round_to_word(
            header_size + len(values) * cls._fixed_width(element_type)
        )
        return size + sum(
            cls._variable_size(value, element_type)
            for value in values
            if value is not None
        )

    @staticmethod
    def _binary_size(value):
        length = len(bytes(value))
        return 0 if length <= 7 else _round_to_word(length)

    @staticmethod
    def _variant_bytes(value):
        if isinstance(value, dict):
            return bytes(value["value"]), bytes(value["metadata"])
        return bytes(value.value()), bytes(value.metadata())

    @staticmethod
    def _map_values(value):
        items = value.items() if isinstance(value, dict) else value
        items = list(items)
        return [item[0] for item in items], [item[1] for item in items]

    @staticmethod
    def _row_values(value, row_type):
        if isinstance(value, dict):
            return [value[field.name] for field in row_type.fields]
        if hasattr(value, "values"):
            return value.values
        return list(value)

    @classmethod
    def _fixed_width(cls, data_type):
        if isinstance(data_type, AtomicType):
            type_name = data_type.type.upper()
            if type_name in ("BOOLEAN", "BOOL", "TINYINT", "BYTE"):
                return 1
            if type_name in ("SMALLINT", "SHORT"):
                return 2
            if type_name in ("INT", "INTEGER", "FLOAT", "REAL", "DATE", "TIME") \
                    or type_name.startswith("TIME("):
                return 4
            return 8
        if isinstance(
                data_type,
                (ArrayType, MapType, MultisetType, RowType, VectorType)):
            return 8
        raise ValueError("Unsupported array element type: {}".format(data_type))

    @staticmethod
    def _primitive_width(data_type):
        type_name = data_type.type.upper()
        if type_name in ("BOOLEAN", "TINYINT"):
            return 1
        if type_name == "SMALLINT":
            return 2
        if type_name in ("INT", "INTEGER", "FLOAT"):
            return 4
        if type_name in ("BIGINT", "DOUBLE"):
            return 8
        raise ValueError("Unsupported vector element type: {}".format(data_type))


class PostponeBucketPlan:
    """Resolved bucket counts by partition."""

    def __init__(self, num_buckets: Dict[Tuple, int]):
        self._num_buckets = dict(num_buckets)

    def contains(self, partition: Tuple) -> bool:
        return tuple(partition) in self._num_buckets

    def num_buckets(self, partition: Tuple) -> int:
        partition = tuple(partition)
        if partition not in self._num_buckets:
            raise ValueError("Missing bucket plan for partition {}".format(partition))
        return self._num_buckets[partition]

    def as_dict(self) -> Dict[Tuple, int]:
        return dict(self._num_buckets)


class PostponeBucketPlanner:
    """Plans fixed bucket counts for postpone batch writes."""

    def __init__(
        self,
        table,
        known_num_buckets=None,
        postpone_row_counts=None,
    ):
        options = table.options
        if options.bucket() != BucketMode.POSTPONE_BUCKET.value:
            raise ValueError(
                "Postpone fixed bucket writes require bucket = -2, got {}"
                .format(options.bucket())
            )
        bucket_function = str(
            table.table_schema.options.get("bucket-function.type", "default")
        ).strip().lower()
        if bucket_function != "default":
            raise ValueError(
                "Postpone fixed bucket writes only support "
                "bucket-function.type=default, got {}"
                .format(bucket_function)
            )

        self.max_num_buckets = (
            options.postpone_batch_write_fixed_bucket_max_parallelism()
        )
        if self.max_num_buckets <= 0:
            raise ValueError(
                "postpone.batch-write-fixed-bucket.max-parallelism must be "
                "positive, got {}".format(self.max_num_buckets)
            )
        self.target_row_num_per_bucket = (
            options.postpone_target_row_num_per_bucket()
        )
        if self.target_row_num_per_bucket is not None:
            if self.target_row_num_per_bucket <= 0:
                raise ValueError(
                    "postpone.target-row-num-per-bucket must be positive, "
                    "got {}".format(self.target_row_num_per_bucket)
                )
            self.target_size_per_bucket = None
        else:
            self.target_size_per_bucket = (
                options.postpone_target_size_per_bucket()
            )
            if self.target_size_per_bucket <= 0:
                raise ValueError(
                    "postpone.target-size-per-bucket must be positive, got "
                    "{}".format(self.target_size_per_bucket)
                )

        self._partition_keys = list(table.partition_keys)
        self._field_dict = dict(table.field_dict)
        if known_num_buckets is None:
            known_num_buckets, loaded_postpone_counts = (
                self._load_bucket_metadata(table)
            )
            if postpone_row_counts is None:
                postpone_row_counts = loaded_postpone_counts
        self._known_num_buckets = dict(known_num_buckets)
        self._postpone_row_counts = dict(postpone_row_counts or {})

    @staticmethod
    def _load_bucket_metadata(table):
        scan = table.new_read_builder().new_scan().file_scanner
        manifest_files, _ = scan.manifest_scanner()
        entries = scan.manifest_file_manager.read_entries_parallel(
            manifest_files,
            max_workers=table.options.scan_manifest_parallelism(),
        )

        known = {}
        postpone_counts = {}
        for entry in entries:
            partition = tuple(entry.partition.values)
            if entry.bucket == BucketMode.POSTPONE_BUCKET.value:
                postpone_counts[partition] = (
                    postpone_counts.get(partition, 0) + entry.file.row_count
                )
            elif entry.bucket >= 0 and entry.total_buckets > 0:
                previous = known.get(partition)
                if previous is not None and previous != entry.total_buckets:
                    raise RuntimeError(
                        "Partition {} has different total buckets {} and {}"
                        .format(partition, previous, entry.total_buckets)
                    )
                known[partition] = entry.total_buckets
        return known, postpone_counts

    def current_plan(self) -> PostponeBucketPlan:
        return PostponeBucketPlan(self._known_num_buckets)

    def input_partition_stats(self, data) -> Dict[Tuple, Tuple[int, int]]:
        if self._partition_keys:
            columns = [data.column(key) for key in self._partition_keys]
            partitions = [
                tuple(column[row].as_py() for column in columns)
                for row in range(data.num_rows)
            ]
        else:
            partitions = [()] * data.num_rows

        stats = {}
        fields = [self._field_dict[name] for name in data.schema.names]
        columns = [data.column(i) for i in range(len(fields))]
        collect_size = self.target_row_num_per_bucket is None
        for row, partition in enumerate(partitions):
            if partition in self._known_num_buckets:
                continue
            data_size = 0
            if collect_size:
                values = [column[row].as_py() for column in columns]
                data_size = _BinaryRowSizeEstimator.row_size(values, fields)
            row_count, total_size = stats.get(partition, (0, 0))
            stats[partition] = (row_count + 1, total_size + data_size)
        return stats

    def plan(
        self,
        partition_stats: Dict[Tuple, Tuple[int, int]],
        include_postpone_rows: bool = True,
    ) -> PostponeBucketPlan:
        for partition, (row_count, data_size) in partition_stats.items():
            partition = tuple(partition)
            if partition in self._known_num_buckets:
                continue
            postpone_rows = (
                self._postpone_row_counts.get(partition, 0)
                if include_postpone_rows
                else 0
            )
            if self.target_row_num_per_bucket is not None:
                total_rows = row_count + postpone_rows
                num_buckets = max(
                    1,
                    _ceil_div(
                        total_rows, self.target_row_num_per_bucket),
                )
            else:
                estimated_size = data_size
                if postpone_rows and row_count:
                    estimated_size = _ceil_div(
                        data_size * (row_count + postpone_rows), row_count)
                num_buckets = max(
                    1,
                    _ceil_div(
                        estimated_size, self.target_size_per_bucket),
                )
            self._known_num_buckets[partition] = min(
                num_buckets, self.max_num_buckets
            )
        return self.current_plan()
