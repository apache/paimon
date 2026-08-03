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

from pypaimon.table.bucket_mode import BucketMode
from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer
from pypaimon.table.row.internal_row import RowKind


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
            data_size = 0
            if collect_size:
                values = [column[row].as_py() for column in columns]
                data_size = len(GenericRowSerializer.to_bytes(
                    GenericRow(values, fields, RowKind.INSERT)
                )) - 4
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
                    (total_rows + self.target_row_num_per_bucket - 1)
                    // self.target_row_num_per_bucket,
                )
            else:
                estimated_size = data_size
                if postpone_rows and row_count:
                    estimated_size = (
                        data_size * (row_count + postpone_rows) + row_count - 1
                    ) // row_count
                num_buckets = max(
                    1,
                    (estimated_size + self.target_size_per_bucket - 1)
                    // self.target_size_per_bucket,
                )
            self._known_num_buckets[partition] = min(
                num_buckets, self.max_num_buckets
            )
        return self.current_plan()
