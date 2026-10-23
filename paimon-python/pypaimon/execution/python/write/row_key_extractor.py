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
import random
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.index.dynamic_bucket import (
    MAX_DYNAMIC_BUCKETS,
    is_my_bucket,
    validate_max_buckets,
)
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.write.input_routing import (
    DynamicBucketInputRouter, FixedBucketInputRouter, PartitionKeyExtractor, PostponeFixedBucketInputRouter,
    _bucket_from_hash as _shared_bucket_from_hash, _hash_bytes_by_words as _shared_hash_bytes_by_words,
)

logger = logging.getLogger(__name__)
_hash_bytes_by_words = _shared_hash_bytes_by_words
_bucket_from_hash = _shared_bucket_from_hash


def _probe_arrow_group_by() -> bool:
    """Return True only if this pyarrow can run the write path's group-by.

    Two versions matter, and a plain ``hasattr(pa.Table, "group_by")`` conflates
    them: ``Table.group_by`` (Acero) landed in pyarrow 7.0.0, but the
    ``hash_list`` aggregate kernel this path relies on only landed in 8.0.0.
    pyarrow 7 therefore *has* ``group_by`` yet raises ``ArrowKeyError`` for
    ``hash_list`` -- and the Python 3.7 dependency range still permits
    ``pyarrow>=7,<13``. Probe on the first Python grouping operation so both
    pyarrow<7 (no ``group_by``) and pyarrow 7 (no ``hash_list``) fall through to
    the per-row grouping instead of failing every ``write_arrow_batch``.
    """
    if not hasattr(pa.Table, "group_by"):
        return False
    try:
        probe = pa.table({
            "__k": pa.array([0], type=pa.int32()),
            "__idx": pa.array([0], type=pa.int64()),
        })
        probe.group_by(["__k"]).aggregate([("__idx", "list")])
    except Exception:  # any failure here means "use the fallback"
        return False
    return True


# pyarrow < 7.0.0 has no ``Table.group_by`` and pyarrow 7 has no ``hash_list``
# aggregate kernel; on either the write path must use per-row grouping.
_ARROW_GROUP_BY_SUPPORTED = None


def _arrow_group_by_supported():
    # Public facades import the Pure implementation for their shared API, but
    # Native execution must not initialize Acero kernels just by importing it.
    global _ARROW_GROUP_BY_SUPPORTED
    if _ARROW_GROUP_BY_SUPPORTED is None:
        _ARROW_GROUP_BY_SUPPORTED = _probe_arrow_group_by()
    return _ARROW_GROUP_BY_SUPPORTED


class RowKeyExtractor(PartitionKeyExtractor, ABC):
    """Base class for extracting partition and bucket information from PyArrow data."""

    def extract_partition_bucket_batch(self, data: pa.RecordBatch) -> Tuple[List[Tuple], List[int]]:
        partitions = self._extract_partitions_batch(data)
        buckets = self._extract_buckets_batch(data)
        return partitions, buckets

    def extract_partitions_batch(self, data: pa.RecordBatch) -> List[Tuple]:
        """Return partition tuples without calculating bucket hashes."""
        return self._extract_partitions_batch(data)

    def extract_partition_bucket_groups(
            self, data: pa.RecordBatch) -> List[Tuple[Tuple, int, Optional[pa.Array]]]:
        """Group row indices by (partition, bucket) for the write path.

        Returns a list of ``(partition, bucket, row_indices)`` where
        ``row_indices`` is an Arrow ``int64`` array of the rows belonging to the
        group, or ``None`` when the whole batch is a single group (so callers can
        pass the original batch through without copying large values, e.g. BLOBs).

        The grouping is done in Arrow so only the distinct group keys are
        materialized into Python objects, instead of one ``.as_py()`` scalar per
        row. The old per-row loop held the GIL for the entire batch, which
        serialized multi-threaded writers down to ~1 core.

        Buckets are computed once here, in row order, via ``_extract_buckets_batch``
        so stateful extractors (dynamic bucket) keep their exact assignment
        sequence and side effects regardless of which grouping path runs.
        """
        buckets = self._extract_buckets_batch(data)
        if _arrow_group_by_supported():
            try:
                return self._group_indices_arrow(data, buckets)
            except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
                # Only Arrow's own "can't group this column type" errors fall
                # back to the legacy per-row grouping; any other exception is a
                # real bug and must propagate rather than silently degrade to the
                # GIL-bound path. `buckets` is reused (never recomputed) so
                # stateful extractors are not double-notified. Log so the
                # (GIL-bound) fallback is visible.
                logger.warning(
                    "Arrow group_by could not handle the partition/bucket key "
                    "types; falling back to per-row grouping (GIL-bound).",
                    exc_info=True)
        # pyarrow < 7.0.0 has no group_by; use the per-row grouping directly.
        return self._group_indices_python(data, buckets)

    def _group_indices_arrow(
            self, data: pa.RecordBatch,
            buckets: List[int]) -> List[Tuple[Tuple, int, Optional[pa.Array]]]:
        num_rows = data.num_rows
        columns = {}
        key_names = []
        for k, column in enumerate(self._input_columns(data, self.partition_indices)):
            name = f"__p{k}"
            columns[name] = column
            key_names.append(name)
        columns["__bucket"] = pa.array(buckets, type=pa.int32())
        key_names.append("__bucket")
        # Build the row index with numpy (C speed, releases the GIL). Using a
        # Python range() here makes pyarrow iterate it element by element under
        # the GIL, which dominates this method and kills multi-thread scaling.
        columns["__idx"] = pa.array(np.arange(num_rows, dtype=np.int64))

        grouped = pa.table(columns).group_by(key_names).aggregate([("__idx", "list")])
        num_groups = grouped.num_rows

        num_part = len(self.partition_indices)
        part_values = [grouped.column(f"__p{k}").to_pylist() for k in range(num_part)]
        bucket_values = grouped.column("__bucket").to_pylist()
        idx_lists = grouped.column("__idx_list")

        groups = []
        for gi in range(num_groups):
            partition = tuple(part_values[k][gi] for k in range(num_part))
            if num_groups == 1:
                row_indices = None
            else:
                # Arrow's threaded ``hash_list`` may return a group's indices out
                # of input order. The writer assigns sequence numbers in the
                # order it receives rows, so unordered indices let an earlier
                # input row (with a repeated primary key) win latest-wins
                # deduplication / partial update. Sort back to ascending input
                # order; np.sort runs in C (releases the GIL) so multi-threaded
                # scaling is preserved.
                row_indices = pa.array(
                    np.sort(idx_lists[gi].values.to_numpy(zero_copy_only=False)))
            groups.append((partition, bucket_values[gi], row_indices))
        return groups

    def _group_indices_python(
            self, data: pa.RecordBatch,
            buckets: List[int]) -> List[Tuple[Tuple, int, Optional[pa.Array]]]:
        partitions = self._extract_partitions_batch(data)
        num_rows = data.num_rows
        partition_bucket_groups = {}
        for i in range(num_rows):
            partition_bucket_groups.setdefault(
                (tuple(partitions[i]), buckets[i]), []).append(i)

        groups = []
        for (partition, bucket), row_indices in partition_bucket_groups.items():
            indices = None if len(row_indices) == num_rows \
                else pa.array(row_indices, type=pa.int64())
            groups.append((partition, bucket, indices))
        return groups

    @abstractmethod
    def _extract_buckets_batch(self, table: pa.RecordBatch) -> List[int]:
        """Extract bucket numbers for all rows. Must be implemented by subclasses."""

    @abstractmethod
    def _extract_bucket_row(self, values_by_name: Dict[str, Any]) -> int:
        """Extract bucket number for a single row."""


class FixedBucketRowKeyExtractor(FixedBucketInputRouter, RowKeyExtractor):
    """Fixed-bucket Python writer grouping using the shared input router."""


class UnawareBucketRowKeyExtractor(RowKeyExtractor):
    """Extractor for unaware bucket mode (bucket = -1, no primary keys)."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -1))

        if num_buckets != -1:
            raise ValueError(f"Unaware bucket mode requires bucket = -1, got {num_buckets}")

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        return [0] * data.num_rows

    def _extract_bucket_row(self, values_by_name: Dict[str, Any]) -> int:
        return 0


def _pick_randomly(bucket_list: List[int]) -> int:
    return random.choice(bucket_list)


class _SimplePartitionIndex:
    def __init__(self, num_assigners: int, assign_id: int, max_buckets_num: int) -> None:
        self.hash2bucket: Dict[int, int] = {}
        self.bucket_information: Dict[int, int] = {}
        self.bucket_list: List[int] = []
        self.current_bucket: int = 0
        self._load_new_bucket(max_buckets_num, num_assigners, assign_id)

    def assign(
        self,
        hash_value: int,
        max_bucket_id: int,
        target_bucket_row_number: int,
        max_buckets_num: int,
        num_assigners: int,
        assign_id: int,
    ) -> Tuple[int, int]:
        if hash_value in self.hash2bucket:
            assigned = self.hash2bucket[hash_value]
            return assigned, max(max_bucket_id, assigned)

        if self.current_bucket not in self.bucket_information:
            self.bucket_information[self.current_bucket] = 0
        num = self.bucket_information[self.current_bucket]

        if num >= target_bucket_row_number:
            if (
                max_buckets_num == -1
                or not self.bucket_information
                or max_bucket_id < max_buckets_num - 1
            ):
                self._load_new_bucket(max_buckets_num, num_assigners, assign_id)
            else:
                self.current_bucket = _pick_randomly(self.bucket_list)

        self.bucket_information[self.current_bucket] = (
            self.bucket_information.get(self.current_bucket, 0) + 1
        )
        self.hash2bucket[hash_value] = self.current_bucket
        new_max = max(max_bucket_id, self.current_bucket)
        return self.current_bucket, new_max

    def _load_new_bucket(
        self, max_buckets_num: int, num_assigners: int, assign_id: int
    ) -> None:
        for i in range(MAX_DYNAMIC_BUCKETS):
            if is_my_bucket(i, num_assigners, assign_id) and (
                i not in self.bucket_information
            ):
                if max_buckets_num == -1 or i <= max_buckets_num - 1:
                    self.current_bucket = i
                    self.bucket_list.append(i)
                    return
                return
        raise RuntimeError(
            "Can't find a suitable bucket to assign, all the bucket are assigned?"
        )


class SimpleDynamicBucketAssigner:
    def __init__(self, num_assigners, assign_id, target_bucket_row_number, max_buckets_num):
        validate_max_buckets(max_buckets_num)
        self.num_assigners = num_assigners
        self.assign_id = assign_id
        self.target_bucket_row_number = target_bucket_row_number
        self.max_buckets_num = max_buckets_num
        self.max_bucket_id = 0
        self._partition_index: Dict[Tuple, _SimplePartitionIndex] = {}

    def assign(self, partition: Tuple, hash_value: int) -> int:
        if partition not in self._partition_index:
            self._partition_index[partition] = _SimplePartitionIndex(
                self.num_assigners, self.assign_id, self.max_buckets_num)
        index = self._partition_index[partition]

        assigned, self.max_bucket_id = index.assign(
            hash_value,
            self.max_bucket_id,
            self.target_bucket_row_number,
            self.max_buckets_num,
            self.num_assigners,
            self.assign_id,
        )
        return assigned


class DynamicBucketRowKeyExtractor(DynamicBucketInputRouter, RowKeyExtractor):
    """Extract dynamic buckets and maintain their persistent hash mapping."""

    def __init__(
        self,
        table_schema: 'TableSchema',
        table=None,
        num_channels: int = 1,
        num_assigners: int = 1,
        assign_id: int = 0,
        ignore_existing: bool = False,
        maintain_index: bool = True,
        base_snapshot_id: Optional[int] = None,
    ):
        super().__init__(table_schema)
        opts = CoreOptions.from_dict(table_schema.options)
        self._table = table
        self.base_snapshot_id = 0
        target_bucket_row_number = opts.dynamic_bucket_target_row_num()
        max_buckets_num = opts.dynamic_bucket_max_buckets()

        if table is None:
            self._assigner = SimpleDynamicBucketAssigner(
                num_assigners=num_assigners,
                assign_id=assign_id,
                target_bucket_row_number=target_bucket_row_number,
                max_buckets_num=max_buckets_num,
            )
            self._index_maintainer = None
        else:
            from pypaimon.execution.python.write.dynamic_bucket import DynamicBucketIndexMaintainer
            from pypaimon.index.dynamic_bucket import DynamicBucketAssigner

            from pypaimon.index.dynamic_bucket import resolve_bucket_snapshot
            snapshot = resolve_bucket_snapshot(table, base_snapshot_id)
            self.base_snapshot_id = snapshot.id if snapshot is not None else 0
            self._assigner = DynamicBucketAssigner(
                table=table,
                num_channels=num_channels,
                num_assigners=num_assigners,
                assign_id=assign_id,
                target_bucket_row_number=target_bucket_row_number,
                max_buckets_num=max_buckets_num,
                ignore_existing=ignore_existing,
                snapshot=snapshot,
            )
            self._index_maintainer = (
                DynamicBucketIndexMaintainer(
                    table,
                    ignore_existing=ignore_existing,
                    snapshot=snapshot,
                )
                if maintain_index
                else None
            )

    def extract_partition_bucket_from_hashes_batch(
        self, data: pa.RecordBatch, key_hashes: List[int]
    ) -> Tuple[List[Tuple], List[int]]:
        """Assign buckets using key hashes calculated by an upstream stage."""
        partitions, buckets, _ = (
            self.extract_partition_bucket_status_from_hashes_batch(
                data, key_hashes
            )
        )
        return partitions, buckets

    def extract_partition_bucket_status_from_hashes_batch(
        self, data: pa.RecordBatch, key_hashes: List[int]
    ) -> Tuple[List[Tuple], List[int], List[bool]]:
        """Assign carried hashes and report whether each mapping is new."""
        from pypaimon.index.dynamic_bucket import to_signed_int32

        if len(key_hashes) != data.num_rows:
            raise ValueError(
                "Precomputed key hash count {} does not match row count {}".format(
                    len(key_hashes), data.num_rows
                )
            )
        partitions = self._extract_partitions_batch(data)
        normalized_hashes = [to_signed_int32(value) for value in key_hashes]
        if self._table is None:
            buckets = [
                self._assigner.assign(partition, key_hash)
                for partition, key_hash in zip(partitions, normalized_hashes)
            ]
            return partitions, buckets, [True] * len(buckets)

        partition_hash_cache = {}
        partition_hashes = []
        for partition in partitions:
            partition_hash = partition_hash_cache.get(partition)
            if partition_hash is None:
                partition_hash = self._binary_row_hash_code(
                    partition, self._partition_fields
                )
                partition_hash_cache[partition] = partition_hash
            partition_hashes.append(partition_hash)
        assignments = self._assigner.assign_batch(
            partitions, partition_hashes, normalized_hashes
        )
        buckets = [assignment[0] for assignment in assignments]
        new_mappings = [assignment[1] for assignment in assignments]
        if self._index_maintainer is not None:
            for partition, bucket, key_hash, is_new in zip(
                partitions, buckets, normalized_hashes, new_mappings
            ):
                if is_new:
                    self._index_maintainer.notify_new_record(
                        partition, bucket, key_hash
                    )
        return partitions, buckets, new_mappings

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        if self._table is None:
            partitions = self._extract_partitions_batch(data)
            key_hashes = self._extract_key_hashes_batch(data)
            return [
                self._assigner.assign(partition, key_hash)
                for partition, key_hash in zip(partitions, key_hashes)
            ]

        partitions, partition_hashes, key_hashes = self.extract_hashes_batch(data)
        assignments = self._assigner.assign_batch(
            partitions, partition_hashes, key_hashes
        )
        buckets = [assignment[0] for assignment in assignments]
        if self._index_maintainer is not None:
            for partition, key_hash, (bucket, is_new) in zip(
                partitions, key_hashes, assignments
            ):
                if is_new:
                    self._index_maintainer.notify_new_record(
                        partition, bucket, key_hash
                    )
        return buckets

    def _extract_bucket_row(self, values_by_name: Dict[str, Any]) -> int:
        key_hash = self._binary_row_hash_code(
            tuple(values_by_name[name] for name in self.bucket_keys),
            self._bucket_key_fields,
        )
        partition = tuple(
            values_by_name[self.table_schema.fields[i].name]
            for i in self.partition_indices
        )
        if self._table is None:
            return self._assigner.assign(partition, key_hash)
        partition_hash = self._binary_row_hash_code(
            partition, self._partition_fields
        )
        bucket, is_new = self._assigner.assign_with_status(
            partition, partition_hash, key_hash
        )
        if self._index_maintainer is not None and is_new:
            self._index_maintainer.notify_new_record(partition, bucket, key_hash)
        return bucket

    def prepare_commit(self):
        if self._index_maintainer is None:
            return {}
        return self._index_maintainer.prepare_commit()

    def notify_precomputed_bucket_batch(
        self, data: pa.RecordBatch, bucket: int
    ) -> Optional[Tuple]:
        """Maintain HASH indexes for every row in an upstream bucket."""
        from pypaimon.index.dynamic_bucket import to_signed_int32

        if self._index_maintainer is None:
            raise RuntimeError('Precomputed dynamic buckets require a persistent table extractor')
        partitions, _, key_hashes = self.extract_hashes_batch(data)
        if not partitions:
            return None
        partition = tuple(partitions[0])
        if any(tuple(actual_partition) != partition for actual_partition in partitions):
            raise RuntimeError(
                "A precomputed dynamic-bucket group contained multiple "
                f"partitions: expected {partition}, got {partitions}"
            )
        # Java's maintainer observes every written key after RowKind filtering.
        for key_hash in key_hashes:
            self._index_maintainer.notify_new_record(
                partition, bucket, to_signed_int32(key_hash)
            )
        return partition

    def release_prepared(self) -> None:
        if self._index_maintainer is not None:
            self._index_maintainer.release_prepared()

    def abort(self) -> None:
        if self._index_maintainer is not None:
            self._index_maintainer.abort()


class PostponeBucketRowKeyExtractor(RowKeyExtractor):
    """Extractor for postpone bucket mode which writes to bucket -2."""

    def __init__(self, table_schema: TableSchema):
        super().__init__(table_schema)
        num_buckets = int(table_schema.options.get(CoreOptions.BUCKET.key(), -2))
        if num_buckets != BucketMode.POSTPONE_BUCKET.value:
            raise ValueError(f"Postpone bucket mode requires bucket = -2, got {num_buckets}")

    def _extract_buckets_batch(self, data: pa.RecordBatch) -> List[int]:
        return [BucketMode.POSTPONE_BUCKET.value] * data.num_rows

    def _extract_bucket_row(self, values_by_name: Dict[str, Any]) -> int:
        return BucketMode.POSTPONE_BUCKET.value


class PostponeFixedBucketRowKeyExtractor(PostponeFixedBucketInputRouter, RowKeyExtractor):
    """Route Python file writes with the shared distributed input router."""
