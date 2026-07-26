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

"""Persistent hash assignment and index maintenance for dynamic buckets."""

import random
import struct
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Set, Tuple

from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.table.row.generic_row import GenericRow

HASH_INDEX = "HASH"
_ADD = 0
_DELETE = 1
SHORT_MAX_VALUE = 32767
_SNAPSHOT_UNSET = object()


def to_signed_int32(value: int) -> int:
    """Return the Java ``int`` representation of ``value``."""
    value &= 0xFFFFFFFF
    return value - 0x100000000 if value >= 0x80000000 else value


def _java_remainder(value: int, divisor: int) -> int:
    remainder = abs(value) % divisor
    return -remainder if value < 0 else remainder


def compute_assigner(
    partition_hash: int,
    key_hash: int,
    num_channels: int,
    num_assigners: int,
) -> int:
    """Mirror Java ``BucketAssigner.computeAssigner``."""
    if num_channels <= 0 or num_assigners <= 0:
        raise ValueError("num_channels and num_assigners must be positive")
    start = abs(_java_remainder(to_signed_int32(partition_hash), num_channels))
    assigner = abs(_java_remainder(to_signed_int32(key_hash), num_assigners))
    return (start + assigner) % num_channels


def is_my_bucket(bucket: int, num_assigners: int, assign_id: int) -> bool:
    return bucket % num_assigners == assign_id % num_assigners


def _iter_hashes(table, entry: IndexManifestEntry) -> Iterator[int]:
    meta = entry.index_file
    path = meta.external_path
    if path is None:
        # Files without an external path always live in the table's index
        # directory, even if global-index.external-path was configured later.
        index_path = table.path_factory().global_index_path_factory().index_path()
        path = f"{index_path}/{meta.file_name}"
    with table.file_io.new_input_stream(path) as stream:
        remainder = b""
        while True:
            payload = stream.read(64 * 1024)
            if not payload:
                break
            payload = remainder + payload
            aligned_size = len(payload) - len(payload) % 4
            for value in struct.iter_unpack(">i", payload[:aligned_size]):
                yield value[0]
            remainder = payload[aligned_size:]
        if remainder:
            raise RuntimeError(
                f"Corrupt dynamic-bucket HASH index {path}: "
                "expected a multiple of 4 bytes"
            )


def _read_hashes(table, entry: IndexManifestEntry) -> Set[int]:
    return set(_iter_hashes(table, entry))


class _PartitionIndex:
    def __init__(
        self,
        hash_to_bucket: Dict[int, int],
        bucket_information: Dict[int, int],
        target_bucket_row_number: int,
    ) -> None:
        self.hash_to_bucket = hash_to_bucket
        self.non_full_bucket_information = bucket_information
        self.total_bucket_set = set(bucket_information)
        self.total_bucket_list = list(bucket_information)
        self.target_bucket_row_number = target_bucket_row_number

    def assign(
        self,
        key_hash: int,
        bucket_filter,
        max_buckets_num: int,
        max_bucket_id: int,
    ) -> int:
        return self.assign_with_status(
            key_hash, bucket_filter, max_buckets_num, max_bucket_id
        )[0]

    def assign_with_status(
        self,
        key_hash: int,
        bucket_filter,
        max_buckets_num: int,
        max_bucket_id: int,
    ) -> Tuple[int, bool]:
        if key_hash in self.hash_to_bucket:
            return self.hash_to_bucket[key_hash], False

        for bucket, row_count in list(self.non_full_bucket_information.items()):
            if row_count < self.target_bucket_row_number:
                self.non_full_bucket_information[bucket] = row_count + 1
                self.hash_to_bucket[key_hash] = bucket
                return bucket, True
            del self.non_full_bucket_information[bucket]

        global_max_bucket_id = (
            SHORT_MAX_VALUE if max_buckets_num == -1 else max_buckets_num
        ) - 1
        if not self.total_bucket_set or max_bucket_id < global_max_bucket_id:
            for bucket in range(global_max_bucket_id + 1):
                if bucket_filter(bucket) and bucket not in self.total_bucket_set:
                    self.non_full_bucket_information[bucket] = 1
                    self.total_bucket_set.add(bucket)
                    self.total_bucket_list.append(bucket)
                    self.hash_to_bucket[key_hash] = bucket
                    return bucket, True
            if max_buckets_num == -1:
                raise RuntimeError(
                    "No dynamic bucket id remains below Java Short.MAX_VALUE. "
                    "Increase dynamic-bucket.target-row-num."
                )

        if not self.total_bucket_list:
            raise RuntimeError(
                "No dynamic bucket is available for this assigner. Reduce the "
                "assigner parallelism or increase dynamic-bucket.max-buckets."
            )
        bucket = random.choice(self.total_bucket_list)
        self.hash_to_bucket[key_hash] = bucket
        return bucket, True


class HashBucketAssigner:
    """Restore dynamic-bucket mappings from HASH indexes before assigning."""

    def __init__(
        self,
        table,
        num_channels: int,
        num_assigners: int,
        assign_id: int,
        target_bucket_row_number: int,
        max_buckets_num: int,
        ignore_existing: bool = False,
        snapshot=_SNAPSHOT_UNSET,
    ) -> None:
        if not 0 <= assign_id < num_channels:
            raise ValueError(
                f"assign_id must be in [0, {num_channels}), got {assign_id}"
            )
        self.table = table
        self.num_channels = num_channels
        self.num_assigners = num_assigners
        self.assign_id = assign_id
        self.target_bucket_row_number = target_bucket_row_number
        self.max_buckets_num = max_buckets_num
        self.ignore_existing = ignore_existing
        self.snapshot = (
            table.snapshot_manager().get_latest_snapshot()
            if snapshot is _SNAPSHOT_UNSET
            else snapshot
        )
        self.max_bucket_id = 0
        self._partition_indexes: Dict[Tuple, _PartitionIndex] = {}

    def assign(self, partition: Tuple, partition_hash: int, key_hash: int) -> int:
        return self.assign_with_status(partition, partition_hash, key_hash)[0]

    def assign_with_status(
        self, partition: Tuple, partition_hash: int, key_hash: int
    ) -> Tuple[int, bool]:
        result = self.assign_batch(
            [partition], [partition_hash], [key_hash]
        )
        return result[0]

    def assign_batch(
        self,
        partitions: List[Tuple],
        partition_hashes: List[int],
        key_hashes: List[int],
    ) -> List[Tuple[int, bool]]:
        if not (
            len(partitions) == len(partition_hashes) == len(key_hashes)
        ):
            raise ValueError(
                "Partition, partition-hash, and key-hash counts must match"
            )
        requested_by_partition: Dict[Tuple, Set[int]] = {}
        normalized_hashes = [to_signed_int32(value) for value in key_hashes]
        for partition, partition_hash, key_hash in zip(
            partitions, partition_hashes, normalized_hashes
        ):
            self._validate_assigner(partition_hash, key_hash)
            requested_by_partition.setdefault(partition, set()).add(key_hash)

        for partition, requested in requested_by_partition.items():
            index = self._partition_indexes.get(partition)
            if index is None:
                index = self._load_partition(
                    partition,
                    requested,
                )
                self._partition_indexes[partition] = index
            else:
                missing = requested.difference(index.hash_to_bucket)
                if missing:
                    self._restore_requested_hashes(
                        partition,
                        missing,
                        index.hash_to_bucket,
                    )

        results = []
        for partition, key_hash in zip(partitions, normalized_hashes):
            index = self._partition_indexes[partition]
            bucket, is_new = index.assign_with_status(
                key_hash,
                lambda value: is_my_bucket(
                    value, self.num_assigners, self.assign_id
                ),
                self.max_buckets_num,
                self.max_bucket_id,
            )
            self.max_bucket_id = max(self.max_bucket_id, bucket)
            results.append((bucket, is_new))
        return results

    def _validate_assigner(self, partition_hash: int, key_hash: int) -> None:
        expected_assigner = compute_assigner(
            partition_hash,
            key_hash,
            self.num_channels,
            self.num_assigners,
        )
        if expected_assigner != self.assign_id:
            raise ValueError(
                f"Record assigner {expected_assigner} does not match writer "
                f"assigner {self.assign_id}"
            )

    def _load_partition(
        self, partition: Tuple, requested_hashes: Set[int]
    ) -> _PartitionIndex:
        if self.ignore_existing:
            return _PartitionIndex({}, {}, self.target_bucket_row_number)
        snapshot = self.snapshot
        if snapshot is None:
            return _PartitionIndex({}, {}, self.target_bucket_row_number)
        entries = IndexFileHandler(self.table).scan(
            snapshot,
            lambda entry: entry.index_file.index_type == HASH_INDEX
            and tuple(entry.partition.values) == partition,
        )
        entries_by_bucket = {}
        for entry in entries:
            previous = entries_by_bucket.get(entry.bucket)
            if previous is not None:
                raise RuntimeError(
                    "Found multiple dynamic-bucket HASH indexes for partition "
                    f"{partition}, bucket {entry.bucket}: "
                    f"{[previous.index_file.file_name, entry.index_file.file_name]}"
                )
            entries_by_bucket[entry.bucket] = entry

        data_buckets = self._active_data_buckets(partition, snapshot)
        missing_buckets = data_buckets.difference(entries_by_bucket)
        if missing_buckets:
            raise RuntimeError(
                f"Dynamic-bucket partition {partition} has data files but no "
                "complete HASH index for buckets "
                f"{sorted(missing_buckets)}. Rewrite the partition before "
                "performing incremental writes."
            )

        hash_to_bucket: Dict[int, int] = {}
        bucket_information: Dict[int, int] = {}
        for entry in entries:
            self.max_bucket_id = max(self.max_bucket_id, entry.bucket)
            if is_my_bucket(
                entry.bucket, self.num_assigners, self.assign_id
            ):
                bucket_information[entry.bucket] = entry.index_file.row_count
        remaining = set(requested_hashes)
        for entry in entries:
            if not remaining:
                break
            for key_hash in _iter_hashes(self.table, entry):
                if key_hash in remaining:
                    hash_to_bucket[key_hash] = entry.bucket
                    remaining.remove(key_hash)
                    if not remaining:
                        break
        return _PartitionIndex(
            hash_to_bucket,
            bucket_information,
            self.target_bucket_row_number,
        )

    def _restore_requested_hashes(
        self,
        partition: Tuple,
        requested_hashes: Set[int],
        hash_to_bucket: Dict[int, int],
    ) -> None:
        if self.ignore_existing or not requested_hashes:
            return
        snapshot = self.snapshot
        if snapshot is None:
            return
        entries = IndexFileHandler(self.table).scan(
            snapshot,
            lambda entry: entry.index_file.index_type == HASH_INDEX
            and tuple(entry.partition.values) == partition,
        )
        remaining = set(requested_hashes)
        for entry in entries:
            for key_hash in _iter_hashes(self.table, entry):
                if key_hash not in remaining:
                    continue
                hash_to_bucket[key_hash] = entry.bucket
                remaining.remove(key_hash)
                if not remaining:
                    return

    def _active_data_buckets(self, partition: Tuple, snapshot) -> Set[int]:
        if snapshot is None:
            return set()
        from pypaimon.common.options.core_options import CoreOptions

        scan_table = self.table.copy(
            {CoreOptions.SCAN_SNAPSHOT_ID.key(): str(snapshot.id)}
        )
        read_builder = scan_table.new_read_builder()
        if scan_table.partition_keys:
            predicate_builder = read_builder.new_predicate_builder()
            predicates = [
                predicate_builder.equal(key, value)
                for key, value in zip(scan_table.partition_keys, partition)
            ]
            read_builder.with_filter(
                predicate_builder.and_predicates(predicates)
            )
        return {
            split.bucket
            for split in read_builder.new_scan().plan_for_write().splits()
            if split.files
        }


@dataclass
class DynamicBucketIndexChanges:
    additions: List[IndexManifestEntry]
    deletions: List[IndexManifestEntry]


class DynamicBucketIndexMaintainer:
    """Maintain one complete HASH index file for every modified bucket."""

    def __init__(
        self,
        table,
        ignore_existing: bool = False,
        snapshot=_SNAPSHOT_UNSET,
    ) -> None:
        self.table = table
        self.ignore_existing = ignore_existing
        self.snapshot = (
            table.snapshot_manager().get_latest_snapshot()
            if snapshot is _SNAPSHOT_UNSET
            else snapshot
        )
        self.base_snapshot_id = (
            self.snapshot.id if self.snapshot is not None else 0
        )
        self._states: Dict[
            Tuple[Tuple, int], Tuple[Set[int], Optional[IndexManifestEntry], bool]
        ] = {}
        self._new_paths: List[str] = []

    def notify_new_record(
        self, partition: Tuple, bucket: int, key_hash: int
    ) -> None:
        key = (partition, bucket)
        state = self._states.get(key)
        if state is None:
            hashes, old_entry = self._load_bucket(partition, bucket)
            state = (hashes, old_entry, False)
        hashes, old_entry, modified = state
        previous_size = len(hashes)
        hashes.add(to_signed_int32(key_hash))
        self._states[key] = (hashes, old_entry, modified or len(hashes) != previous_size)

    def prepare_commit(self) -> Dict[Tuple[Tuple, int], DynamicBucketIndexChanges]:
        changes: Dict[Tuple[Tuple, int], DynamicBucketIndexChanges] = {}
        for key, (hashes, old_entry, modified) in self._states.items():
            if not modified:
                continue
            partition, bucket = key
            new_entry = self._write_index(partition, bucket, hashes)
            changes[key] = DynamicBucketIndexChanges(
                additions=[new_entry],
                deletions=[
                    IndexManifestEntry(
                        kind=_DELETE,
                        partition=old_entry.partition,
                        bucket=old_entry.bucket,
                        index_file=old_entry.index_file,
                    )
                ] if old_entry is not None else [],
            )
            self._states[key] = (hashes, new_entry, False)
        return changes

    def release_prepared(self) -> None:
        """Release files after their commit messages leave writer ownership."""
        self._new_paths.clear()

    def abort(self) -> None:
        for path in self._new_paths:
            self.table.file_io.delete_quietly(path)
        self._new_paths.clear()

    def _load_bucket(
        self, partition: Tuple, bucket: int
    ) -> Tuple[Set[int], Optional[IndexManifestEntry]]:
        """Load a complete bucket index for full-file replacement.

        This matches Java's HASH index lifecycle. A modified bucket therefore
        incurs O(bucket size) read memory and IO until an incremental index
        format is introduced.
        """
        if self.ignore_existing:
            return set(), None
        snapshot = self.snapshot
        if snapshot is None:
            return set(), None
        entries = IndexFileHandler(self.table).scan(
            snapshot,
            lambda entry: entry.index_file.index_type == HASH_INDEX
            and tuple(entry.partition.values) == partition
            and entry.bucket == bucket,
        )
        if len(entries) > 1:
            raise RuntimeError(
                "Found multiple dynamic-bucket HASH indexes for partition "
                f"{partition}, bucket {bucket}: "
                f"{[entry.index_file.file_name for entry in entries]}"
            )
        if not entries:
            return set(), None
        return _read_hashes(self.table, entries[0]), entries[0]

    def _write_index(
        self, partition: Tuple, bucket: int, hashes: Set[int]
    ) -> IndexManifestEntry:
        path_factory = self.table.path_factory().global_index_path_factory()
        self.table.file_io.check_or_mkdirs(path_factory.global_index_root_path())
        path = path_factory.new_path()
        payload = b"".join(struct.pack(">i", value) for value in sorted(hashes))
        try:
            with self.table.file_io.new_output_stream(path) as stream:
                stream.write(payload)
        except Exception:
            self.table.file_io.delete_quietly(path)
            raise
        self._new_paths.append(path)

        meta = IndexFileMeta(
            index_type=HASH_INDEX,
            file_name=path.rsplit("/", 1)[-1],
            file_size=self.table.file_io.get_file_size(path),
            row_count=len(hashes),
            external_path=path if path_factory.is_external_path() else None,
        )
        return IndexManifestEntry(
            kind=_ADD,
            partition=GenericRow(
                list(partition), self.table.partition_keys_fields
            ),
            bucket=bucket,
            index_file=meta,
        )
