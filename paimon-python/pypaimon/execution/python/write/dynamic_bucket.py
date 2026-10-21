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

"""Python writer lifecycle for persistent dynamic-bucket HASH indexes."""

import struct
import uuid
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

from pypaimon.index.dynamic_bucket import HASH_INDEX, _iter_hashes, to_signed_int32
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.table.row.generic_row import GenericRow

_ADD = 0
_SNAPSHOT_UNSET = object()


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
            Tuple[Tuple, int], Tuple[Set[int], bool]
        ] = {}
        self._new_paths: List[str] = []

    def notify_new_record(
        self, partition: Tuple, bucket: int, key_hash: int
    ) -> None:
        key = (partition, bucket)
        state = self._states.get(key)
        if state is None:
            state = (self._load_bucket(partition, bucket), False)
        hashes, modified = state
        previous_size = len(hashes)
        hashes.add(to_signed_int32(key_hash))
        self._states[key] = (hashes, modified or len(hashes) != previous_size)

    def prepare_commit(self) -> Dict[Tuple[Tuple, int], DynamicBucketIndexChanges]:
        changes: Dict[Tuple[Tuple, int], DynamicBucketIndexChanges] = {}
        prepared_states = {}
        for key, (hashes, modified) in self._states.items():
            if not modified:
                continue
            partition, bucket = key
            new_entry = self._write_index(partition, bucket, hashes)
            changes[key] = DynamicBucketIndexChanges(
                additions=[new_entry],
                # Java sends only the full new HASH file. BucketedCombiner
                # replaces the previous entry by partition and bucket.
                deletions=[],
            )
            prepared_states[key] = (hashes, False)
        # A failed bucket must not make a retry forget earlier additions.
        self._states.update(prepared_states)
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
    ) -> Set[int]:
        """Load a complete bucket index for full-file replacement.

        This matches Java's HASH index lifecycle. A modified bucket therefore
        incurs O(bucket size) read memory and IO until an incremental index
        format is introduced.
        """
        if self.ignore_existing:
            return set()
        snapshot = self.snapshot
        if snapshot is None:
            return set()
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
            return set()
        return set(_iter_hashes(self.table, entries[0]))

    def _write_index(
        self, partition: Tuple, bucket: int, hashes: Set[int]
    ) -> IndexManifestEntry:
        file_name = f"index-{uuid.uuid4()}-0"
        path, external = self.table.path_factory().new_bucket_index_path(
            partition, bucket, file_name
        )
        self.table.file_io.check_or_mkdirs(path.rsplit("/", 1)[0])
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
            external_path=path if external else None,
        )
        return IndexManifestEntry(
            kind=_ADD,
            partition=GenericRow(
                list(partition), self.table.partition_keys_fields
            ),
            bucket=bucket,
            index_file=meta,
        )
