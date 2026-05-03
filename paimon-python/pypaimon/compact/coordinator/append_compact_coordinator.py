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

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from pypaimon.common.predicate import Predicate
from pypaimon.compact.coordinator.coordinator import CompactCoordinator
from pypaimon.compact.options import CompactOptions
from pypaimon.compact.task.append_compact_task import AppendCompactTask
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.scanner.file_scanner import FileScanner


class AppendCompactCoordinator(CompactCoordinator):
    """Plan compaction tasks for append-only tables (HASH_FIXED or BUCKET_UNAWARE).

    For each (partition, bucket) we compact files smaller than the table's
    target_file_size. A bucket is eligible only when it has at least
    `min_file_num` such files (default 5) — matching the Java
    AppendCompactCoordinator threshold and avoiding pointless rewrites of a
    single small file. `full_compaction=True` overrides the threshold and
    rewrites every file in every bucket regardless of size.

    The coordinator caps each task at `max_file_num` files; oversized buckets
    produce multiple tasks so an executor can spread the work in parallel
    instead of hot-spotting one worker on a huge bucket.
    """

    def __init__(
        self,
        table,
        compact_options: Optional[CompactOptions] = None,
        partition_predicate: Optional[Predicate] = None,
    ):
        if table.is_primary_key_table:
            raise ValueError(
                "AppendCompactCoordinator only handles append-only tables; "
                "use the merge-tree coordinator for primary-key tables."
            )
        self.table = table
        self.options = compact_options or CompactOptions()
        self.partition_predicate = partition_predicate

    def plan(self) -> List[AppendCompactTask]:
        manifest_entries = self._scan_live_files()
        if not manifest_entries:
            return []

        # Reduce the manifest entry stream to (partition, bucket) → live files.
        # We trust the manifest scanner to have already merged ADD/DELETE
        # entries; whatever survives here is currently in the snapshot.
        bucket_files: Dict[Tuple[Tuple, int], List[DataFileMeta]] = defaultdict(list)
        for entry in manifest_entries:
            key = (tuple(entry.partition.values), entry.bucket)
            bucket_files[key].append(entry.file)

        target_file_size = self.table.options.target_file_size(False)

        tasks: List[AppendCompactTask] = []
        for (partition, bucket), files in bucket_files.items():
            for chunk in self._pick_files_for_bucket(files, target_file_size):
                tasks.append(
                    AppendCompactTask(
                        partition=partition,
                        bucket=bucket,
                        files=chunk,
                        table=self.table,
                    )
                )
        return tasks

    def _pick_files_for_bucket(
        self,
        files: List[DataFileMeta],
        target_file_size: int,
    ) -> List[List[DataFileMeta]]:
        """Choose which files in a single bucket get compacted, batching if needed.

        Files >= target_file_size are skipped (they're already at output size and
        rewriting them only spends IO). full_compaction overrides this skip.
        """
        if self.options.full_compaction:
            candidates = list(files)
            if not candidates:
                return []
        else:
            candidates = [f for f in files if f.file_size < target_file_size]
            if len(candidates) < self.options.min_file_num:
                return []

        # Stable order: oldest sequence first so rewrites preserve append order
        # if the executor later relies on file ordering for something.
        candidates.sort(key=lambda f: f.min_sequence_number)

        chunks: List[List[DataFileMeta]] = []
        max_per_task = max(self.options.max_file_num, self.options.min_file_num)
        for start in range(0, len(candidates), max_per_task):
            chunk = candidates[start:start + max_per_task]
            if len(chunk) >= self.options.min_file_num or self.options.full_compaction:
                chunks.append(chunk)
        return chunks

    def _scan_live_files(self):
        """Read manifest entries from the latest snapshot, applying partition filter."""
        snapshot = self.table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return []

        from pypaimon.manifest.manifest_list_manager import ManifestListManager
        manifest_list_manager = ManifestListManager(self.table)

        def manifest_scanner():
            return manifest_list_manager.read_all(snapshot), snapshot

        scanner = FileScanner(
            self.table,
            manifest_scanner,
            partition_predicate=self.partition_predicate,
        )
        return scanner.plan_files()
