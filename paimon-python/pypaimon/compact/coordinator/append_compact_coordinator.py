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

    Per (partition, bucket) we filter to files smaller than the table's
    target_file_size and bin-pack them into compaction tasks using the same
    algorithm as Java AppendCompactCoordinator.SubCoordinator.pack:

    1. Sort candidates by file size ascending (smaller files lead, so the
       packer has the most flexibility to grow a bin without immediately
       overshooting).
    2. Walk the sorted list adding each file to the current bin, accruing
       (file_size + open_file_cost) — open_file_cost mirrors Java's per-file
       IO weight so a bin of many tiny files drains earlier than naive
       size accounting would suggest.
    3. Drain a bin as soon as it has >1 file AND its weighted size hits
       target_file_size * 2. The ×2 is Java's hardcoded constant: each
       task should produce roughly two target-sized output files, which
       amortizes task setup cost while keeping output sizes predictable.
    4. The trailing bin is emitted only if it has at least min_file_num
       files; smaller tails are dropped to avoid spending an entire task
       on a couple of files that will collect company on the next plan.

    full_compaction=True relaxes both the size filter (large files also
    enter packing) and the trailing-bin threshold (any non-empty tail
    is emitted), matching the user-level intent of "rewrite this bucket
    regardless of current shape".
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
        open_file_cost = self.table.options.source_split_open_file_cost()

        tasks: List[AppendCompactTask] = []
        for (partition, bucket), files in bucket_files.items():
            for chunk in self._pick_files_for_bucket(files, target_file_size, open_file_cost):
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
        open_file_cost: int,
    ) -> List[List[DataFileMeta]]:
        """Bin-pack one bucket's files into compaction tasks.

        Mirrors org.apache.paimon.append.AppendCompactCoordinator
        .SubCoordinator.pack — see class docstring for the reasoning behind
        the size-based packing, the open_file_cost weight, and the
        target_file_size * 2 drain threshold.
        """
        if self.options.full_compaction:
            candidates = list(files)
        else:
            # Files already at or above target size aren't worth rewriting —
            # the output would be near-identical and we'd burn IO for it.
            candidates = [f for f in files if f.file_size < target_file_size]

        if not candidates:
            return []

        candidates.sort(key=lambda f: f.file_size)

        chunks: List[List[DataFileMeta]] = []
        bin_files: List[DataFileMeta] = []
        bin_size = 0
        drain_threshold = target_file_size * 2
        for f in candidates:
            bin_files.append(f)
            bin_size += f.file_size + open_file_cost
            if len(bin_files) > 1 and bin_size >= drain_threshold:
                chunks.append(bin_files)
                bin_files = []
                bin_size = 0

        # Trailing bin: under full_compaction any non-empty tail ships;
        # otherwise we require min_file_num files so a tiny tail waits for
        # company on the next plan instead of paying task overhead now.
        min_tail = 1 if self.options.full_compaction else self.options.min_file_num
        if len(bin_files) >= min_tail:
            chunks.append(bin_files)
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
