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

"""Driver-side planner for primary-key (merge-tree) compaction."""

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from pypaimon.common.predicate import Predicate
from pypaimon.compact.coordinator.coordinator import CompactCoordinator
from pypaimon.compact.levels import Levels
from pypaimon.compact.options import CompactOptions
from pypaimon.compact.strategy.strategy import (CompactStrategy,
                                                pick_full_compaction)
from pypaimon.compact.strategy.universal_compaction import UniversalCompaction
from pypaimon.compact.task.merge_tree_compact_task import MergeTreeCompactTask
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.reader.sort_merge_reader import builtin_key_comparator
from pypaimon.read.scanner.file_scanner import FileScanner

DEFAULT_NUM_LEVELS = 5


class MergeTreeCompactCoordinator(CompactCoordinator):
    """Plan one MergeTreeCompactTask per (partition, bucket) that the strategy says to compact.

    The coordinator owns the Levels object for a bucket and asks the
    strategy.pick(...) which runs to combine. full_compaction=True bypasses
    the strategy entirely and picks every file in every bucket.
    """

    def __init__(
        self,
        table,
        compact_options: Optional[CompactOptions] = None,
        partition_predicate: Optional[Predicate] = None,
        strategy: Optional[CompactStrategy] = None,
    ):
        if not table.is_primary_key_table:
            raise ValueError(
                "MergeTreeCompactCoordinator only handles primary-key tables; "
                "use AppendCompactCoordinator for append-only tables."
            )
        self.table = table
        self.options = compact_options or CompactOptions()
        self.partition_predicate = partition_predicate
        self.num_levels = self._resolve_num_levels()
        self.strategy = strategy or self._default_strategy()
        self.key_comparator = builtin_key_comparator(self.table.trimmed_primary_keys_fields)

    def plan(self) -> List[MergeTreeCompactTask]:
        manifest_entries = self._scan_live_files()
        if not manifest_entries:
            return []

        bucket_files: Dict[Tuple[Tuple, int], List[DataFileMeta]] = defaultdict(list)
        for entry in manifest_entries:
            key = (tuple(entry.partition.values), entry.bucket)
            bucket_files[key].append(entry.file)

        tasks: List[MergeTreeCompactTask] = []
        for (partition, bucket), files in bucket_files.items():
            levels = Levels(self.key_comparator, files, self.num_levels)
            unit = self._pick_unit(levels)
            if unit is None:
                continue
            drop_delete = self._should_drop_delete(unit, levels)
            tasks.append(
                MergeTreeCompactTask(
                    partition=partition,
                    bucket=bucket,
                    files=unit.files,
                    output_level=unit.output_level,
                    drop_delete=drop_delete,
                    table=self.table,
                )
            )
        return tasks

    # ---- internals ---------------------------------------------------------

    def _pick_unit(self, levels: Levels):
        runs = levels.level_sorted_runs()
        if self.options.full_compaction:
            return pick_full_compaction(levels.number_of_levels(), runs)
        return self.strategy.pick(levels.number_of_levels(), runs)

    def _should_drop_delete(self, unit, levels: Levels) -> bool:
        # Mirrors MergeTreeCompactManager.triggerCompaction's dropDelete rule:
        # we may drop retract rows only when nothing older could need them, i.e.
        # we are writing to a level >= the highest non-empty level (and never
        # to L0, which by definition can have older data above and below).
        if unit.output_level == 0:
            return False
        return unit.output_level >= levels.non_empty_highest_level()

    def _resolve_num_levels(self) -> int:
        # Java reads num-levels off CoreOptions; pypaimon's CoreOptions doesn't
        # surface it as a typed accessor yet, so we read the raw map and fall
        # back to Java's CoreOptions.NUM_LEVELS default (5). Any input file
        # already at a higher level wins during Levels construction anyway.
        raw = self._raw_options_map().get("num-levels")
        return int(raw) if raw is not None else DEFAULT_NUM_LEVELS

    def _default_strategy(self) -> CompactStrategy:
        raw = self._raw_options_map()
        max_size_amp = int(raw.get("compaction.max-size-amplification-percent") or 200)
        size_ratio = int(raw.get("compaction.size-ratio") or 1)
        trigger = int(raw.get("num-sorted-run.compaction-trigger") or 5)
        return UniversalCompaction(
            max_size_amp=max_size_amp,
            size_ratio=size_ratio,
            num_run_compaction_trigger=trigger,
        )

    def _raw_options_map(self) -> dict:
        opts = self.table.options.options
        if hasattr(opts, "to_map"):
            return opts.to_map()
        return dict(opts) if opts else {}

    def _scan_live_files(self):
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
