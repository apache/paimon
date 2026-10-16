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

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set, Tuple


@dataclass
class ScanStats:
    """Counters accumulated by :class:`FileScanner` when it is asked to
    track stats for ``ReadBuilder.explain()``.

    The scanner mutates these counters in place; consumers should treat
    instances as immutable once the scan returns. Default factory values
    keep the dataclass usable both as a blank "no tracking" sentinel and
    as a live accumulator.
    """

    manifest_files_total: int = 0
    manifest_files_after_partition: int = 0

    # ``entries_potential_total`` is the row count we would have processed if no
    # filtering occurred — derived from manifest-file metadata before the
    # manifest-level partition skip. ``entries_total`` is what actually reached
    # ``_filter_manifest_entry``; the gap between the two captures
    # manifest-level pruning.
    entries_potential_total: Optional[int] = 0
    entries_total: int = 0
    entries_after_partition: Optional[int] = 0
    entries_after_bucket: Optional[int] = 0
    entries_after_stats: Optional[int] = 0

    # Identity sets are Python-scanner bookkeeping. Native returns counters
    # without transporting partition values back across the language boundary.
    partition_keys_before: Optional[Set[Tuple]] = field(default_factory=set)
    partition_keys_after: Optional[Set[Tuple]] = field(default_factory=set)

    buckets_seen: Optional[Set[Tuple[Tuple, int]]] = field(default_factory=set)
    buckets_after_pruning: Optional[Set[Tuple[Tuple, int]]] = field(default_factory=set)
    native_planned: bool = False
    # Native counters describe actual planning work; skipped metadata is not decoded
    # to reconstruct the Python scanner's before/after funnel.
    planning_counters: Optional[Dict[str, Any]] = None

    @classmethod
    def from_native(cls, stats):
        return cls(
            manifest_files_total=stats.manifest_files_before_partition_pruning,
            manifest_files_after_partition=stats.manifest_files_after_partition_pruning,
            entries_total=stats.manifest_entries_read,
            entries_potential_total=None, entries_after_partition=None,
            entries_after_bucket=None, entries_after_stats=None,
            planning_counters={name: getattr(stats, name) for name in (
                'base_manifest_files',
                'delta_manifest_files',
                'manifest_files_before_partition_pruning',
                'manifest_files_after_partition_pruning',
                'manifest_files_pruned_by_row_ranges',
                'manifest_entries_read',
                'manifest_entries_pruned_by_bucket',
                'manifest_entries_pruned_by_partition',
                'manifest_entries_after_entry_pruning',
                'manifest_entries_pruned_by_level',
                'manifest_entries_pruned_by_row_ranges',
                'manifest_entries_pruned_by_data_stats',
                'manifest_entries_after_manifest_filters',
                'manifest_entries_after_merge',
                'manifest_entries_pruned_by_cross_schema_stats',
                'manifest_entries_after_cross_schema_stats',
                'data_evolution_groups_before_stats',
                'data_evolution_groups_pruned_by_stats',
                'data_evolution_groups_pruned_by_row_ranges',
                'split_candidates_built',
                'limit_early_stopped',
                'splits_before_limit',
                'splits_after_limit',
                'final_splits',
                'final_files',
                'planned_data_file_bytes',
                'limit',
            )},
            partition_keys_before=None, partition_keys_after=None,
            buckets_seen=None, buckets_after_pruning=None, native_planned=True)
