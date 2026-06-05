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
import os
import time
from typing import Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

from pypaimon.common.predicate import Predicate
from pypaimon.globalindex import ScoredGlobalIndexResult
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.manifest.simple_stats_evolutions import SimpleStatsEvolutions
from pypaimon.schema.data_types import DataField
from pypaimon.read.plan import Plan
from pypaimon.read.push_down_utils import (_get_all_fields,
                                           remove_row_id_filter,
                                           trim_and_transform_predicate)
from pypaimon.read.scan_stats import ScanStats
from pypaimon.read.scanner.append_table_split_generator import \
    AppendTableSplitGenerator
from pypaimon.read.scanner.bucket_select_converter import \
    create_bucket_selector
from pypaimon.read.scanner.data_evolution_split_generator import \
    DataEvolutionSplitGenerator
from pypaimon.read.scanner.primary_key_table_split_generator import \
    PrimaryKeyTableSplitGenerator
from pypaimon.read.split import DataSplit
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.table.source.deletion_file import DeletionFile


def _row_ranges_from_predicate(predicate: Optional[Predicate]) -> Optional[List]:
    from pypaimon.table.special_fields import SpecialFields
    from pypaimon.utils.range import Range

    if predicate is None:
        return None

    def visit(p: Predicate):
        if p.method == 'and':
            result = None
            for child in p.literals:
                sub = visit(child)
                if sub is None:
                    continue
                result = Range.and_(result, sub) if result is not None else sub
                if not result:
                    return result
            return result
        if p.method == 'or':
            parts = []
            for child in p.literals:
                sub = visit(child)
                if sub is None:
                    return None
                parts.extend(sub)
            if not parts:
                return []
            return Range.sort_and_merge_overlap(parts, merge=True, adjacent=True)
        if p.field != SpecialFields.ROW_ID.name:
            return None
        if p.method == 'equal':
            if not p.literals:
                return []
            return Range.to_ranges([int(p.literals[0])])
        if p.method == 'in':
            if not p.literals:
                return []
            return Range.to_ranges([int(x) for x in p.literals])
        if p.method == 'between':
            if not p.literals or len(p.literals) < 2:
                return []
            return [Range(int(p.literals[0]), int(p.literals[1]))]
        return None

    return visit(predicate)


def _filter_manifest_files_by_row_ranges(
        manifest_files: List[ManifestFileMeta],
        row_ranges: List) -> List[ManifestFileMeta]:
    """
    Filter manifest files by row ranges.

    Only keep manifest files that have min_row_id and max_row_id and overlap with the given row ranges.

    Args:
        manifest_files: List of manifest file metadata
        row_ranges: List of row ranges to filter by

    Returns:
        Filtered list of manifest files
    """
    from pypaimon.utils.range import Range

    filtered_files = []
    for manifest in manifest_files:
        min_row_id = manifest.min_row_id
        max_row_id = manifest.max_row_id

        # If min_row_id or max_row_id is None, we cannot filter, keep the file
        if min_row_id is None or max_row_id is None:
            filtered_files.append(manifest)
            continue

        # Check if manifest row range overlaps with any of the expected row ranges
        manifest_row_range = Range(min_row_id, max_row_id)
        should_keep = False

        for expected_range in row_ranges:
            # Check if ranges intersect
            intersect = Range.intersect(
                manifest_row_range.from_,
                manifest_row_range.to,
                expected_range.from_,
                expected_range.to)
            if intersect:
                should_keep = True
                break

        if should_keep:
            filtered_files.append(manifest)

    return filtered_files


def _filter_manifest_entries_by_row_ranges(
        entries: List[ManifestEntry],
        row_ranges: List) -> List[ManifestEntry]:

    if not row_ranges:
        return []

    filtered = []
    for entry in entries:
        first_row_id = entry.file.first_row_id
        if first_row_id is None:
            filtered.append(entry)
            continue
        file_range = entry.file.row_id_range()
        for r in row_ranges:
            if file_range.overlaps(r):
                filtered.append(entry)
                break
    return filtered


class FileScanner:
    def __init__(
        self,
        table,
        manifest_scanner: Callable[[], Tuple[List[ManifestFileMeta], Optional[Snapshot]]],
        predicate: Optional[Predicate] = None,
        limit: Optional[int] = None,
        partition_predicate: Optional[Predicate] = None
    ):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.manifest_scanner = manifest_scanner
        self.predicate = predicate
        self.predicate_for_stats = remove_row_id_filter(predicate) if predicate else None
        self.limit = limit

        self.snapshot_manager = table.snapshot_manager()
        self.manifest_list_manager = ManifestListManager(table)
        self.manifest_file_manager = ManifestFileManager(table)

        self.primary_key_predicate = trim_and_transform_predicate(
            self.predicate, self.table.field_names, self.table.trimmed_primary_keys)

        if partition_predicate is None:
            self.partition_key_predicate = trim_and_transform_predicate(
                self.predicate, self.table.field_names, self.table.partition_keys)
        else:
            self.partition_key_predicate = partition_predicate
        options = self.table.options
        # Get split target size and open file cost from table options
        self.target_split_size = options.source_split_target_size()
        self.open_file_cost = options.source_split_open_file_cost()

        self.idx_of_this_subtask = None
        self.number_of_para_subtasks = None
        self.start_pos_of_this_subtask = None
        self.end_pos_of_this_subtask = None

        self.only_read_real_buckets = options.bucket() == BucketMode.POSTPONE_BUCKET.value
        self.data_evolution = options.data_evolution_enabled()
        self.deletion_vectors_enabled = options.deletion_vectors_enabled()
        self._global_index_result = None
        self._scanned_snapshot = None
        self._scanned_snapshot_id = None
        # Opt-in scan-plan tracking. Stays ``None`` for the read hot path;
        # ``scan_with_stats()`` flips it on for a single explain pass and
        # the filter callbacks below increment counters when present.
        self.scan_stats: Optional[ScanStats] = None

        # Predicate-driven bucket pruning (HASH_FIXED only). Mirrors Java
        # BucketSelectConverter. Set on demand and reused across all
        # _filter_manifest_entry calls; the inner _Selector caches the
        # bucket set per ``total_buckets`` value.
        self._bucket_selector = self._init_bucket_selector()

        def schema_fields_func(schema_id: int):
            return self.table.schema_manager.get_schema(schema_id).fields

        self.simple_stats_evolutions = SimpleStatsEvolutions(
            schema_fields_func,
            self.table.table_schema.id
        )

    def _deletion_files_map(self, entries: List[ManifestEntry]) -> Dict[tuple, Dict[str, DeletionFile]]:
        if not self.deletion_vectors_enabled:
            return {}
        # Extract unique partition-bucket pairs from file entries
        bucket_files = set()
        for e in entries:
            bucket_files.add((tuple(e.partition.values), e.bucket))
        snapshot = self._scanned_snapshot if self._scanned_snapshot else self.snapshot_manager.get_latest_snapshot()
        return self._scan_dv_index(snapshot, bucket_files)

    def scan(self) -> Plan:
        start_ms = time.time() * 1000
        # Create appropriate split generator based on table type
        if self.table.is_primary_key_table:
            entries = self.plan_files()
            split_generator = PrimaryKeyTableSplitGenerator(
                self.table,
                self.target_split_size,
                self.open_file_cost,
                self._deletion_files_map(entries)
            )
        elif self.data_evolution:
            entries, split_generator = self._create_data_evolution_split_generator()
        else:
            entries = self.plan_files()
            split_generator = AppendTableSplitGenerator(
                self.table,
                self.target_split_size,
                self.open_file_cost,
                self._deletion_files_map(entries)
            )

        if not entries:
            return Plan([], snapshot_id=self._scanned_snapshot_id)

        # Configure sharding if needed
        if self.idx_of_this_subtask is not None:
            split_generator.with_shard(self.idx_of_this_subtask, self.number_of_para_subtasks)
        elif self.start_pos_of_this_subtask is not None:
            split_generator.with_slice(self.start_pos_of_this_subtask, self.end_pos_of_this_subtask)

        # Generate splits
        splits = split_generator.create_splits(entries)

        splits = self._apply_push_down_limit(splits)
        duration_ms = int(time.time() * 1000 - start_ms)
        logger.info(
            "File store scan plan completed in %d ms. Files size: %d",
            duration_ms, len(entries)
        )
        return Plan(splits, snapshot_id=self._scanned_snapshot_id)

    def _create_data_evolution_split_generator(self):
        row_ranges = None
        score_getter = None
        # Fetch snapshot once and share with global index evaluation to avoid
        # a duplicate /snapshot REST round-trip (#7513).
        manifest_files, snapshot = self.manifest_scanner()
        self._scanned_snapshot = snapshot
        self._scanned_snapshot_id = snapshot.id if snapshot else None

        global_index_result = self._global_index_result if self._global_index_result is not None \
            else self._eval_global_index(snapshot)
        if global_index_result is not None:
            row_ranges = global_index_result.results().to_range_list()
            if isinstance(global_index_result, ScoredGlobalIndexResult):
                score_getter = global_index_result.score_getter()
        if row_ranges is None and self.predicate is not None:
            row_ranges = _row_ranges_from_predicate(self.predicate)

        # Filter manifest files by row ranges if available
        if row_ranges is not None:
            manifest_files = _filter_manifest_files_by_row_ranges(manifest_files, row_ranges)

        entries = self.read_manifest_entries(manifest_files)

        if row_ranges is not None:
            entries = _filter_manifest_entries_by_row_ranges(entries, row_ranges)

        return entries, DataEvolutionSplitGenerator(
            self.table,
            self.target_split_size,
            self.open_file_cost,
            self._deletion_files_map(entries),
            row_ranges,
            score_getter
        )

    def plan_files(self) -> List[ManifestEntry]:
        manifest_files, snapshot = self.manifest_scanner()
        self._scanned_snapshot = snapshot
        self._scanned_snapshot_id = snapshot.id if snapshot else None
        if len(manifest_files) == 0:
            return []
        return self.read_manifest_entries(manifest_files)

    def _eval_global_index(self, snapshot=None):
        # No filter - nothing to evaluate
        if self.predicate is None:
            return None

        # Check if global index is enabled
        if not self.table.options.global_index_enabled():
            return None

        from pypaimon.globalindex.global_index_scanner import GlobalIndexScanner

        try:
            scanner = GlobalIndexScanner.create(
                self.table,
                partition_filter=self.partition_key_predicate,
                predicate=self.predicate,
                snapshot=snapshot,
            )
            if scanner is None:
                return None
            with scanner:
                return scanner.scan(self.predicate)
        except Exception:
            return None

    def read_manifest_entries(self, manifest_files: List[ManifestFileMeta]) -> List[ManifestEntry]:
        max_workers = self.table.options.scan_manifest_parallelism(os.cpu_count() or 8)
        if self.scan_stats is not None:
            self.scan_stats.manifest_files_total += len(manifest_files)
            # ``num_added_files + num_deleted_files`` is the entry count
            # recorded in the manifest-file metadata; combined across
            # all input manifest files this is the partition-prune-free
            # baseline. The difference against ``entries_total`` reveals
            # how much manifest-level pruning saved.
            self.scan_stats.entries_potential_total += sum(
                f.num_added_files + f.num_deleted_files for f in manifest_files)
        manifest_files = [entry for entry in manifest_files if self._filter_manifest_file(entry)]
        if self.scan_stats is not None:
            self.scan_stats.manifest_files_after_partition += len(manifest_files)
            # Force single-threaded so we can mutate stats without locking.
            max_workers = 1
        return self.manifest_file_manager.read_entries_parallel(
            manifest_files,
            self._filter_manifest_entry,
            max_workers=max_workers,
            early_entry_filter=self._build_early_bucket_filter(),
        )

    def _build_early_bucket_filter(self):
        """Compose the (bucket, total_buckets) -> bool used by the manifest
        reader to drop entries before deserialising ``_FILE`` / partition.

        The selector is partition-aware now, but at this early stage the
        partition field has not been deserialised yet, so callers stick
        with the two-arg form. The selector internally falls back to a
        partition-agnostic over-approximation; per-partition tightening
        still happens later in ``_filter_manifest_entry`` once the entry
        is fully decoded.
        """
        # explain() needs accurate before/after pruning counters; suppress
        # the early bucket filter so every entry reaches
        # ``_filter_manifest_entry`` where each rejection stage is counted.
        if self.scan_stats is not None:
            return None
        only_real = self.only_read_real_buckets
        selector = self._bucket_selector
        if not only_real and selector is None:
            return None

        def _filter(bucket: int, total_buckets: int) -> bool:
            if only_real and bucket < 0:
                return False
            if (selector is not None
                    and bucket >= 0
                    and not selector(bucket, total_buckets)):
                return False
            return True

        return _filter

    def with_shard(self, idx_of_this_subtask: int, number_of_para_subtasks: int) -> 'FileScanner':
        if idx_of_this_subtask >= number_of_para_subtasks:
            raise ValueError("idx_of_this_subtask must be less than number_of_para_subtasks")
        if self.start_pos_of_this_subtask is not None:
            raise Exception("with_shard and with_slice cannot be used simultaneously")
        self.idx_of_this_subtask = idx_of_this_subtask
        self.number_of_para_subtasks = number_of_para_subtasks
        return self

    def with_slice(self, start_pos: int, end_pos: int) -> 'FileScanner':
        if start_pos >= end_pos:
            raise ValueError("start_pos must be less than end_pos")
        if self.idx_of_this_subtask is not None:
            raise Exception("with_slice and with_shard cannot be used simultaneously")
        self.start_pos_of_this_subtask = start_pos
        self.end_pos_of_this_subtask = end_pos
        return self

    def with_global_index_result(self, result) -> 'FileScanner':
        self._global_index_result = result
        return self

    def scan_with_stats(self) -> Tuple[Plan, ScanStats]:
        """Run one scan pass while recording :class:`ScanStats` counters.

        Side-effects: forces single-thread manifest reads and disables the
        early bucket filter so every entry reaches
        ``_filter_manifest_entry`` exactly once. The scanner is one-shot
        in this mode — call ``scan()`` on a fresh instance afterwards if
        you need the regular hot path.
        """
        self.scan_stats = ScanStats()
        plan = self.scan()
        return plan, self.scan_stats

    def _apply_push_down_limit(self, splits: List[DataSplit]) -> List[DataSplit]:
        """Mirror Java ``DataTableBatchScan.applyPushDownLimit``: sum the
        DV-aware ``merged_row_count`` (== Java ``Split.mergedRowCount()``)
        until the limit is met. Splits with unknown merged count fall
        through to the reader unchanged.
        """
        if self.limit is None:
            return splits
        if self._has_non_partition_filter():
            return splits

        scanned_row_count = 0
        limited_splits: List[DataSplit] = []
        for split in splits:
            merged = split.merged_row_count()
            if merged is not None:
                limited_splits.append(split)
                scanned_row_count += merged
                if scanned_row_count >= self.limit:
                    return limited_splits
        return splits

    def _has_non_partition_filter(self) -> bool:
        """Mirror Java ``SnapshotReaderImpl.hasNonPartitionFilter``."""
        if self.predicate is None:
            return False
        partition_keys = set(self.table.partition_keys or [])
        return not _get_all_fields(self.predicate).issubset(partition_keys)

    def _filter_manifest_file(self, file: ManifestFileMeta) -> bool:
        if not self.partition_key_predicate:
            return True
        return self.partition_key_predicate.test_by_simple_stats(
            file.partition_stats,
            file.num_added_files + file.num_deleted_files)

    def _init_bucket_selector(self):
        """Build the predicate-driven bucket selector if (and only if) the
        table is in HASH_FIXED mode and the predicate pins all bucket-key
        fields to Equal/In literals. Anything else returns None — the
        caller treats None as "no bucket-level pruning".

        Bucket-key fields come from ``TableSchema.logical_bucket_key_fields``
        — the same source the writer's ``FixedBucketRowKeyExtractor`` reads
        from, which is what makes the read/write hash agreement a property
        of the schema rather than of any particular extractor instance.

        Sound across rescale: ``_Selector`` caches per ``total_buckets``,
        which can vary between manifest entries after a bucket rescale.
        """
        if self.predicate is None:
            return None
        # ``bucket_mode()`` returns HASH_FIXED only when ``options.bucket()
        # > 0``; other modes (DYNAMIC / POSTPONE / UNAWARE / CROSS_PARTITION)
        # have no fixed hash → bucket mapping at write time and must NOT
        # be pruned here.
        try:
            if self.table.bucket_mode() != BucketMode.HASH_FIXED:
                return None
        except Exception:
            # Defensive: any catalog/proxy table that fails the mode check
            # falls back to no pruning rather than crashing the scan.
            return None
        # Only the default hash function (Math.abs(hash % numBuckets)) is
        # supported for bucket pruning. Non-default functions (mod, hive)
        # use different algorithms and would produce wrong bucket sets.
        bucket_func = self.table.table_schema.options.get('bucket-function.type', 'default')
        if bucket_func.lower() != 'default':
            return None
        try:
            bucket_key_fields = self.table.table_schema.logical_bucket_key_fields
        except Exception:
            # ``bucket_keys`` raises on misconfigured ``bucket-key`` (e.g.
            # references an unknown column). The previous extractor-based
            # path failed open here; preserve that — pruning is an
            # optimisation, never a correctness requirement.
            return None
        if not bucket_key_fields:
            return None
        # Partition fields are passed so the selector can specialise
        # the predicate per partition value at the late filter stage,
        # turning ``(part='a' AND bk=1) OR (part='b' AND bk=2)`` into a
        # precise bucket pick per partition instead of an over-scan.
        partition_fields: Optional[List[DataField]] = None
        if self.table.partition_keys:
            partition_fields = [
                self.table.field_dict[name]
                for name in self.table.partition_keys
                if name in self.table.field_dict
            ]
        return create_bucket_selector(
            self.predicate, bucket_key_fields,
            partition_fields=partition_fields,
        )

    def _filter_manifest_entry(self, entry: ManifestEntry) -> bool:
        stats = self.scan_stats
        if stats is not None:
            stats.entries_total += 1
            partition_key = tuple(entry.partition.values)
            stats.partition_keys_before.add(partition_key)
            stats.buckets_seen.add((partition_key, entry.bucket))
        # Stage 1: partition predicate. The early manifest-reader filter
        # only sees ``(bucket, total_buckets)`` and never enforces
        # partition predicates, so this check is the sole partition gate
        # at the entry level — not a "redundant safety net".
        if self.partition_key_predicate and not self.partition_key_predicate.test(entry.partition):
            return False
        if stats is not None:
            stats.entries_after_partition += 1
            stats.partition_keys_after.add(partition_key)
        # Stage 2: bucket rejection. Two reasons land here:
        #   * ``only_read_real_buckets`` drops the synthetic
        #     POSTPONE_BUCKET bucket id (also enforced by the early
        #     filter when present; kept here so the method is correct
        #     standalone).
        #   * ``_bucket_selector`` is the HASH_FIXED predicate-driven
        #     selector built by ``_init_bucket_selector``.
        # Both are accounted for under ``entries_after_bucket`` so the
        # explain funnel reports bucket-level pruning end-to-end.
        if self.only_read_real_buckets and entry.bucket < 0:
            return False
        if (self._bucket_selector is not None
                and entry.bucket >= 0
                and not self._bucket_selector(
                    entry.partition, entry.bucket, entry.total_buckets)):
            return False
        if stats is not None:
            stats.entries_after_bucket += 1
            stats.buckets_after_pruning.add((partition_key, entry.bucket))
        # Get SimpleStatsEvolution for this schema
        evolution = self.simple_stats_evolutions.get_or_create(entry.file.schema_id)

        # Apply evolution to stats
        if self.table.is_primary_key_table:
            if self.deletion_vectors_enabled and entry.file.level == 0:  # do not read level 0 file
                return False
            if self.primary_key_predicate:
                if not self.primary_key_predicate.test_by_simple_stats(
                    entry.file.key_stats,
                    entry.file.row_count
                ):
                    return False
            # In DV mode, files within a bucket don't overlap (level 0 excluded above),
            # so we can safely filter by value stats per file.
            if self.deletion_vectors_enabled and self.predicate_for_stats:
                if entry.file.value_stats_cols is None and entry.file.write_cols is not None:
                    stats_fields = entry.file.write_cols
                else:
                    stats_fields = entry.file.value_stats_cols
                evolved_stats = evolution.evolution(
                    entry.file.value_stats,
                    entry.file.row_count,
                    stats_fields
                )
                if not self.predicate_for_stats.test_by_simple_stats(
                    evolved_stats,
                    entry.file.row_count
                ):
                    return False
            if stats is not None:
                stats.entries_after_stats += 1
            return True
        else:
            if not self.predicate or self.predicate_for_stats is None:
                if stats is not None:
                    stats.entries_after_stats += 1
                return True
            # Data evolution: file stats may be from another schema, skip stats filter and filter in reader.
            if self.data_evolution:
                if stats is not None:
                    stats.entries_after_stats += 1
                return True
            if entry.file.value_stats_cols is None and entry.file.write_cols is not None:
                stats_fields = entry.file.write_cols
            else:
                stats_fields = entry.file.value_stats_cols
            evolved_stats = evolution.evolution(
                entry.file.value_stats,
                entry.file.row_count,
                stats_fields
            )
            kept = self.predicate_for_stats.test_by_simple_stats(
                evolved_stats,
                entry.file.row_count
            )
            if kept and stats is not None:
                stats.entries_after_stats += 1
            return kept

    def _scan_dv_index(self, snapshot, buckets: Set[tuple]) -> Dict[tuple, Dict[str, DeletionFile]]:
        """
        Scan deletion vector index from snapshot.
        Returns a map of (partition, bucket) -> {filename -> DeletionFile}

        Reference: SnapshotReaderImpl.scanDvIndex() in Java
        """
        if not snapshot or not snapshot.index_manifest:
            return {}

        result = {}

        # Read index manifest file
        index_manifest_file = IndexManifestFile(self.table)
        index_entries = index_manifest_file.read(snapshot.index_manifest)

        # Filter by DELETION_VECTORS_INDEX type and requested buckets
        for entry in index_entries:
            if entry.index_file.index_type != IndexManifestFile.DELETION_VECTORS_INDEX:
                continue

            partition_bucket = (tuple(entry.partition.values), entry.bucket)
            if partition_bucket not in buckets:
                continue

            # Convert to deletion files
            deletion_files = self._to_deletion_files(entry)
            if deletion_files:
                result[partition_bucket] = deletion_files

        return result

    def _to_deletion_files(self, index_entry) -> Dict[str, DeletionFile]:
        """
        Convert index manifest entry to deletion files map.
        Returns {filename -> DeletionFile}
        """
        deletion_files = {}
        index_file = index_entry.index_file

        # Check if dv_ranges exists
        if not index_file.dv_ranges:
            return deletion_files

        # Build deletion file path
        # Format: manifest/index-manifest-{uuid}
        index_path = self.table.table_path.rstrip('/') + '/index'
        dv_file_path = f"{index_path}/{index_file.file_name}"

        # Convert each DeletionVectorMeta to DeletionFile
        for data_file_name, dv_meta in index_file.dv_ranges.items():
            deletion_file = DeletionFile(
                dv_index_path=dv_file_path,
                offset=dv_meta.offset,
                length=dv_meta.length,
                cardinality=dv_meta.cardinality
            )
            deletion_files[data_file_name] = deletion_file

        return deletion_files
