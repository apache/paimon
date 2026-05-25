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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional, Set, Tuple

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.file_entry import FileEntry
from pypaimon.snapshot.snapshot import Snapshot

logger = logging.getLogger(__name__)


class SnapshotDeletion:
    """Deletes data files and manifests associated with expired snapshots."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable
        self.table: FileStoreTable = table
        self.file_io = table.file_io
        self.path_factory = table.path_factory()
        self.manifest_list_manager = table.manifest_list_manager()
        self.manifest_file_manager = table.manifest_file_manager()
        self.deletion_buckets: Dict[tuple, Set[int]] = {}
        self._cached_tag_id: Optional[int] = None
        self._cached_tag_data_files: Optional[Dict[tuple, Dict[int, Set[str]]]] = None

    def clean_unused_data_files(self, snapshot: Snapshot, skipper: Callable = lambda entry: False,
                                changelog_decoupled: bool = False) -> None:
        if not snapshot.delta_manifest_list:
            return
        manifest_metas = self._try_read_manifest_list(snapshot.delta_manifest_list)
        if not manifest_metas:
            return
        data_file_to_delete: Dict[str, Tuple] = {}
        for meta in manifest_metas:
            try:
                entries = self.manifest_file_manager.read(meta.file_name, drop_stats=True)
            except IOError:
                logger.warning("Failed to read manifest %s, skipping",
                               meta.file_name, exc_info=True)
                return
            self._get_data_file_to_delete(data_file_to_delete, entries)

        if changelog_decoupled:
            self._do_clean_unused_data_files(
                data_file_to_delete,
                self._decoupled_changelog_skipper(skipper))
        else:
            self._do_clean_unused_data_files(data_file_to_delete, skipper)

    def delete_added_data_files(self, manifest_list_name: str) -> None:
        """Delete all ADD-kind data files from a manifest list (for changelog cleanup)."""
        metas = self._try_read_manifest_list(manifest_list_name)
        for meta in metas:
            try:
                entries = self.manifest_file_manager.read(meta.file_name, drop_stats=True)
                for entry in entries:
                    if entry.kind == FileEntry.KIND_ADD:
                        bucket_path = self.path_factory.bucket_path(entry.partition, entry.bucket)
                        self.file_io.delete_quietly(f"{bucket_path}/{entry.file.file_name}")
            except IOError:
                logger.debug("Failed to read manifest %s for changelog cleanup",
                             meta.file_name, exc_info=True)

    def clean_unused_manifests(self, snapshot: Snapshot, skipping_set: Set[str],
                               changelog_decoupled: bool = False) -> None:
        paths_to_delete = []
        manifest_path = self.path_factory.manifest_path()

        if not changelog_decoupled:
            for ml_name in [snapshot.base_manifest_list, snapshot.delta_manifest_list,
                            snapshot.changelog_manifest_list]:
                if not ml_name:
                    continue
                metas = self._try_read_manifest_list(ml_name)
                for meta in metas:
                    if meta.file_name not in skipping_set:
                        paths_to_delete.append(f"{manifest_path}/{meta.file_name}")
                        skipping_set.add(meta.file_name)
                if ml_name not in skipping_set:
                    paths_to_delete.append(f"{manifest_path}/{ml_name}")
                    skipping_set.add(ml_name)

        if snapshot.index_manifest and snapshot.index_manifest not in skipping_set:
            paths_to_delete.append(f"{manifest_path}/{snapshot.index_manifest}")
            skipping_set.add(snapshot.index_manifest)
        if snapshot.statistics and snapshot.statistics not in skipping_set:
            stats_path = self.path_factory.statistics_path()
            paths_to_delete.append(f"{stats_path}/{snapshot.statistics}")
            skipping_set.add(snapshot.statistics)
        self._delete_files_parallel(paths_to_delete)

    def manifest_skipping_set(self, skipping_snapshots: List[Snapshot]) -> Set[str]:
        result: Set[str] = set()
        for snapshot in skipping_snapshots:
            for ml_name in [snapshot.base_manifest_list, snapshot.delta_manifest_list,
                            snapshot.changelog_manifest_list]:
                if ml_name:
                    result.add(ml_name)
                    for meta in self._try_read_manifest_list(ml_name):
                        result.add(meta.file_name)
            if snapshot.index_manifest:
                result.add(snapshot.index_manifest)
                try:
                    for ie in self.table.index_manifest_file().read(snapshot.index_manifest):
                        result.add(ie.index_file.file_name)
                except IOError:
                    logger.debug("Failed to read index manifest %s",
                                 snapshot.index_manifest, exc_info=True)
            if snapshot.statistics:
                result.add(snapshot.statistics)
        return result

    def create_data_file_skipper_for_tags(self, tagged_snapshots: list,
                                          expiring_snapshot_id: int) -> Callable:
        prev_tag = self._find_previous_tag(tagged_snapshots, expiring_snapshot_id)
        if prev_tag is None:
            return lambda entry: False
        if self._cached_tag_id != prev_tag.id:
            self._cached_tag_id = prev_tag.id
            self._cached_tag_data_files = self._collect_tag_data_files(prev_tag)
        return lambda entry: self._contains_data_file(self._cached_tag_data_files, entry)

    def clean_empty_directories(self) -> None:
        if not self.deletion_buckets:
            return
        parent_dirs_to_check = set()
        for partition, buckets in self.deletion_buckets.items():
            for bucket in buckets:
                bucket_path = self.path_factory.bucket_path(partition, bucket)
                if self._try_delete_empty_directory(bucket_path):
                    parent_dirs_to_check.add(bucket_path.rsplit('/', 1)[0])
        for _ in range(len(self.table.partition_keys)):
            next_parents = set()
            for dir_path in parent_dirs_to_check:
                if self._try_delete_empty_directory(dir_path):
                    next_parents.add(dir_path.rsplit('/', 1)[0])
            parent_dirs_to_check = next_parents
        self.deletion_buckets.clear()

    # --- private helpers ---

    @staticmethod
    def _decoupled_changelog_skipper(base_skipper: Callable) -> Callable:
        """In decoupled mode, also skip APPEND-source files (or files with unknown source
        from old table versions). These are retained for ExpireChangelogImpl."""
        def _should_skip(entry) -> bool:
            if base_skipper(entry):
                return True
            file_source = getattr(entry.file, 'file_source', None)
            return file_source is None or file_source == DataFileMeta.FILE_SOURCE_APPEND
        return _should_skip

    def _get_data_file_to_delete(self, data_file_to_delete: Dict[str, Tuple],
                                 entries: list) -> None:
        for entry in entries:
            file_name = entry.file.file_name
            if entry.kind == FileEntry.KIND_DELETE:
                extra = (entry.file.extra_files
                         if hasattr(entry.file, 'extra_files') and entry.file.extra_files
                         else [])
                data_file_to_delete[file_name] = (entry, extra)
            elif entry.kind == FileEntry.KIND_ADD:
                data_file_to_delete.pop(file_name, None)

    def _do_clean_unused_data_files(self, data_file_to_delete: Dict[str, Tuple],
                                    skipper: Callable) -> None:
        paths_to_delete = []
        for file_name, (entry, extra_files) in data_file_to_delete.items():
            if skipper(entry):
                continue
            bucket_path = self.path_factory.bucket_path(entry.partition, entry.bucket)
            paths_to_delete.append(f"{bucket_path}/{file_name}")
            for extra in extra_files:
                paths_to_delete.append(f"{bucket_path}/{extra}")
            self._record_deletion_bucket(entry.partition, entry.bucket)
        self._delete_files_parallel(paths_to_delete)

    def _record_deletion_bucket(self, partition: tuple, bucket: int) -> None:
        self.deletion_buckets.setdefault(partition, set()).add(bucket)

    def _find_previous_tag(self, tagged_snapshots: list, snapshot_id: int):
        result = None
        for tag in tagged_snapshots:
            if tag.id < snapshot_id:
                result = tag
            else:
                break
        return result

    def _collect_tag_data_files(self, tag_snapshot: Snapshot) -> Dict[tuple, Dict[int, Set[str]]]:
        result: Dict[tuple, Dict[int, Set[str]]] = {}
        all_metas = []
        for ml_name in [tag_snapshot.base_manifest_list, tag_snapshot.delta_manifest_list]:
            if ml_name:
                all_metas.extend(self._try_read_manifest_list(ml_name))
        all_entries = []
        for meta in all_metas:
            try:
                all_entries.extend(self.manifest_file_manager.read(meta.file_name, drop_stats=True))
            except IOError:
                logger.debug("Failed to read manifest %s for tag data files",
                             meta.file_name, exc_info=True)
        for entry in FileEntry.merge_entries(all_entries):
            (result
             .setdefault(entry.partition, {})
             .setdefault(entry.bucket, set())
             .add(entry.file.file_name))
        return result

    def _contains_data_file(self, tag_data_files, entry) -> bool:
        if tag_data_files is None:
            return False
        bucket_map = tag_data_files.get(entry.partition)
        if bucket_map is None:
            return False
        file_set = bucket_map.get(entry.bucket)
        return file_set is not None and entry.file.file_name in file_set

    def _try_delete_empty_directory(self, path: str) -> bool:
        try:
            if not self.file_io.list_status(path):
                self.file_io.delete(path, recursive=False)
                return True
        except OSError:
            logger.debug("Failed to delete empty directory %s", path, exc_info=True)
        return False

    def _try_read_manifest_list(self, manifest_list_name: str) -> list:
        try:
            return self.manifest_list_manager.read(manifest_list_name)
        except IOError:
            logger.debug("Failed to read manifest list %s", manifest_list_name, exc_info=True)
            return []

    def _delete_files_parallel(self, paths: List[str]) -> None:
        if not paths:
            return
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(self.file_io.delete_quietly, p) for p in paths]
            for f in as_completed(futures):
                try:
                    f.result()
                except OSError:
                    logger.debug("Failed to delete file during parallel cleanup",
                                 exc_info=True)
