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

import uuid
from typing import List, Tuple

from pypaimon.manifest.schema.file_entry import FileEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta


class ManifestFileMerger:
    """Minor manifest compaction for Python commits.

    This intentionally implements only the minor compaction path from Java
    ManifestFileMerger. It does not do full compaction or manifest sort rewrite.
    """

    def __init__(self, manifest_file_manager, suggested_meta_size: int,
                 suggested_min_meta_count: int):
        self.manifest_file_manager = manifest_file_manager
        self.suggested_meta_size = suggested_meta_size
        self.suggested_min_meta_count = suggested_min_meta_count

    def merge(self, manifest_files: List[ManifestFileMeta]) -> Tuple[List[ManifestFileMeta],
                                                                     List[ManifestFileMeta]]:
        new_files = []
        try:
            return self._try_minor_compaction(manifest_files, new_files), new_files
        except Exception:
            self._delete_manifests(new_files)
            raise

    def _try_minor_compaction(self, manifest_files: List[ManifestFileMeta],
                              new_files: List[ManifestFileMeta]) -> List[ManifestFileMeta]:
        result = []
        candidates = []
        total_size = 0

        for manifest in manifest_files:
            total_size += manifest.file_size
            candidates.append(manifest)
            if total_size >= self.suggested_meta_size:
                self._merge_candidates(candidates, result, new_files)
                candidates = []
                total_size = 0

        if len(candidates) >= self.suggested_min_meta_count:
            self._merge_candidates(candidates, result, new_files)
        else:
            result.extend(candidates)

        return result

    def _merge_candidates(self, candidates: List[ManifestFileMeta],
                          result: List[ManifestFileMeta],
                          new_files: List[ManifestFileMeta]):
        if len(candidates) == 1:
            result.append(candidates[0])
            return

        entries = []
        for manifest in candidates:
            entries.extend(
                self.manifest_file_manager.read(
                    manifest.file_name,
                    drop_stats=False,
                )
            )

        merged_entries = FileEntry.merge_entries(entries)
        if not merged_entries:
            return

        manifest_file = "manifest-{}".format(str(uuid.uuid4()))
        merged_metas = self.manifest_file_manager.rolling_write(
            merged_entries, self.suggested_meta_size, manifest_file)
        result.extend(merged_metas)
        new_files.extend(merged_metas)

    def _delete_manifests(self, manifests: List[ManifestFileMeta]):
        for manifest in manifests:
            manifest_path = "{}/{}".format(
                self.manifest_file_manager.manifest_path,
                manifest.file_name,
            )
            self.manifest_file_manager.file_io.delete_quietly(manifest_path)
