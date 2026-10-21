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

"""Python global index deletion message preparation."""

from typing import List

from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.write.commit_message import CommitMessage


def build_global_index_drop(self, partition_filter):
    snapshot = self._table.snapshot_manager().get_latest_snapshot()
    if snapshot is None:
        return []

    target_field_ids = [
        self._table.field_dict[column].id for column in self._index_columns
    ]

    def should_delete(entry: IndexManifestEntry) -> bool:
        index_file = entry.index_file
        global_meta = index_file.global_index_meta
        if index_file.index_type.lower().strip() != self._index_type or global_meta is None:
            return False
        if _indexed_field_ids(global_meta) != target_field_ids:
            return False
        return partition_filter is None or partition_filter.test(entry.partition)

    entries = IndexFileHandler(self._table).scan(snapshot, should_delete)
    if not entries:
        return []

    by_partition = {}
    for entry in entries:
        key = tuple(entry.partition.values)
        by_partition.setdefault(key, []).append(
            IndexManifestEntry(
                kind=1,
                partition=entry.partition,
                bucket=entry.bucket,
                index_file=entry.index_file,
            )
        )

    return [
        CommitMessage(
            partition=partition,
            bucket=0,
            new_files=[],
            index_deletes=deletes,
        )
        for partition, deletes in by_partition.items()
    ]


def _indexed_field_ids(global_meta) -> List[int]:
    field_ids = [global_meta.index_field_id]
    if global_meta.extra_field_ids:
        field_ids.extend(global_meta.extra_field_ids)
    return field_ids
