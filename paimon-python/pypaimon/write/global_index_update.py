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

from typing import Sequence, Set, Tuple

from pypaimon.common.options.core_options import GlobalIndexColumnUpdateAction


def scan_global_index_entries(table, snapshot):
    from pypaimon.index.index_file_handler import IndexFileHandler

    handler = IndexFileHandler(table=table)
    return handler.scan(
        snapshot, lambda e: e.index_file.global_index_meta is not None
    )


def build_index_delete_msgs(entries) -> list:
    """Group scanned index entries by partition into index-delete messages."""
    from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
    from pypaimon.write.commit_message import CommitMessage

    by_partition = {}
    for e in entries:
        key = tuple(e.partition.values)
        by_partition.setdefault(key, []).append(
            IndexManifestEntry(
                kind=1, partition=e.partition, bucket=e.bucket, index_file=e.index_file
            )
        )
    return [
        CommitMessage(partition=key, bucket=0, new_files=[], index_files=dels)
        for key, dels in by_partition.items()
    ]


def apply_global_index_update_action(
    table,
    snapshot,
    updated_cols: Sequence[str],
    written_partitions: Set[Tuple],
) -> list:
    """Enforce ``global-index.column-update-action`` when an update touches
    columns covered by a global index.

    Scoped to the partitions actually written and the updated indexed columns
    (mirrors Spark's ``checkUpdateResult``). THROW_ERROR (default) raises;
    DROP_PARTITION_INDEX drops the affected index entries, returned as
    index-delete commit messages to commit alongside the update.
    """
    if snapshot is None or not updated_cols or not written_partitions:
        return []
    entries = scan_global_index_entries(table, snapshot)
    if not entries:
        return []
    field_by_id = {f.id: f.name for f in table.fields}
    update_set = set(updated_cols)
    affected = [
        e for e in entries
        if field_by_id.get(e.index_file.global_index_meta.index_field_id) in update_set
        and tuple(e.partition.values) in written_partitions
    ]
    if not affected:
        return []
    action = (
        table.options.global_index_column_update_action()
        or GlobalIndexColumnUpdateAction.THROW_ERROR
    )
    if action == GlobalIndexColumnUpdateAction.DROP_PARTITION_INDEX:
        return build_index_delete_msgs(affected)
    conflicted = sorted(
        {field_by_id.get(e.index_file.global_index_meta.index_field_id) for e in affected}
    )
    raise NotImplementedError(
        f"Update would modify columns {conflicted} covered by a global index; "
        f"set 'global-index.column-update-action' = 'DROP_PARTITION_INDEX' to "
        f"drop the affected index instead (rebuild afterwards)."
    )
