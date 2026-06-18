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
        CommitMessage(partition=key, bucket=0, new_files=[], index_deletes=dels)
        for key, dels in by_partition.items()
    ]


def indexed_field_names(global_index_meta, field_by_id) -> set:
    field_ids = [global_index_meta.index_field_id]
    if global_index_meta.extra_field_ids:
        field_ids.extend(global_index_meta.extra_field_ids)
    return {
        field_by_id[field_id]
        for field_id in field_ids
        if field_id in field_by_id
    }


def apply_global_index_update_action(
    table,
    snapshot,
    updated_cols: Sequence[str],
    written_partitions: Set[Tuple],
) -> list:
    if snapshot is None or not updated_cols or not written_partitions:
        return []
    entries = scan_global_index_entries(table, snapshot)
    if not entries:
        return []
    field_by_id = {f.id: f.name for f in table.fields}
    update_set = set(updated_cols)
    affected = []
    conflicted = set()
    for e in entries:
        if tuple(e.partition.values) not in written_partitions:
            continue
        matched = indexed_field_names(
            e.index_file.global_index_meta, field_by_id
        ).intersection(update_set)
        if matched:
            affected.append(e)
            conflicted.update(matched)
    if not affected:
        return []
    action = table.options.global_index_column_update_action()
    if action is None:
        action = GlobalIndexColumnUpdateAction.THROW_ERROR
    if action == GlobalIndexColumnUpdateAction.DROP_PARTITION_INDEX:
        return build_index_delete_msgs(affected)
    raise RuntimeError(
        f"Update columns contain globally indexed columns, not supported now.\n"
        f"Updated columns: {sorted(update_set)}\n"
        f"Conflicted columns: {sorted(conflicted)}"
    )
