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
from pypaimon.utils.range import Range


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


def _updated_columns_for_commit(table, messages):
    """Resolve physical update columns by field identity across schema changes."""
    if table.options.global_index_column_update_action() == GlobalIndexColumnUpdateAction.IGNORE:
        return set(), set()
    current = table.table_schema
    schemas = {current.id: current}
    names_by_id = {field.id: field.name for field in current.fields}
    columns, partitions = set(), set()
    for message in messages:
        if message.check_from_snapshot is None:
            continue
        for file in message.new_files:
            if file.first_row_id is None and '_ROW_ID' not in (file.write_cols or []):
                continue
            if file.schema_id not in schemas:
                schema = table.schema_manager.get_schema(file.schema_id)
                if schema is None:
                    raise ValueError('Cannot resolve update file schema %s' % file.schema_id)
                schemas[file.schema_id] = schema
            schema = schemas[file.schema_id]
            # None also represents full-row updates and compact normal files.
            # Dedicated Blob/vector files retain their explicit write columns.
            written = {names_by_id[field.id] for field in schema.data_file_fields(file.write_cols)
                       if field.id in names_by_id}
            if written:
                columns.update(written)
                partitions.add(message.partition)
    return columns, partitions


def check_retained_centroid_scalar_indexes(table, snapshot, updates, index_deletes):
    """Protect exact shard filters against updates at every publication attempt."""
    if snapshot is None or not snapshot.index_manifest or not updates:
        return

    by_partition = {}
    for entry in updates:
        by_partition.setdefault(tuple(entry.partition.values), []).append(entry.file)
    deleted = {_index_identity(entry) for entry in index_deletes if entry.kind == 1}
    current = table.table_schema
    schemas = {current.id: current}
    for entry in scan_global_index_entries(table, snapshot):
        meta = entry.index_file.global_index_meta
        if not meta.is_centroid_scalar_router() or _index_identity(entry) in deleted:
            continue
        indexed_fields = {meta.index_field_id, *(meta.extra_field_ids or [])}
        indexed_range = Range(meta.row_range_start, meta.row_range_end)
        for file in by_partition.get(tuple(entry.partition.values), ()):
            if not file.non_null_row_id_range().overlaps(indexed_range):
                continue
            if file.schema_id not in schemas:
                schema = table.schema_manager.get_schema(file.schema_id)
                if schema is None:
                    raise ValueError('Cannot resolve update file schema %s' % file.schema_id)
                schemas[file.schema_id] = schema
            written = {
                field.id for field in schemas[file.schema_id].data_file_fields(file.write_cols)
            }
            if indexed_fields.intersection(written):
                raise RuntimeError(
                    "Update affects an active centroid index with Bitmap fields. "
                    "Drop the index or use DROP_PARTITION_INDEX before updating."
                )


def apply_global_index_update_action(
    table,
    snapshot,
    updated_cols: Sequence[str],
    written_partitions: Set[Tuple],
    index_deletes=(),
) -> list:
    if snapshot is None or not updated_cols or not written_partitions:
        return []
    action = table.options.global_index_column_update_action()
    if action == GlobalIndexColumnUpdateAction.IGNORE:
        return []
    entries = scan_global_index_entries(table, snapshot)
    if not entries:
        return []
    field_by_id = {f.id: f.name for f in table.fields}
    update_set = set(updated_cols)
    deleted = {_index_identity(e) for e in index_deletes if e.kind == 1}
    affected = []
    conflicted = set()
    for e in entries:
        if _index_identity(e) in deleted:
            continue
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
    if action is None:
        action = GlobalIndexColumnUpdateAction.THROW_ERROR
    if action == GlobalIndexColumnUpdateAction.DROP_PARTITION_INDEX:
        return build_index_delete_msgs(affected)
    raise RuntimeError(
        f"Update columns contain globally indexed columns, not supported now.\n"
        f"Updated columns: {sorted(update_set)}\n"
        f"Conflicted columns: {sorted(conflicted)}"
    )


def _index_identity(entry):
    return (tuple(entry.partition.values), entry.bucket,
            entry.index_file.index_type, entry.index_file.file_name)
