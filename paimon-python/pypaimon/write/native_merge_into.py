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

"""Transport existing MERGE inputs into the core operation."""

import pyarrow as pa

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.native_commit import create_native_write_table, from_native_commit_messages
from pypaimon.write.native_update import (
    _native_row_id_table, _native_update_columns_supported, _supported_upsert_key_type,
)


def create_native_merge_into(table, source, on, matched, not_matched, commit_user, commit_identifier):
    from pypaimon.table.data_evolution_merge_into import _is_self_merge, _is_table_like, _prepare
    from pypaimon.ray.data_evolution_merge_into import _normalize_on, _union_update_cols
    from pypaimon.ray.data_evolution_merge_transform import SourceColumnRef, TargetColumnRef, LiteralValue
    from pypaimon.ray.merge_condition import rewrite_condition

    target_keys, source_keys = _normalize_on(on)
    self_merge = _is_self_merge(table, source, target_keys, source_keys)
    if not_matched and table.options.video_frame_fields():
        return None
    native_table = _native_row_id_table(table)
    if native_table is None:
        return None
    source_schema = None
    native_source = None
    if _is_table_like(source) and not self_merge:
        # The source can be an append or PK table; only the target requires DE.
        from pypaimon.read.native_plan import _native_blob_view_supported
        from pypaimon.table.file_store_table import FileStoreTable
        if (type(source) is not FileStoreTable
                or source.options.file_format() != 'parquet'
                or source.options.video_frame_fields() or source.options.with_vector_format()
                or (source.is_primary_key_table and not source.trimmed_primary_keys)):
            return None
        source.new_read_builder().new_scan()._validate_scan_mode()
        if not _native_blob_view_supported(source, source.field_names):
            return None
        # This operation reads one full source snapshot. Incremental scans stay
        # on the Python path, whose scanner applies the requested delta range.
        if (source.options.scan_mode() not in ('default', 'from-snapshot', 'from-timestamp')
                or any(str(key).startswith('incremental-') for key in source.table_schema.options)):
            return None
        native_source = create_native_write_table(source)
        if native_source is None:
            return None
        from pypaimon.schema.data_types import PyarrowFieldParser
        source_schema = PyarrowFieldParser.from_paimon_schema(source.table_schema.fields)
    source, matched, not_matched, context = _prepare(
        table, source, list(matched), list(not_matched), on, source_schema=source_schema)
    if any(callable(value) for clause in matched + not_matched for value in clause.spec.values()):
        return None
    if not _native_update_columns_supported(table, _union_update_cols(matched)):
        return None
    for target, origin in zip(target_keys, source_keys):
        target_type = pa.int64() if target == '_ROW_ID' else context.full_pa_schema.field(target).type
        source_type = target_type if self_merge else source.schema.field(origin).type
        if source_type != target_type or not _supported_upsert_key_type(target_type):
            return None

    def encode(clause):
        values = []
        for name, value in clause.spec.items():
            if isinstance(value, SourceColumnRef):
                values.append((name, 'source', value.column))
            elif isinstance(value, TargetColumnRef):
                values.append((name, 'target', value.column))
            elif isinstance(value, LiteralValue):
                values.append((name, 'literal', value.value))
            else:
                return None
        condition = None if clause.condition is None else dict(sql=rewrite_condition(clause.condition))
        return dict(assignments=values, condition=condition, delete=clause.delete)

    encoded_matched = [encode(clause) for clause in matched]
    encoded_not_matched = [encode(clause) for clause in not_matched]
    if any(clause is None for clause in encoded_matched + encoded_not_matched):
        return None
    builder = (native_table.new_batch_write_builder()._with_commit_user(commit_user)
               if commit_identifier == BATCH_COMMIT_IDENTIFIER else
               native_table.new_stream_write_builder().with_commit_user(commit_user))
    writer = builder.new_update()
    return NativeTableMergeInto(table, writer, native_source if native_source is not None else source,
                                list(zip(target_keys, source_keys)),
                                encoded_matched, encoded_not_matched, commit_identifier)


class NativeTableMergeInto:
    def __init__(self, table, writer, source, on, matched, not_matched, commit_identifier):
        self.table = table
        self.writer = writer
        self.source = source
        self.on = on
        self.matched = matched
        self.not_matched = not_matched
        self.commit_identifier = commit_identifier

    def prepare_commit(self):
        # The core operation owns the snapshot, matching, action selection and staging.
        # Never retry via Python after it starts or delete prepared messages on failure.
        kwargs = ({} if self.commit_identifier == BATCH_COMMIT_IDENTIFIER else
                  dict(commit_identifier=self.commit_identifier))
        return from_native_commit_messages(self.table, self.writer.merge_into(
            self.source, on=self.on, when_matched=self.matched, when_not_matched=self.not_matched, **kwargs))
