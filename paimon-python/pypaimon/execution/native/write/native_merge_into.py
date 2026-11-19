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
from pypaimon.execution.native.keytime_capability import (
    table_has_string_keytime,
)
from pypaimon.execution.native.keytime_merge_guard import (
    refuse_partial_update_keytime_merge_source,
)
from pypaimon.execution.native.table import _native_variant_write_supported
from pypaimon.execution.native.write.native_commit import create_native_write_table, from_native_commit_messages
from pypaimon.execution.native.write.native_update import (
    _dictionary_value_type, _native_row_id_table, _native_update_columns_supported,
    _native_update_formats_supported, _supported_upsert_key_type,
)
from pypaimon.execution.native.write.merge_transform import encode_merge_clause


def _python_materialize_merge_source(source, columns):
    """Read a table source with Python UTF-16 keytime semantics as Arrow.

    Loads the full projected snapshot into memory via ``to_arrow()``. Native
    MERGE currently accepts Arrow / native table sources only; there is no
    streaming materialize API, so large STRING keytime sources pay full-table
    memory cost before the Native MERGE join runs.
    """
    table = source.copy({
        'read.native.enabled': 'false',
        'scan.native-plan.enabled': 'false',
    })
    builder = table.new_read_builder().with_projection(columns)
    return builder.new_read().to_arrow(builder.new_scan().plan().splits())


def _native_merge_table_source_supported(source) -> bool:
    """Shared Native MERGE constraints for table sources.

    Applies whether Core opens the source natively or Python materializes it
    to Arrow first. Keeps FileStoreTable, parquet formats, video-frame, PK
    shape, scan-mode, incremental and blob-view gates aligned with the
    historical Native source path so materialization does not widen support.
    """
    from pypaimon.execution.native.table import _native_blob_view_supported
    from pypaimon.table.file_store_table import FileStoreTable

    if (type(source) is not FileStoreTable
            or not _native_update_formats_supported(source)
            or source.options.video_frame_fields()
            or (source.is_primary_key_table and not source.trimmed_primary_keys)):
        return False
    from pypaimon.read.scan_config import _validate_scan_mode
    _validate_scan_mode(source)
    if not _native_blob_view_supported(source, source.field_names):
        return False
    # Core resolves a full snapshot or PyPaimon's existing timestamp window.
    if (source.options.scan_mode() not in (
            'default', 'latest', 'latest-full', 'from-snapshot',
            'from-snapshot-full', 'from-timestamp', 'incremental')
            or any(str(key).startswith('incremental-') and key != 'incremental-between-timestamp'
                   for key in source.table_schema.options)):
        return False
    return True


def create_native_merge_into(table, source, on, matched, not_matched, commit_user, commit_identifier):
    from pypaimon.write.merge_inputs import _is_self_merge, _is_table_like, _prepare
    from pypaimon.write.merge_inputs import _normalize_on, _union_update_cols

    target_keys, source_keys = _normalize_on(on)
    self_merge = _is_self_merge(table, source, target_keys, source_keys)
    # Refuse before Native target init, Native source open, or Python MERGE.
    if _is_table_like(source) and not self_merge:
        refuse_partial_update_keytime_merge_source(source)
    if not_matched and (table.options.video_frame_fields() or not _native_variant_write_supported(table)):
        return None
    native_table = _native_row_id_table(table)
    if native_table is None:
        return None
    source_schema = None
    native_source = None
    if _is_table_like(source) and not self_merge:
        # Shared table-source gates apply before either Native open or
        # Python Arrow materialization (do not widen the supported set).
        if not _native_merge_table_source_supported(source):
            return None
        # STRING keytime: materialize via Python so Native MERGE never opens
        # the source table through create_native_write_table / Rust UTF-8.
        if table_has_string_keytime(source):
            # Query auth is enforced by the Python read used for materialize;
            # Native REST-grant checks below apply only when Core opens the
            # source table itself.
            projection = list(dict.fromkeys(list(source.field_names) + source_keys))
            source = _python_materialize_merge_source(source, projection)
        else:
            from pypaimon.schema.data_types import PyarrowFieldParser
            projection = list(dict.fromkeys(list(source.field_names) + source_keys))
            source_fields = source.new_read_builder().with_projection(projection).read_type()
            source_schema = PyarrowFieldParser.from_paimon_schema(source_fields)
            if source.options.query_auth_enabled:
                from pypaimon.execution.native.table import (_native_query_auth_supported, _native_table)
                from pypaimon.execution.native.write.native_commit import _rest_catalog_supported
                if not _rest_catalog_supported(source) or not _native_query_auth_supported(source):
                    return None
                # A source is a read, not a write. Core obtains a fresh REST grant
                # and requires it to be unrestricted, as Python plan_all does.
                native_source = _native_table(source)
            else:
                native_source = create_native_write_table(source)
                if native_source is None:
                    return None
                from pypaimon.execution.native.table import (_with_native_read_snapshot)
                native_source = _with_native_read_snapshot(source, native_source)
    source, matched, not_matched, context = _prepare(
        table, source, list(matched), list(not_matched), on, source_schema=source_schema)
    if not _native_update_columns_supported(table, _union_update_cols(matched)):
        return None
    for target, origin in zip(target_keys, source_keys):
        target_type = pa.int64() if target == '_ROW_ID' else context.full_pa_schema.field(target).type
        source_type = target_type if self_merge else _dictionary_value_type(source.schema.field(origin).type)
        if source_type != target_type or not _supported_upsert_key_type(target_type):
            return None

    on_pairs = list(zip(source_keys, target_keys))
    encoded_matched = [encode_merge_clause(clause, on_pairs) for clause in matched]
    encoded_not_matched = [encode_merge_clause(clause, on_pairs, not_matched=True) for clause in not_matched]
    if any(clause is None for clause in encoded_matched + encoded_not_matched):
        return None
    builder = (native_table.new_batch_write_builder().with_commit_user(commit_user)
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
