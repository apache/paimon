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

"""Index update policy uses the identities in a prepared file's own schema."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests.global_index_update_action_test import _index_entry
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.global_index_update_checker import scan_global_index_entries

pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _commit(table, messages, native=False):
    commit = table.new_batch_write_builder().new_commit()
    try:
        if native:
            with patch.object(commit.file_store_commit, 'commit', side_effect=AssertionError('Python fallback')):
                commit.commit(messages)
        else:
            commit.commit(messages)
    finally:
        commit.close()


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('evolution', ['rename', 'drop_readd', 'complete_row'])
@pytest.mark.parametrize('action', ['THROW_ERROR', 'DROP_PARTITION_INDEX', 'IGNORE'])
def test_commit_resolves_updated_index_columns_by_file_schema(native_rest_catalog, native, evolution, action):
    catalog = native_rest_catalog
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.identity', Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        'global-index.column-update-action': action,
    }), False)
    original = catalog.get_table('default.identity')
    writer = original.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(pa.table({'id': [1], 'value': [10]}, schema=schema))
        _commit(original, writer.prepare_commit())
    finally:
        writer.close()
    if evolution == 'rename':
        catalog.alter_table('default.identity', [SchemaChange.rename_column('value', 'renamed')])
    elif evolution == 'drop_readd':
        catalog.alter_table('default.identity', [
            SchemaChange.drop_column('value'), SchemaChange.add_column('value', AtomicType('INT'))])
    latest = catalog.get_table('default.identity')
    index_field = latest.fields[1]
    _commit(latest, [CommitMessage((), 0, [], index_adds=[_index_entry('identity.index', (), index_field.id)])])
    before = latest.snapshot_manager().get_latest_snapshot()
    # Prepare externally with IGNORE so the committer, rather than the updater,
    # has to enforce its current policy against the latest index manifest.
    source = (latest if evolution == 'complete_row' else latest.copy({'scan.snapshot-id': '1'})).copy({
        'global-index.column-update-action': 'IGNORE'})
    columns = ['id', 'value'] if evolution == 'complete_row' else ['value']
    values = {'_ROW_ID': [0], 'value': [11]}
    if evolution == 'complete_row':
        values['id'] = [1]
    messages = source.new_batch_write_builder().new_update().new_update_by_row_id().update_columns(
        pa.table(values), columns)
    assert messages
    if evolution == 'complete_row':
        # Java/Native may omit an explicit full column list. The physical
        # file is identical; commit policy must handle either representation.
        assert messages[0].new_files[0].write_cols in (None, columns)
        messages[0].new_files[0].write_cols = None
    else:
        assert messages[0].new_files[0].schema_id == original.table_schema.id
    target = latest.copy({'commit.native.enabled': str(native).lower()})
    if action == 'THROW_ERROR' and evolution != 'drop_readd':
        with pytest.raises(Exception, match='globally indexed columns'):
            _commit(target, messages, native)
        assert latest.snapshot_manager().get_latest_snapshot().id == before.id
    else:
        _commit(target, messages, native)
        after = latest.snapshot_manager().get_latest_snapshot()
        assert after.id == before.id + 1
        indexes = scan_global_index_entries(latest, after)
        expected = [] if action == 'DROP_PARTITION_INDEX' and evolution != 'drop_readd' else ['identity.index']
        assert [entry.index_file.file_name for entry in indexes] == expected
        read = latest.new_read_builder()
        rows = read.new_read().to_arrow(read.new_scan().plan().splits()).to_pydict()
        assert rows == {'id': [1], index_field.name: [None if evolution == 'drop_readd' else 11]}
