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

"""MERGE conditions use the independently selected source read schema."""

from contextlib import ExitStack
import importlib.util
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from pypaimon import Schema
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col


pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]
_HAS_PYTHON_DATAFUSION = importlib.util.find_spec('datafusion') is not None
_ENGINES = [pytest.param(False, marks=pytest.mark.skipif(
    not _HAS_PYTHON_DATAFUSION, reason='Python MERGE conditions require the datafusion extra')),
    pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
        not native_runtime_available(), reason='pypaimon-rust not installed')])]
_SCHEMA = pa.schema([('key', pa.int64()), ('value', pa.int32())])


def _append(table, key, value):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(pa.table({'key': [key], 'value': [value]}, schema=_SCHEMA))
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()


def _new_table(catalog, name, evolution=True):
    options = {
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false', 'row-tracking.enabled': 'true',
    }
    if evolution:
        options['data-evolution.enabled'] = 'true'
    catalog.create_table(name, Schema.from_pyarrow_schema(_SCHEMA, options=options), False)
    return catalog.get_table(name)


def _rows(table, metadata=()):
    builder = table.new_read_builder().with_projection(list(dict.fromkeys(table.field_names + list(metadata))))
    return builder.new_read().to_arrow(builder.new_scan().plan_all_files().splits()).to_pylist()


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('historical', [False, True], ids=['current-source', 'historical-source'])
@pytest.mark.parametrize('metadata', ['_ROW_ID', '_SEQUENCE_NUMBER'])
@pytest.mark.parametrize('action', ['matched', 'insert'])
def test_table_source_metadata_conditions_keep_selected_view_and_callable_rows(
        native_rest_catalog, native, stream, historical, metadata, action):
    source = _new_table(native_rest_catalog, 'default.metadata_source', evolution=False)
    for key, value in [(9, 91), (8, 82), (7, 73)]:
        _append(source, key, value)
    source_rows = sorted(_rows(source, ['_ROW_ID', '_SEQUENCE_NUMBER']), key=lambda row: row['_ROW_ID'])
    first, second, third = [row[metadata] for row in source_rows]
    assert len({first, second, third}) == 3
    target = _new_table(native_rest_catalog, 'default.metadata_target')
    _append(target, first, 10)
    if action == 'matched':
        _append(target, second, 20)
    source = source.copy({'scan.snapshot-id': '1'}) if historical else source.copy({})
    target = target.copy({'write.native.enabled': str(native).lower()})
    called = []

    def assignment(batch):
        called.append(batch.column('s.' + metadata).to_pylist())
        return pc.add(batch.column('s.value'), pa.scalar(100, type=pa.int32()))

    condition = 's.{} = {}'.format(metadata, second)
    if action == 'matched':
        matched = [WhenMatched.update({'value': assignment}, condition=condition),
                   WhenMatched.update({'value': source_col('value')})]
        not_matched = []
    else:
        matched = [WhenMatched.update({'value': source_col('value')})]
        not_matched = [WhenNotMatched({'value': assignment}, condition=condition),
                       WhenNotMatched({'value': source_col('value')})]
    builder = target.new_stream_write_builder() if stream else target.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.execution.python.write.merge_into._normalize_source',
                                      side_effect=AssertionError('Python source materialization')))
            stack.enter_context(patch('pypaimon.execution.python.write.merge_into._build_tables',
                                      side_effect=AssertionError('Python MERGE matching')))
            stack.enter_context(patch('pypaimon.execution.merge_condition._load_datafusion',
                                      side_effect=AssertionError('Native conditions loaded Python DataFusion')))
        messages = builder.new_update().merge_into(
            source, on={'key': metadata}, when_matched=matched, when_not_matched=not_matched,
            **({'commit_identifier': 71} if stream else {}))
    assert called == ([] if historical else [[second]])
    committer = builder.new_commit()
    try:
        committer.commit(messages, 71) if stream else committer.commit(messages)
    finally:
        committer.close()
    expected = [{'key': first, 'value': 91, '_ROW_ID': 0}]
    if action == 'matched':
        expected.append({'key': second, 'value': 20 if historical else 182, '_ROW_ID': 1})
    elif not historical:
        expected.extend([{'key': second, 'value': 182, '_ROW_ID': 1},
                         {'key': third, 'value': 73, '_ROW_ID': 2}])
    assert sorted(_rows(target, ['_ROW_ID']), key=lambda row: row['_ROW_ID']) == expected


@pytest.mark.parametrize('table_source', [False, True], ids=['arrow-source', 'table-source'])
def test_native_condition_transport_does_not_load_python_datafusion(native_rest_catalog, table_source):
    target = _new_table(native_rest_catalog, 'default.no_python_engine').copy({'write.native.enabled': 'true'})
    source = (_new_table(native_rest_catalog, 'default.metadata_source') if table_source else
              pa.table({'key': [0], 'value': [91]}, schema=_SCHEMA))
    source_key = '_ROW_ID' if table_source else 'key'
    native_table, native_source = Mock(), Mock()
    core = native_table.new_batch_write_builder.return_value._with_commit_user.return_value.new_update.return_value
    core.merge_into.return_value = []
    with patch('pypaimon.execution.native.write.native_merge_into._native_row_id_table', return_value=native_table), \
            patch(
                'pypaimon.execution.native.write.native_merge_into.create_native_write_table',
                return_value=native_source), \
            patch('pypaimon.execution.merge_condition._load_datafusion',
                  side_effect=AssertionError('Loaded optional Python execution engine')) as python_engine:
        messages = target.new_batch_write_builder().new_update().merge_into(
            source, on={'key': source_key}, when_matched=[WhenMatched.update(
                {'value': source_col('value')}, condition='s.{} = 0'.format(source_key))])
    assert messages == []
    python_engine.assert_not_called()
    core.merge_into.assert_called_once()
    args, kwargs = core.merge_into.call_args
    assert args[0] is (native_source if table_source else source)
    assert kwargs['when_matched'][0]['condition'] == {'sql': '"s.{}" = 0'.format(source_key)}


def test_python_merge_conditions_still_require_the_python_execution_engine(native_rest_catalog):
    target = _new_table(native_rest_catalog, 'default.python_engine_required')
    source = pa.table({'key': [0], 'value': [91]}, schema=_SCHEMA)
    with patch('pypaimon.execution.merge_condition._load_datafusion',
               side_effect=ImportError('Install pypaimon[datafusion]')) as python_engine, \
            patch('pypaimon.execution.python.write.merge_into._build_tables') as matching:
        with pytest.raises(ImportError, match=r'pypaimon\[datafusion\]'):
            target.new_batch_write_builder().new_update().merge_into(
                source, on=['key'], when_matched=[WhenMatched.update(
                    {'value': source_col('value')}, condition='s.key = 0')])
    python_engine.assert_called_once()
    matching.assert_not_called()
