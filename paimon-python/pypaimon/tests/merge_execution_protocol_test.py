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

"""Transport and backend selection for distributed MERGE action batches."""

import pickle
import subprocess
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.write.backend_dispatch import create_merge_batch_transform
from pypaimon.write.merge_types import (
    LiteralValue, SourceColumnRef, TargetColumnRef, _NormalizedClause,
)


@pytest.fixture
def native_protocol(monkeypatch):
    calls = []
    execute = Mock()

    class MergeBatchExecution:
        def __init__(self, input_schema, output_schema, clauses, mode):
            calls.append((input_schema, output_schema, clauses, mode))

        def execute(self, batch):
            return execute(batch, calls[-1])

    runtime = ModuleType('pypaimon_rust')
    runtime.__path__ = []
    datafusion = ModuleType('pypaimon_rust.datafusion')
    datafusion.MergeBatchExecution = MergeBatchExecution
    monkeypatch.setitem(sys.modules, 'pypaimon_rust', runtime)
    monkeypatch.setitem(sys.modules, 'pypaimon_rust.datafusion', datafusion)
    return SimpleNamespace(calls=calls, execute=execute, module=datafusion)


def _joined_batch():
    return pa.table({
        't._ROW_ID': pa.array([7, 8], pa.int64()),
        't.id': pa.array([1, 2], pa.int32()),
        't.value': pa.array([10, 20], pa.int32()),
        's.value': pa.array([100, 200], pa.int32()),
        '_scratch': [0, 0],
    })


def _update_schema():
    return pa.schema([
        pa.field('_ROW_ID', pa.int64(), nullable=False),
        pa.field('value', pa.int32()),
    ])


def _native_update(clauses, callable_input_columns=None):
    return create_merge_batch_transform(
        'update', clauses, {'source_id': 'id'}, [('source_id', 'id')],
        ['value'], '_ROW_ID', _update_schema(), callable_input_columns,
        native_enabled=True,
    )


def test_native_update_restores_callable_aliases_and_preserves_result_chunks(native_protocol):
    observed = []
    chunks = pa.chunked_array([
        pa.array([101], pa.int32()), pa.array([202], pa.int32()),
    ])

    def values(view):
        observed.append(view)
        return chunks

    transform = _native_update([_NormalizedClause(
        {'value': values}, 's.source_id > 0',
    )])
    batch = _joined_batch()
    result = pa.Table.from_arrays([
        batch['t._ROW_ID'], chunks,
    ], schema=_update_schema())

    def execute(rows, compiled):
        input_schema, output_schema, clauses, mode = compiled
        assert input_schema == batch.schema
        assert output_schema == _update_schema()
        assert mode == 'update'
        assert clauses[0]['condition'] == {'sql': '"t.id" > 0'}
        name, kind, callback = clauses[0]['assignments'][0]
        assert (name, kind) == ('value', 'callable')
        assert callback(rows).equals(result['value'])
        return result

    native_protocol.execute.side_effect = execute
    assert not observed
    actual = transform(batch)
    assert actual is result
    assert actual['value'].num_chunks == 2
    assert len(observed) == 1
    assert observed[0].column_names == ['s.source_id', 's.value', 't._ROW_ID', 't.id', 't.value']
    assert observed[0]['s.source_id'].equals(batch['t.id'])


def test_native_self_update_keeps_explicit_callable_read_order(native_protocol):
    observed = []

    def values(view):
        observed.append(view)
        return view['value']

    transform = _native_update(
        [_NormalizedClause({'value': values})], ['value', '_ROW_ID', 'id'],
    )
    batch = _joined_batch()

    def execute(rows, compiled):
        callback = compiled[2][0]['assignments'][0][2]
        return pa.Table.from_arrays([rows['t._ROW_ID'], callback(rows)], schema=_update_schema())

    native_protocol.execute.side_effect = execute
    assert transform(batch).to_pylist() == [{'_ROW_ID': 7, 'value': 10}, {'_ROW_ID': 8, 'value': 20}]
    assert observed[0].column_names == ['value', '_ROW_ID', 'id']


@pytest.mark.parametrize('operation', ['update', 'delete'])
def test_native_custom_row_id_preserves_aliases_and_output_schema(native_protocol, operation):
    observed = []

    def values(view):
        observed.append(view)
        return view['t.value']

    schema = pa.schema([pa.field('custom_id', pa.int64(), nullable=False)])
    if operation == 'update':
        schema = schema.append(pa.field('value', pa.int32()))
        transform = create_merge_batch_transform(
            'update', [_NormalizedClause({'value': values}, 't.custom_id > 0')], {}, [],
            ['value'], 'custom_id', schema, native_enabled=True,
        )
    else:
        transform = create_merge_batch_transform(
            'delete', [_NormalizedClause({}, 't.custom_id > 0', delete=True)], {},
            'custom_id', schema, native_enabled=True,
        )
    batch = pa.table({'t.custom_id': pa.array([7, 8], pa.int64()), 't.value': pa.array([10, 20], pa.int32())})

    def execute(rows, compiled):
        assert rows['t._ROW_ID'].equals(batch['t.custom_id'])
        assert compiled[2][0]['condition'] == {'sql': '"t.custom_id" > 0'}
        assert compiled[1].names[0] == '_ROW_ID'
        columns = [rows['t._ROW_ID']]
        if operation == 'update':
            columns.append(compiled[2][0]['assignments'][0][2](rows))
        return pa.Table.from_arrays(columns, schema=compiled[1])

    native_protocol.execute.side_effect = execute
    result = transform(batch)
    assert result.schema == schema
    assert result['custom_id'].to_pylist() == [7, 8]
    if operation == 'update':
        assert observed[0].column_names == ['t.custom_id', 't.value']


def test_native_transports_coalesced_and_missing_column_assignments(native_protocol):
    clauses = [_NormalizedClause({
        'key': SourceColumnRef('source_id'),
        'source': SourceColumnRef('value'),
        'target': TargetColumnRef('value'),
        'missing_source': SourceColumnRef('absent'),
        'missing_target': TargetColumnRef('absent'),
        'constant': LiteralValue(9),
    })]
    transform = _native_update(clauses)
    native_protocol.execute.return_value = _update_schema().empty_table()
    transform(_joined_batch())
    assert native_protocol.calls[0][2][0]['assignments'] == [
        ('key', 'target', 'id'), ('source', 'source', 'value'),
        ('target', 'target', 'value'), ('missing_source', 'literal', None),
        ('missing_target', 'literal', None), ('constant', 'literal', 9),
    ]


def test_native_insert_callback_retains_source_aliases_and_dictionary_types(native_protocol):
    batch = pa.table({
        's.source_id': pa.array([1, 2], pa.int32()).dictionary_encode(),
        's.value': pa.array([10, 20], pa.int32()),
        '_scratch': [0, 0],
    })
    observed = []

    def values(view):
        observed.append(view)
        return view['s.value']

    schema = pa.schema([('value', pa.int32())])
    transform = create_merge_batch_transform(
        'insert', [_NormalizedClause({'value': values})], ['value'], schema,
        native_enabled=True,
    )

    def execute(rows, compiled):
        assert compiled[3] == 'insert'
        return pa.Table.from_arrays([compiled[2][0]['assignments'][0][2](rows)], schema=schema)

    native_protocol.execute.side_effect = execute
    assert transform(batch)['value'].to_pylist() == [10, 20]
    assert observed[0].column_names == ['s.source_id', 's.value']
    assert observed[0]['s.source_id'].type == batch['s.source_id'].type


@pytest.mark.parametrize('operation', ['update', 'delete', 'insert'])
def test_native_transform_keeps_empty_schema_and_compiled_state_local(native_protocol, operation):
    clauses = [_NormalizedClause({'value': SourceColumnRef('value')})]
    schema = _update_schema()
    if operation == 'update':
        transform = _native_update(clauses)
    elif operation == 'delete':
        schema = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False)])
        transform = create_merge_batch_transform(
            'delete', clauses, {'source_id': 'id'}, '_ROW_ID', schema, native_enabled=True,
        )
    else:
        schema = pa.schema([pa.field('value', pa.int32())])
        transform = create_merge_batch_transform(
            'insert', clauses, ['value'], schema, native_enabled=True,
        )
    batch = _joined_batch()
    empty = transform(batch.slice(0, 0))
    assert empty.schema == schema
    assert empty.num_rows == 0
    assert native_protocol.calls == []
    native_protocol.execute.return_value = schema.empty_table()
    transform(batch)
    transform(batch)
    assert len(native_protocol.calls) == 1
    assert native_protocol.calls[0][3] == operation
    restored = pickle.loads(pickle.dumps(transform))
    restored(batch)
    assert len(native_protocol.calls) == 2


@pytest.mark.parametrize('phase', ['compile', 'execute', 'callable'])
def test_native_failure_preserves_exception_identity_without_python_replay(native_protocol, phase):
    failure = ValueError('MERGE failure after selecting native execution')

    def callback(rows):
        raise failure

    if phase == 'compile':
        native_protocol.module.MergeBatchExecution = Mock(side_effect=failure)
    elif phase == 'execute':
        native_protocol.execute.side_effect = failure
    else:
        native_protocol.execute.side_effect = lambda rows, compiled: compiled[2][0]['assignments'][0][2](rows)
    with patch('pypaimon.execution.python.write.merge_transform._build_matched_transform') as python:
        transform = _native_update([_NormalizedClause({'value': callback})])
        with pytest.raises(ValueError) as caught:
            transform(_joined_batch())
        assert caught.value is failure
        python.assert_not_called()


@pytest.mark.parametrize('native_enabled', [False, True])
@pytest.mark.parametrize('self_merge', [False, True])
def test_ray_merge_seals_table_backend_for_every_action(native_enabled, self_merge):
    from pypaimon.ray import data_evolution_merge_into as merge

    requested = Mock(return_value=native_enabled)
    table = SimpleNamespace(options=SimpleNamespace(native_write_enabled=requested))
    ctx = SimpleNamespace(
        is_self_merge=self_merge, full_target_field_names=['id', 'value'],
        settable_field_names=['id', 'value'], target_on_cols=['id'], source_on_cols=['source_id'],
        update_pa_schema=_update_schema(), full_pa_schema=_update_schema(),
        catalog_options={'warehouse': '/tmp/merge'}, self_merge_scan_predicate=None,
        read_columns=('value',),
    )
    names = (['build_self_merge_update_plan', 'build_self_merge_delete_ds'] if self_merge else
             ['build_matched_update_ds', 'build_matched_delete_ds', 'build_not_matched_insert_ds'])
    builders = {name: Mock(return_value=object()) for name in names}
    matched = [_NormalizedClause({'value': LiteralValue(1)}), _NormalizedClause({}, delete=True)]
    with patch.multiple(merge, **builders):
        merge._build_datasets(
            table, 'default.t', object(), matched, [_NormalizedClause({'value': LiteralValue(2)})],
            ctx, SimpleNamespace(id=3, total_record_count=1), 2, None,
        )
    requested.assert_called_once_with()
    for builder in builders.values():
        builder.assert_called_once()
        assert builder.call_args.kwargs['native_enabled'] is native_enabled


def test_missing_optional_runtime_selects_python_before_consuming_input():
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=False) as available, \
            patch('pypaimon.execution.native.write.merge_transform._build_matched_transform') as native:
        transform = _native_update([_NormalizedClause({'value': LiteralValue(5)})])
        result = transform(_joined_batch())
    assert result.to_pylist() == [{'_ROW_ID': 7, 'value': 5}, {'_ROW_ID': 8, 'value': 5}]
    available.assert_called_once_with()
    native.assert_not_called()


def test_incompatible_installed_runtime_is_not_replayed_in_python(native_protocol):
    del native_protocol.module.MergeBatchExecution
    with patch('pypaimon.execution.python.write.merge_transform._build_matched_transform') as python:
        with pytest.raises(ImportError):
            _native_update([_NormalizedClause({'value': LiteralValue(5)})])
        python.assert_not_called()


@pytest.mark.parametrize('native_enabled', [False, True])
def test_merge_transform_imports_only_its_selected_backend(native_enabled):
    source = r'''
import importlib.abc
import sys
from types import ModuleType
import pyarrow as pa

native_enabled = NATIVE_ENABLED
blocked = 'pypaimon.execution.python' if native_enabled else 'pypaimon.execution.native'
class RejectOtherBackend(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(blocked) or (not native_enabled and fullname.startswith('pypaimon_rust')):
            raise AssertionError('Loaded disabled backend: ' + fullname)
sys.meta_path.insert(0, RejectOtherBackend())
if native_enabled:
    class MergeBatchExecution:
        def __init__(self, input_schema, output_schema, clauses, mode):
            self.output_schema = output_schema
        def execute(self, batch):
            return self.output_schema.empty_table()
    module = ModuleType('pypaimon_rust.datafusion')
    module.MergeBatchExecution = MergeBatchExecution
    sys.modules['pypaimon_rust.datafusion'] = module
from pypaimon.ray.data_evolution_merge_join import _build_matched_transform
from pypaimon.write.merge_types import LiteralValue, _NormalizedClause
schema = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False), pa.field('value', pa.int32())])
transform = _build_matched_transform([_NormalizedClause({'value': LiteralValue(5)})], {}, [],
                                    ['value'], '_ROW_ID', schema, native_enabled=native_enabled)
result = transform(pa.table({'t._ROW_ID': pa.array([7], pa.int64()), 't.value': pa.array([10], pa.int32())}))
assert result.schema == schema
assert result.num_rows == (0 if native_enabled else 1)
'''.replace('NATIVE_ENABLED', repr(native_enabled))
    result = subprocess.run([sys.executable, '-c', source], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stderr
