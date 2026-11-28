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

"""Public table routing for nested-sequence-overwrite-on-equal=false.

Native 0.5.x does not implement the equal-sequence retention policy. When any
nested_update field sets the option to false, both Native read and Native write
must yield to Python even if the Rust runtime is installed and Native defaults
are enabled.
"""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.data_evolution_merge_into import WhenMatched, source_col
from pypaimon.common.native_compatibility import (
    nested_update_overwrite_on_equal_false_reason,
    require_native_compatible,
)
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.write.table_write import BatchTableWrite
from pypaimon.write.write_dispatch import NativeBatchTableWrite


_NESTED = pa.struct([('key', pa.int64()), ('value', pa.string()), ('seq', pa.int64())])
_SCHEMA = pa.schema([
    ('id', pa.int64()),
    ('updates', pa.list_(_NESTED)),
])


def _options(overwrite=None, **extra):
    options = {
        'bucket': '1',
        'file.format': 'parquet',
        'merge-engine': 'aggregation',
        'fields.updates.aggregate-function': 'nested_update',
        'fields.updates.nested-key': 'key',
        'fields.updates.nested-sequence-field': 'seq',
        'read.native.enabled': 'true',
        'write.native.enabled': 'true',
        'commit.native.enabled': 'false',
    }
    if overwrite is not None:
        options['fields.updates.nested-sequence-overwrite-on-equal'] = overwrite
    options.update(extra)
    return options


def _table(tmp_path, overwrite=None, name='t', **extra):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / name)})
    catalog.create_database('default', True)
    catalog.create_table(
        'default.t',
        Schema.from_pyarrow_schema(_SCHEMA, primary_keys=['id'], options=_options(overwrite, **extra)),
        False)
    return catalog.get_table('default.t')


def _batch(first='first', second='second'):
    return pa.Table.from_pylist([
        {'id': 1, 'updates': [{'key': 1, 'value': first, 'seq': 7}]},
        {'id': 1, 'updates': [{'key': 1, 'value': second, 'seq': 7}]},
    ], schema=_SCHEMA)


def _read_values(table):
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    rows = builder.new_read().to_arrow(splits).to_pylist()
    assert len(rows) == 1
    return [item['value'] for item in rows[0]['updates']]


def _default_nested_options(overwrite='false', field_agg=None, default_agg='nested_update'):
    options = {
        'fields.default-aggregate-function': default_agg,
        'fields.updates.nested-key': 'key',
        'fields.updates.nested-sequence-field': 'seq',
        'fields.updates.nested-sequence-overwrite-on-equal': overwrite,
    }
    if field_agg is not None:
        options['fields.updates.aggregate-function'] = field_agg
    return options


@pytest.mark.python_read
@pytest.mark.python_write
def test_compatibility_helper_only_flags_explicit_false():
    assert nested_update_overwrite_on_equal_false_reason(CoreOptions(Options({}))) is None
    assert nested_update_overwrite_on_equal_false_reason(CoreOptions(Options(_options('true')))) is None
    assert nested_update_overwrite_on_equal_false_reason(CoreOptions(Options(_options(None)))) is None
    reason = nested_update_overwrite_on_equal_false_reason(CoreOptions(Options(_options('false'))))
    assert reason is not None
    assert 'nested-sequence-overwrite-on-equal=false' in reason
    with pytest.raises(RuntimeError, match='without a Python fallback'):
        require_native_compatible(CoreOptions(Options(_options('false'))), allow_python_fallback=False)


@pytest.mark.python_read
@pytest.mark.python_write
def test_compatibility_helper_honors_default_aggregate_function():
    # default nested_update + false → incompatible
    reason = nested_update_overwrite_on_equal_false_reason(
        CoreOptions(Options(_default_nested_options('false'))))
    assert reason is not None
    assert 'nested-sequence-overwrite-on-equal=false' in reason

    # default nested_update + field-level last_value override → still Native-eligible
    assert nested_update_overwrite_on_equal_false_reason(
        CoreOptions(Options(_default_nested_options('false', field_agg='last_value')))) is None

    # default other + field-level nested_update + false → incompatible
    reason = nested_update_overwrite_on_equal_false_reason(
        CoreOptions(Options(_default_nested_options(
            'false', field_agg='nested_update', default_agg='last_value'))))
    assert reason is not None

    # default nested_update + true → eligible
    assert nested_update_overwrite_on_equal_false_reason(
        CoreOptions(Options(_default_nested_options('true')))) is None


@pytest.mark.python_read
@pytest.mark.python_write
def test_native_read_bypassed_for_overwrite_on_equal_false(tmp_path):
    table = _table(tmp_path, 'false', name='read_false')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        # Force Python write for the seed so this case isolates read routing.
        assert isinstance(writer, BatchTableWrite)
        assert not isinstance(writer, NativeBatchTableWrite)
        writer.write_arrow(_batch())
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    # Prove routing does not depend on the Rust runtime being absent.
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.read.table_read.NativeTableRead') as native_read, \
            patch('pypaimon.execution.native.write.native_write.create_native_write',
                  side_effect=AssertionError('create_native_write must not run for read')):
        values = _read_values(table)
    native_read.assert_not_called()
    assert values == ['first']


@pytest.mark.python_read
@pytest.mark.python_write
def test_native_write_bypassed_for_overwrite_on_equal_false(tmp_path):
    table = _table(tmp_path, 'false', name='write_false')
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.native_write.create_native_write') as create_native, \
            patch('pypaimon.write.write_dispatch.NativeBatchTableWrite',
                  side_effect=AssertionError('NativeBatchTableWrite selected')):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            assert isinstance(writer, BatchTableWrite)
            assert not isinstance(writer, NativeBatchTableWrite)
            writer.write_arrow(_batch())
            messages = writer.prepare_commit()
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()
    create_native.assert_not_called()

    # Re-open and read through the public API after commit / persistence.
    reopened = CatalogFactory.create({'warehouse': str(tmp_path / 'write_false')}).get_table('default.t')
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.read.table_read.NativeTableRead') as native_read:
        assert _read_values(reopened) == ['first']
    native_read.assert_not_called()

    # A later equal-sequence write must still keep first after another commit.
    builder = reopened.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, BatchTableWrite)
        writer.write_arrow(_batch('ignored', 'also-ignored'))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    with patch('pypaimon.execution.native.read.table_read.NativeTableRead') as native_read:
        assert _read_values(reopened) == ['first']
    native_read.assert_not_called()


@pytest.mark.python_read
@pytest.mark.python_write
@pytest.mark.parametrize('overwrite', [None, 'true'])
def test_default_and_true_keep_native_eligible(tmp_path, overwrite):
    table = _table(tmp_path, overwrite, name='compat_%s' % (overwrite or 'default'))
    assert nested_update_overwrite_on_equal_false_reason(table.options) is None
    assert require_native_compatible(table.options, allow_python_fallback=True) is None
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(_batch('first', 'second'))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    # Default / explicit true keep historical equal-sequence overwrite.
    assert _read_values(table) == ['second']


@pytest.mark.python_read
@pytest.mark.python_write
def test_default_aggregate_function_nested_update_false_routes_to_python(tmp_path):
    """Table-wide default nested_update + overwrite=false must keep first."""
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'default_agg')})
    catalog.create_database('default', True)
    options = {
        'bucket': '1',
        'file.format': 'parquet',
        'merge-engine': 'aggregation',
        'fields.default-aggregate-function': 'nested_update',
        'fields.updates.nested-key': 'key',
        'fields.updates.nested-sequence-field': 'seq',
        'fields.updates.nested-sequence-overwrite-on-equal': 'false',
        'read.native.enabled': 'true',
        'write.native.enabled': 'true',
        'commit.native.enabled': 'false',
    }
    catalog.create_table(
        'default.t',
        Schema.from_pyarrow_schema(_SCHEMA, primary_keys=['id'], options=options),
        False)
    table = catalog.get_table('default.t')
    assert nested_update_overwrite_on_equal_false_reason(table.options) is not None
    with pytest.raises(RuntimeError, match='without a Python fallback'):
        require_native_compatible(table.options, allow_python_fallback=False)

    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.native_write.create_native_write') as create_native, \
            patch('pypaimon.write.write_dispatch.NativeBatchTableWrite',
                  side_effect=AssertionError('NativeBatchTableWrite selected')):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            assert isinstance(writer, BatchTableWrite)
            assert not isinstance(writer, NativeBatchTableWrite)
            writer.write_arrow(_batch())
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    create_native.assert_not_called()

    reopened = CatalogFactory.create({'warehouse': str(tmp_path / 'default_agg')}).get_table('default.t')
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.read.table_read.NativeTableRead') as native_read:
        assert _read_values(reopened) == ['first']
    native_read.assert_not_called()
    with pytest.raises(RuntimeError, match='without a Python fallback'):
        require_native_compatible(reopened.options, allow_python_fallback=False)


def _commit_arrow(table, data):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        messages = writer.prepare_commit()
        assert messages
        commit.commit(messages)
        return messages
    finally:
        writer.close()
        commit.close()


@pytest.mark.python_read
@pytest.mark.python_write
def test_table_source_overwrite_on_equal_false_falls_back_before_native_source(tmp_path):
    """A Paimon source with overwrite=false must use Python MERGE."""
    source = _table(tmp_path, 'false', name='merge_source')
    _commit_arrow(source, _batch('first', 'second'))

    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'merge_target')})
    catalog.create_database('default', True)
    catalog.create_table(
        'default.t',
        Schema.from_pyarrow_schema(_SCHEMA, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'file.format': 'parquet',
            'write.native.enabled': 'false',
        }),
        False)
    target = catalog.get_table('default.t')
    _commit_arrow(target, pa.Table.from_pylist([
        {'id': 1, 'updates': [{'key': 1, 'value': 'seed', 'seq': 1}]},
    ], schema=_SCHEMA))
    merge_target = target.copy({'write.native.enabled': 'true'})

    def _reject_native_source(table):
        if table.table_path == source.table_path:
            raise AssertionError('Native source must not be created')
        return object()

    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.native_update.create_native_write_table',
                  side_effect=_reject_native_source), \
            patch('pypaimon.execution.native.write.native_commit.create_native_write_table',
                  side_effect=_reject_native_source), \
            patch('pypaimon.execution.native.table._native_table',
                  side_effect=AssertionError('Native source must not be created')):
        builder = merge_target.new_batch_write_builder()
        update, commit = builder.new_update(), builder.new_commit()
        try:
            messages = update.merge_into(
                source, on=['id'],
                when_matched=[WhenMatched.update({'updates': source_col('updates')})])
            assert messages
            commit.commit(messages)
        finally:
            commit.close()

    reopened = CatalogFactory.create({'warehouse': str(tmp_path / 'merge_target')}).get_table('default.t')
    assert _read_values(reopened) == ['first']


_ROW_SEQ = pa.struct([('event', pa.int64()), ('sub', pa.int32())])
_ROW_NESTED = pa.struct([
    ('key', pa.int64()),
    ('value', pa.string()),
    ('seq', _ROW_SEQ),
])
_ROW_SCHEMA = pa.schema([
    ('id', pa.int64()),
    ('updates', pa.list_(_ROW_NESTED)),
])


@pytest.mark.python_read
@pytest.mark.python_write
def test_sequence_field_rejected_with_nested_first_wins(tmp_path):
    """Outer sequence.field reorders merges; first-wins cannot stay cross-commit consistent."""
    from pypaimon.read.merge_engine_support import check_nested_update_configuration
    from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite

    schema = pa.schema([
        ('id', pa.int64()),
        ('ord', pa.int64()),
        ('updates', pa.list_(_NESTED)),
    ])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'seq_ord')})
    catalog.create_database('default', True)
    options = _options('false', **{'sequence.field': 'ord'})
    catalog.create_table(
        'default.t',
        Schema.from_pyarrow_schema(schema, primary_keys=['id'], options=options),
        False)
    table = catalog.get_table('default.t')

    with pytest.raises(ValueError, match='sequence.field is incompatible'):
        check_nested_update_configuration(table)
    with pytest.raises(ValueError, match='sequence.field is incompatible'):
        table.new_batch_write_builder().new_write()
    with pytest.raises(ValueError, match='sequence.field is incompatible'):
        BatchTableWrite(table, 'test')
    with pytest.raises(ValueError, match='sequence.field is incompatible'):
        StreamTableWrite(table, 'test')
    with pytest.raises(ValueError, match='sequence.field is incompatible'):
        table.new_read_builder().new_read()

    # Compatible: nested_update + sequence.field with overwrite=true stays allowed.
    compatible = _table(tmp_path, 'true', name='seq_ok', **{'sequence.field': 'id'})
    check_nested_update_configuration(compatible)
    writer = compatible.new_batch_write_builder().new_write()
    writer.close()


@pytest.mark.python_read
@pytest.mark.python_write
@pytest.mark.parametrize('overwrite', ['true', 'false'])
@pytest.mark.parametrize('via_default_agg', [False, True])
def test_explicit_overwrite_requires_nested_sequence_before_backend(
        tmp_path, overwrite, via_default_agg):
    """Explicit true/false must fail before Native/Python writer backend selection."""
    from pypaimon.write.backend_dispatch import _prepare_write
    from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite

    catalog = CatalogFactory.create({
        'warehouse': str(tmp_path / ('dep_%s_%s' % (overwrite, via_default_agg))),
    })
    catalog.create_database('default', True)
    options = {
        'bucket': '1',
        'file.format': 'parquet',
        'merge-engine': 'aggregation',
        'fields.updates.nested-key': 'key',
        'fields.updates.nested-sequence-overwrite-on-equal': overwrite,
        'read.native.enabled': 'true',
        'write.native.enabled': 'true',
        'commit.native.enabled': 'false',
    }
    if via_default_agg:
        options['fields.default-aggregate-function'] = 'nested_update'
    else:
        options['fields.updates.aggregate-function'] = 'nested_update'
    catalog.create_table(
        'default.t',
        Schema.from_pyarrow_schema(_SCHEMA, primary_keys=['id'], options=options),
        False)
    table = catalog.get_table('default.t')
    match = 'nested-sequence-overwrite-on-equal'
    with pytest.raises(ValueError, match=match):
        table.new_batch_write_builder().new_write()
    with pytest.raises(ValueError, match=match):
        BatchTableWrite(table, 'test')
    with pytest.raises(ValueError, match=match):
        StreamTableWrite(table, 'test')
    with pytest.raises(ValueError, match=match):
        _prepare_write(table, 'test', None, False, None, False, None)
    with pytest.raises(ValueError, match=match):
        table.new_read_builder().new_read()


@pytest.mark.python_read
@pytest.mark.python_write
@pytest.mark.parametrize('overwrite', ['true', 'false'])
def test_overwrite_option_ignored_for_non_nested_update_fields(tmp_path, overwrite):
    """Overwrite-on-equal only constrains nested_update (Java factory semantics)."""
    from pypaimon.read.merge_engine_support import check_nested_update_configuration

    def _non_nested_table(name, **extra):
        catalog = CatalogFactory.create({'warehouse': str(tmp_path / name)})
        catalog.create_database('default', True)
        options = {
            'bucket': '1',
            'file.format': 'parquet',
            'merge-engine': 'aggregation',
            'fields.updates.aggregate-function': 'last_value',
            'fields.updates.nested-sequence-overwrite-on-equal': overwrite,
            'sequence.field': 'id',
            'read.native.enabled': 'true',
            'write.native.enabled': 'true',
            'commit.native.enabled': 'false',
        }
        options.update(extra)
        catalog.create_table(
            'default.t',
            Schema.from_pyarrow_schema(_SCHEMA, primary_keys=['id'], options=options),
            False)
        return catalog.get_table('default.t')

    # Field-level last_value: stray overwrite must not require nested-sequence-field,
    # and sequence.field + overwrite=false must not be rejected.
    table = _non_nested_table('ignore_%s' % overwrite)
    check_nested_update_configuration(table)
    writer = table.new_batch_write_builder().new_write()
    writer.close()

    # Default nested_update overridden by field-level last_value: also ignored.
    table2 = _non_nested_table(
        'ignore_default_%s' % overwrite,
        **{
            'fields.default-aggregate-function': 'nested_update',
            'fields.updates.aggregate-function': 'last_value',
        })
    check_nested_update_configuration(table2)
    writer2 = table2.new_batch_write_builder().new_write()
    writer2.close()


@pytest.mark.python_read
@pytest.mark.python_write
def test_unset_overwrite_keeps_default_with_sequence_field(tmp_path):
    """Unset overwrite-on-equal stays default true; sequence.field remains allowed."""
    from pypaimon.read.merge_engine_support import check_nested_update_configuration

    table = _table(tmp_path, None, name='default_seq', **{'sequence.field': 'id'})
    check_nested_update_configuration(table)
    writer = table.new_batch_write_builder().new_write()
    writer.close()


@pytest.mark.python_read
@pytest.mark.python_write
def test_row_sequence_equal_keeps_first_across_commit(tmp_path):
    """Same nested key and ROW sequence keeps the first payload."""
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'row_seq')})
    catalog.create_database('default', True)
    options = _options('false')
    catalog.create_table(
        'default.t',
        Schema.from_pyarrow_schema(_ROW_SCHEMA, primary_keys=['id'], options=options),
        False)
    table = catalog.get_table('default.t')
    sequence = {'event': 5, 'sub': 1}
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, BatchTableWrite)
        assert not isinstance(writer, NativeBatchTableWrite)
        writer.write_arrow(pa.Table.from_pylist([
            {'id': 1, 'updates': [{'key': 7, 'value': 'first', 'seq': sequence}]},
        ], schema=_ROW_SCHEMA))
        writer.write_arrow(pa.Table.from_pylist([
            {'id': 1, 'updates': [{'key': 7, 'value': 'second', 'seq': dict(sequence)}]},
        ], schema=_ROW_SCHEMA))
        messages = writer.prepare_commit()
        assert messages
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()

    reopened = CatalogFactory.create({'warehouse': str(tmp_path / 'row_seq')}).get_table('default.t')
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.read.table_read.NativeTableRead') as native_read:
        assert _read_values(reopened) == ['first']
    native_read.assert_not_called()
