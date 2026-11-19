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

"""Native gates for merge_map_with_keytime (UTF-8 vs UTF-16).

* Aggregate Reader: fall back to Python before Native opens.
* Partial Update Reader: raise before Native opens (Python unsupported).
* Writer: refuse Native so backend_dispatch selects Python Writer.
Non-keytime aggregators keep Native.
"""

from contextlib import ExitStack
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.execution.native.read.table_read import (
    NativeTableRead,
    _native_merge_map_keytime_timestamp_supported,
    _table_uses_merge_map_keytime,
)
from pypaimon.execution.native.write.native_write import (
    _native_write_refuses_merge_map_keytime,
    create_native_write,
)
from pypaimon.execution.python.write.table_write import PythonTableWrite
from pypaimon.schema.data_types import AtomicType, DataField, MapType, RowType
from pypaimon.write.table_write import BatchTableWrite
from pypaimon.write.write_dispatch import NativeTableWrite


def _options(**extra):
    data = {
        'bucket': '1',
        'merge-engine': 'aggregation',
        'file.format': 'parquet',
        'read.native.enabled': 'true',
        'write.native.enabled': 'false',
    }
    data.update(extra)
    return CoreOptions(Options(data))


def _value_row(ts_type, fields=None):
    if fields is not None:
        return RowType(True, fields)
    return RowType(True, [
        DataField(2, 'value', AtomicType('STRING')),
        DataField(3, 'event_time', AtomicType(ts_type)),
    ])


def _keytime_fields(value_type):
    return [
        DataField(0, 'id', AtomicType('STRING')),
        DataField(1, 'updates', MapType(True, AtomicType('STRING'), value_type)),
    ]


def _table(fields, options, primary_keys=('id',)):
    table = Mock()
    table.is_primary_key_table = bool(primary_keys)
    table.primary_keys = list(primary_keys)
    table.fields = fields
    table.options = options
    return table


def _split():
    split = Mock()
    split.files = [Mock(
        file_name='data-1.parquet', file_size=1, extra_files=[], write_cols=None)]
    split._native_split = object()
    return split


def _table_read(table):
    from pypaimon.read.table_read import TableRead
    from pypaimon.read.read_type import reader_adapter

    read = TableRead.__new__(TableRead)
    read.table = table
    read.predicate = None
    read.read_type = list(table.fields)
    read.include_row_kind = False
    read._row_kind_column = '_row_kind'
    read.output_projection = None
    read.limit = None
    read._read_parallelism = 1
    read._deferred_blob_fields = set()
    read._predicate_extra_fields = []
    read._adapter_read_type, _ = reader_adapter(read.read_type, table.fields)
    read._scan_read_type = read._adapter_read_type
    read._output_column_names = [field.name for field in read.read_type]
    table.options.native_read_enabled = Mock(return_value=True)
    table.options.file_format = Mock(return_value='parquet')
    table.options.row_tracking_enabled = Mock(return_value=False)
    table.options.data_evolution_enabled = Mock(return_value=False)
    table.options.blob_as_descriptor = Mock(return_value=False)
    table.options.blob_descriptor_fields = Mock(return_value=set())
    table.options.blob_view_fields = Mock(return_value=set())
    table.options.blob_view_resolve_enabled = Mock(return_value=True)
    return read


def _keytime_options(ts_field='event_time', agg='merge_map_with_keytime'):
    options = {
        'fields.updates.aggregate-function': agg,
    }
    if ts_field is not None:
        options['fields.updates.ts-field'] = ts_field
    return _options(**options)


def _assert_falls_back_before_native(table):
    assert _native_merge_map_keytime_timestamp_supported(table) is False
    read = _table_read(table)
    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert NativeTableRead(read).read_batches(
            [_split()], pa.schema([('id', pa.string())])) is None
    native.assert_not_called()


def _assert_stays_on_native(table, batch):
    assert _native_merge_map_keytime_timestamp_supported(table) is True
    read = _table_read(table)
    with patch('pypaimon.execution.native.read.table_read.native_read',
               return_value=[batch]) as native:
        assert list(NativeTableRead(read).read_batches(
            [_split()], batch.schema))
    native.assert_called_once()


# -- capability / early fallback (no Rust required) --------------------


@pytest.mark.parametrize('ts_type', [
    'STRING', 'CHAR(8)', 'VARCHAR(16)',
])
def test_string_keytime_falls_back_before_opening_native(ts_type):
    _assert_falls_back_before_native(_table(
        _keytime_fields(_value_row(ts_type)),
        _keytime_options()))


@pytest.mark.parametrize('ts_type', [
    'BOOLEAN', 'BINARY(4)', 'VARBINARY(8)', 'BYTES',
    'TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT',
    'FLOAT', 'DOUBLE',
    'DATE', 'TIME', 'DECIMAL(10, 2)',
    'TIMESTAMP(9)', 'TIMESTAMP_LTZ(9)', 'TIMESTAMP',
])
def test_non_string_keytime_stays_eligible_for_native(ts_type):
    table = _table(_keytime_fields(_value_row(ts_type)), _keytime_options())
    assert _native_merge_map_keytime_timestamp_supported(table) is True
    assert _table_uses_merge_map_keytime(table) is True
    batch = pa.record_batch(
        [pa.array(['1']), pa.array([None], type=pa.map_(
            pa.string(), pa.struct([
                ('value', pa.string()), ('event_time', pa.string())])))],
        names=['id', 'updates'])
    _assert_stays_on_native(table, batch)


def test_default_ts_field_string_falls_back_non_string_stays():
    double_last = _table(
        _keytime_fields(_value_row('DOUBLE')),
        _keytime_options(ts_field=None))
    assert _native_merge_map_keytime_timestamp_supported(double_last) is True

    string_last = _table(
        _keytime_fields(RowType(True, [
            DataField(2, 'event_time', AtomicType('DOUBLE')),
            DataField(3, 'ts', AtomicType('STRING')),
        ])),
        _keytime_options(ts_field=None))
    assert _native_merge_map_keytime_timestamp_supported(string_last) is False


def test_non_pk_non_aggregate_and_other_aggs_stay_on_native():
    fields = _keytime_fields(_value_row('STRING'))
    cases = (
        _table(fields, _keytime_options(), primary_keys=()),
        _table(fields, _options(
            **{'merge-engine': 'deduplicate',
               'fields.updates.aggregate-function': 'merge_map_with_keytime',
               'fields.updates.ts-field': 'event_time'})),
        _table(fields, _options(
            **{'fields.updates.aggregate-function': 'merge_map'})),
    )
    batch = pa.record_batch(
        [pa.array(['1']), pa.array([None], type=pa.map_(
            pa.string(), pa.struct([
                ('value', pa.string()), ('event_time', pa.string())])))],
        names=['id', 'updates'])
    for table in cases:
        _assert_stays_on_native(table, batch)

    # Plain partial-update without keytime must keep Native.
    pu = _table(
        [DataField(0, 'id', AtomicType('STRING')),
         DataField(1, 'v', AtomicType('STRING'))],
        _options(**{'merge-engine': 'partial-update'}))
    _assert_stays_on_native(
        pu, pa.record_batch([pa.array(['1']), pa.array(['x'])], names=['id', 'v']))


def _partial_update_keytime_fields():
    return [
        DataField(0, 'id', AtomicType('STRING')),
        DataField(1, 'seq', AtomicType('INT')),
        DataField(2, 'updates', MapType(True, AtomicType('STRING'), _value_row('STRING'))),
    ]


def _partial_update_keytime_options():
    return _options(**{
        'merge-engine': 'partial-update',
        'sequence.field': 'seq',
        'fields.seq.sequence-group': 'updates',
        'fields.updates.aggregate-function': 'merge_map_with_keytime',
        'fields.updates.ts-field': 'event_time',
    })


def test_partial_update_sequence_group_keytime_detected():
    table = _table(_partial_update_keytime_fields(), _partial_update_keytime_options())
    assert _table_uses_merge_map_keytime(table) is True
    assert _native_merge_map_keytime_timestamp_supported(table) is False
    assert _native_write_refuses_merge_map_keytime(table) is True


def test_partial_update_sequence_group_without_keytime_not_flagged():
    table = _table(
        _partial_update_keytime_fields(),
        _options(**{
            'merge-engine': 'partial-update',
            'sequence.field': 'seq',
            'fields.seq.sequence-group': 'updates',
        }))
    assert _table_uses_merge_map_keytime(table) is False
    assert _native_merge_map_keytime_timestamp_supported(table) is True
    assert _native_write_refuses_merge_map_keytime(table) is False


def test_sequence_group_ordering_field_not_effective_keytime():
    """Reviewer case: seq is ordering-only; updates overrides the default.

    ``fields.default-aggregate-function=merge_map_with_keytime`` must not
    classify ``seq`` (from ``fields.seq.sequence-group``) as keytime when
    ``fields.updates.aggregate-function`` is overridden.
    """
    table = _table(
        _partial_update_keytime_fields(),
        _options(**{
            'merge-engine': 'partial-update',
            'fields.seq.sequence-group': 'updates',
            'fields.default-aggregate-function': 'merge_map_with_keytime',
            'fields.updates.aggregate-function': 'last_non_null_value',
            'fields.updates.ts-field': 'event_time',
        }))
    assert _table_uses_merge_map_keytime(table) is False
    assert _native_merge_map_keytime_timestamp_supported(table) is True
    assert _native_write_refuses_merge_map_keytime(table) is False
    batch = pa.record_batch(
        [pa.array(['1']), pa.array([1], type=pa.int32()),
         pa.array([None], type=pa.map_(
             pa.string(), pa.struct([
                 ('value', pa.string()), ('event_time', pa.string())])))],
        names=['id', 'seq', 'updates'])
    _assert_stays_on_native(table, batch)


def test_partial_update_keytime_raises_before_opening_native():
    table = _table(_partial_update_keytime_fields(), _partial_update_keytime_options())
    read = _table_read(table)
    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        with pytest.raises(NotImplementedError, match='partial-update'):
            NativeTableRead(read).read_batches(
                [_split()], pa.schema([('id', pa.string())]))
    native.assert_not_called()


# -- real FileStore public-read fallback --------------------------------


def _create_keytime_table(tmp_path, name, ts_pa_type, ts_field='event_time'):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    ts_name = 'ts' if ts_field is None else ts_field
    value_type = pa.struct([
        pa.field('value', pa.string()),
        pa.field(ts_name, ts_pa_type),
    ])
    pa_schema = pa.schema([
        pa.field('id', pa.string(), nullable=False),
        pa.field('updates', pa.map_(pa.string(), value_type)),
    ])
    options = {
        'bucket': '1',
        'merge-engine': 'aggregation',
        'file.format': 'parquet',
        'fields.updates.aggregate-function': 'merge_map_with_keytime',
        'read.native.enabled': 'true',
        'write.native.enabled': 'false',
        'scan.native-plan.enabled': 'false',
    }
    if ts_field is not None:
        options['fields.updates.ts-field'] = ts_name
    schema = Schema.from_pyarrow_schema(
        pa_schema, primary_keys=['id'], options=options)
    catalog.create_table('default.{}'.format(name), schema, False)
    table = catalog.get_table('default.{}'.format(name))
    return table, pa_schema, ts_name


def _write_two_versions(table, pa_schema, ts_name, older, newer):
    for value, ts in (('old', older), ('new', newer)):
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            w.write_arrow(pa.Table.from_pylist([{
                'id': '1',
                'updates': [('k', {'value': value, ts_name: ts})],
            }], schema=pa_schema))
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()


def _public_read(table):
    rb = table.new_read_builder()
    splits = rb.new_scan().plan().splits()
    return rb.new_read().to_arrow(splits)


def test_catalog_string_utf16_order_falls_back_and_keeps_python_winner(tmp_path):
    table, pa_schema, ts_name = _create_keytime_table(
        tmp_path, 'string_utf16', pa.string())
    assert _native_merge_map_keytime_timestamp_supported(table) is False
    older = '\uE000'
    newer_under_utf8 = '\U00010000'
    _write_two_versions(table, pa_schema, ts_name, older, newer_under_utf8)

    read = table.new_read_builder().new_read()
    splits = table.new_read_builder().new_scan().plan().splits()
    schema = read._output_arrow_schema(splits)
    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert NativeTableRead(read).read_batches(splits, schema) is None
    native.assert_not_called()

    rows = _public_read(table).to_pylist()
    assert len(rows) == 1
    updates = dict(rows[0]['updates'])
    assert updates['k']['value'] == 'old'
    assert updates['k'][ts_name] == older


def test_catalog_ascii_string_still_prefers_lexically_newer(tmp_path):
    table, pa_schema, ts_name = _create_keytime_table(
        tmp_path, 'string_ascii', pa.string())
    assert _native_merge_map_keytime_timestamp_supported(table) is False
    _write_two_versions(table, pa_schema, ts_name, '100', '200')

    read = table.new_read_builder().new_read()
    splits = table.new_read_builder().new_scan().plan().splits()
    schema = read._output_arrow_schema(splits)
    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert NativeTableRead(read).read_batches(splits, schema) is None
    native.assert_not_called()

    rows = _public_read(table).to_pylist()
    assert len(rows) == 1
    updates = dict(rows[0]['updates'])
    assert updates['k']['value'] == 'new'
    assert updates['k'][ts_name] == '200'


def test_catalog_double_keytime_stays_eligible_and_keeps_newer(tmp_path):
    """DOUBLE keytime has no UTF-16 mismatch; Native remains eligible."""
    table, pa_schema, ts_name = _create_keytime_table(
        tmp_path, 'double_keytime', pa.float64())
    assert _table_uses_merge_map_keytime(table) is True
    assert _native_merge_map_keytime_timestamp_supported(table) is True
    _write_two_versions(table, pa_schema, ts_name, 9.5, 10.25)

    rows = _public_read(table).to_pylist()
    assert len(rows) == 1
    updates = dict(rows[0]['updates'])
    assert updates['k']['value'] == 'new'
    assert updates['k'][ts_name] == 10.25


# -- Native Writer refuses keytime; Python keeps UTF-16 winner ------------


def _create_keytime_table_native_write_requested(tmp_path, name):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    value_type = pa.struct([
        pa.field('value', pa.string()),
        pa.field('event_time', pa.string()),
    ])
    pa_schema = pa.schema([
        pa.field('id', pa.string(), nullable=False),
        pa.field('updates', pa.map_(pa.string(), value_type)),
    ])
    options = {
        'bucket': '1',
        'merge-engine': 'aggregation',
        'file.format': 'parquet',
        'fields.updates.aggregate-function': 'merge_map_with_keytime',
        'fields.updates.ts-field': 'event_time',
        'write.native.enabled': 'true',
        'read.native.enabled': 'true',
        'scan.native-plan.enabled': 'false',
        'commit.native.enabled': 'false',
    }
    schema = Schema.from_pyarrow_schema(
        pa_schema, primary_keys=['id'], options=options)
    catalog.create_table('default.{}'.format(name), schema, False)
    return catalog.get_table('default.{}'.format(name)), pa_schema


def test_native_writer_refuses_keytime_same_batch_keeps_utf16_first(tmp_path):
    """Same write_arrow batch: Native refused; Python keeps UTF-16 first.

    PMC scenario: one batch, same PK/map key, U+E000/first then
    U+10000/second. Native UTF-8 would persist second; gated Python
    Writer must keep first through commit and reopen.
    """
    table, pa_schema = _create_keytime_table_native_write_requested(
        tmp_path, 'writer_utf16_gate')
    assert _native_write_refuses_merge_map_keytime(table) is True

    older = '\uE000'
    newer_under_utf8 = '\U00010000'

    with ExitStack() as stack:
        stack.enter_context(patch(
            'pypaimon.execution.native.write.native_write.native_runtime_available',
            return_value=True))
        create_table = stack.enter_context(patch(
            'pypaimon.execution.native.write.native_write.create_native_write_table'))
        assert create_native_write(table, 'job') is None
        create_table.assert_not_called()

        wb = table.new_batch_write_builder()
        writer = wb.new_write()
        create_table.assert_not_called()
        assert not isinstance(writer, NativeTableWrite)
        assert isinstance(writer, BatchTableWrite)
        assert isinstance(writer._execution, PythonTableWrite)

        try:
            writer.write_arrow(pa.Table.from_pylist([
                {'id': '1', 'updates': [
                    ('k', {'value': 'first', 'event_time': older})]},
                {'id': '1', 'updates': [
                    ('k', {'value': 'second', 'event_time': newer_under_utf8})]},
            ], schema=pa_schema))
            commit = wb.new_commit()
            try:
                commit.commit(writer.prepare_commit())
            finally:
                commit.close()
        finally:
            writer.close()

        create_table.assert_not_called()

    reopened = CatalogFactory.create(
        {'warehouse': str(tmp_path / 'warehouse')}
    ).get_table('default.writer_utf16_gate')
    rows = _public_read(reopened).to_pylist()
    assert len(rows) == 1
    updates = dict(rows[0]['updates'])
    assert updates['k']['value'] == 'first'
    assert updates['k']['event_time'] == older


# -- Partial Update + sequence-group: raise before Native Reader ----------


def _create_partial_update_keytime_table(tmp_path, name):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    value_type = pa.struct([
        pa.field('value', pa.string()),
        pa.field('event_time', pa.string()),
    ])
    pa_schema = pa.schema([
        pa.field('id', pa.string(), nullable=False),
        pa.field('seq', pa.int32(), nullable=False),
        pa.field('updates', pa.map_(pa.string(), value_type)),
    ])
    options = {
        'bucket': '1',
        'merge-engine': 'partial-update',
        'file.format': 'parquet',
        'sequence.field': 'seq',
        'fields.seq.sequence-group': 'updates',
        'fields.updates.aggregate-function': 'merge_map_with_keytime',
        'fields.updates.ts-field': 'event_time',
        'read.native.enabled': 'true',
        'write.native.enabled': 'false',
        'scan.native-plan.enabled': 'false',
    }
    schema = Schema.from_pyarrow_schema(
        pa_schema, primary_keys=['id'], options=options)
    catalog.create_table('default.{}'.format(name), schema, False)
    return catalog.get_table('default.{}'.format(name)), pa_schema


def test_partial_update_java_shaped_table_raises_before_native_reader(tmp_path):
    """Real table matching Java Writer Partial Update + sequence-group.

    Python cannot legally write this merge config, so write-side
    ``check_supported`` is bypassed only to plant the same on-disk shape a
    Java Writer leaves (top-level seq 1/2, nested U+E000 then U+10000).
    Reading must raise before Native opens; silent UTF-8 ``second`` is
    forbidden.
    """
    table, pa_schema = _create_partial_update_keytime_table(
        tmp_path, 'pu_keytime_java_shape')
    assert _native_merge_map_keytime_timestamp_supported(table) is False

    older = '\uE000'
    newer_under_utf8 = '\U00010000'
    # Python refuses sequence-group / field aggregators on Partial Update
    # write; bypass only while planting files that mirror a Java Writer
    # leaving top-level seq 1/2 and nested U+E000 / U+10000 for Native
    # Reader merge.
    with patch(
            'pypaimon.execution.python.merge_engine_dispatch.'
            'partial_update_unsupported_options',
            return_value=set()):
        for seq, value, ts in (
                (1, 'first', older),
                (2, 'second', newer_under_utf8),
        ):
            wb = table.new_batch_write_builder()
            w = wb.new_write()
            c = wb.new_commit()
            try:
                w.write_arrow(pa.Table.from_pylist([{
                    'id': '1',
                    'seq': seq,
                    'updates': {
                        'k': {'value': value, 'event_time': ts}},
                }], schema=pa_schema))
                c.commit(w.prepare_commit())
            finally:
                w.close()
                c.close()

    reopened = CatalogFactory.create(
        {'warehouse': str(tmp_path / 'warehouse')}
    ).get_table('default.pu_keytime_java_shape')
    read = reopened.new_read_builder().new_read()
    splits = reopened.new_read_builder().new_scan().plan().splits()
    assert splits, 'expected persisted splits from planted Java-shaped writes'
    schema = read._output_arrow_schema(splits)

    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        with pytest.raises(NotImplementedError, match='partial-update'):
            NativeTableRead(read).read_batches(splits, schema)
    native.assert_not_called()

    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        with pytest.raises(NotImplementedError, match='partial-update'):
            _public_read(reopened)
    native.assert_not_called()


def test_string_keytime_projection_without_map_stays_on_native():
    """Unrelated projections must not be blocked by a STRING keytime map."""
    from pypaimon.read.read_type import reader_adapter

    fields = _keytime_fields(_value_row('STRING')) + [
        DataField(4, 'label', AtomicType('STRING'))]
    table = _table(fields, _keytime_options())
    assert _table_uses_merge_map_keytime(table) is True
    assert _native_merge_map_keytime_timestamp_supported(table) is False
    assert _native_merge_map_keytime_timestamp_supported(
        table, ['id', 'label']) is True

    read = _table_read(table)
    read.read_type = [field for field in fields if field.name != 'updates']
    read._adapter_read_type, _ = reader_adapter(read.read_type, table.fields)
    read._scan_read_type = read._adapter_read_type
    read._predicate_extra_fields = []
    read.predicate = None
    read._output_column_names = [field.name for field in read.read_type]
    batch = pa.record_batch(
        [pa.array(['1']), pa.array(['ok'])], names=['id', 'label'])
    with patch('pypaimon.execution.native.read.table_read.native_read',
               return_value=[batch]) as native:
        assert list(NativeTableRead(read).read_batches([_split()], batch.schema))
    native.assert_called_once()


def test_string_keytime_predicate_on_map_falls_back_even_if_not_projected():
    """WHERE on a STRING keytime map must keep the Native safety gate."""
    from pypaimon.common.predicate_builder import PredicateBuilder
    from pypaimon.execution.native.read.table_read import _accessed_field_names
    from pypaimon.read.read_type import reader_adapter

    fields = _keytime_fields(_value_row('STRING')) + [
        DataField(4, 'label', AtomicType('STRING'))]
    table = _table(fields, _keytime_options())
    projected = [field for field in fields if field.name != 'updates']
    read = _table_read(table)
    read.read_type = projected
    read._adapter_read_type, _ = reader_adapter(projected, table.fields)
    predicate = PredicateBuilder(table.fields).is_not_null('updates')
    read.predicate = predicate
    read._predicate_extra_fields = [field for field in fields if field.name == 'updates']
    read._scan_read_type = read._adapter_read_type + read._predicate_extra_fields
    read._output_column_names = [field.name for field in projected]

    assert 'updates' in _accessed_field_names(read)
    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert NativeTableRead(read).read_batches(
            [_split()], pa.schema([('id', pa.string()), ('label', pa.string())])) is None
    native.assert_not_called()


def test_string_keytime_variant_projection_without_map_stays_on_native():
    """VARIANT projection that omits STRING keytime MAP stays Native-eligible."""
    from pypaimon.execution.native.read.table_read import _accessed_field_names
    from pypaimon.read.read_type import OutputProjection, reader_adapter

    fields = _keytime_fields(_value_row('STRING')) + [
        DataField(4, 'payload', AtomicType('VARIANT'))]
    table = _table(fields, _keytime_options())
    projected = [field for field in fields if field.name != 'updates']
    read = _table_read(table)
    read.read_type = projected
    read._adapter_read_type, _ = reader_adapter(projected, table.fields)
    read.predicate = None
    read._predicate_extra_fields = []
    read._scan_read_type = read._adapter_read_type
    # Named VARIANT expression output still roots on payload, not updates.
    read.output_projection = OutputProjection(
        [('id', ['id']), ('score', ['payload', '0'])], True)
    read._output_column_names = ['id', 'score']
    accessed = _accessed_field_names(read)
    assert 'updates' not in accessed
    assert 'payload' in accessed
    assert _native_merge_map_keytime_timestamp_supported(table, accessed) is True
    # Native returns already-projected columns; skip local output rewrite.
    read.output_projection = None
    batch = pa.record_batch(
        [pa.array(['1']), pa.array([9.5], type=pa.float32())],
        names=['id', 'score'])
    with patch('pypaimon.execution.native.read.table_read.native_read',
               return_value=[batch]) as native:
        assert list(NativeTableRead(read).read_batches([_split()], batch.schema))
    native.assert_called_once()


def test_string_keytime_variant_projection_predicate_on_map_falls_back():
    """VARIANT projection + Predicate on STRING keytime MAP must gate Native."""
    from pypaimon.common.predicate_builder import PredicateBuilder
    from pypaimon.execution.native.read.table_read import _accessed_field_names
    from pypaimon.read.read_type import OutputProjection, reader_adapter

    fields = _keytime_fields(_value_row('STRING')) + [
        DataField(4, 'payload', AtomicType('VARIANT'))]
    table = _table(fields, _keytime_options())
    projected = [field for field in fields if field.name != 'updates']
    read = _table_read(table)
    read.read_type = projected
    read._adapter_read_type, _ = reader_adapter(projected, table.fields)
    read.predicate = PredicateBuilder(table.fields).is_not_null('updates')
    read._predicate_extra_fields = [
        field for field in fields if field.name == 'updates']
    read._scan_read_type = read._adapter_read_type + read._predicate_extra_fields
    read.output_projection = OutputProjection(
        [('id', ['id']), ('score', ['payload', '0'])], True)

    accessed = _accessed_field_names(read)
    assert 'updates' in accessed
    assert _native_merge_map_keytime_timestamp_supported(table, accessed) is False
    with patch('pypaimon.execution.native.read.table_read.native_read') as native:
        assert NativeTableRead(read).read_batches(
            [_split()],
            pa.schema([('id', pa.string()), ('score', pa.float32())])) is None
    native.assert_not_called()


# -- Reviewer MERGE source + VARIANT projection regressions -------------


def _native_merge_assignment_available():
    try:
        from pypaimon_rust.datafusion import MergeAssignment  # noqa: F401
        return True
    except ImportError:
        return False


def _native_read_arrow_accepts_parallelism():
    """Objective probe: installed Rust TableRead.read_arrow kwargs."""
    try:
        from pypaimon_rust.datafusion import TableRead
    except ImportError:
        return False
    text = getattr(TableRead.read_arrow, '__text_signature__', None)
    if text is not None:
        return 'parallelism' in text
    # Unknown signature: do not skip; let the real call decide.
    return True


def _persist_string_keytime_utf16_source(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    value_type = pa.struct([
        pa.field('value', pa.string()),
        pa.field('event_time', pa.string()),
    ])
    shared_schema = pa.schema([
        pa.field('id', pa.string(), nullable=False),
        pa.field('updates', pa.map_(pa.string(), value_type)),
    ])
    catalog.create_table('default.keytime_src', Schema.from_pyarrow_schema(
        shared_schema, primary_keys=['id'], options={
            'bucket': '1',
            'merge-engine': 'aggregation',
            'file.format': 'parquet',
            'fields.updates.aggregate-function': 'merge_map_with_keytime',
            'fields.updates.ts-field': 'event_time',
            'read.native.enabled': 'true',
            'write.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
        }), False)
    source = catalog.get_table('default.keytime_src')
    utf16_winner_ts = '\uE000'
    utf8_winner_ts = '\U00010000'
    for value, ts in (('first', utf16_winner_ts), ('second', utf8_winner_ts)):
        wb = source.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        try:
            w.write_arrow(pa.Table.from_pylist([{
                'id': '1',
                'updates': [('k', {'value': value, 'event_time': ts})],
            }], schema=shared_schema))
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()
    return catalog, shared_schema, utf16_winner_ts


def _persist_partial_update_keytime_merge_tables(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    value_type = pa.struct([
        pa.field('value', pa.string()),
        pa.field('event_time', pa.string()),
    ])
    shared_schema = pa.schema([
        pa.field('id', pa.string(), nullable=False),
        pa.field('seq', pa.int32()),
        pa.field('updates', pa.map_(pa.string(), value_type)),
    ])
    catalog.create_table('default.pu_src', Schema.from_pyarrow_schema(
        shared_schema, primary_keys=['id'], options={
            'bucket': '1',
            'merge-engine': 'partial-update',
            'file.format': 'parquet',
            'fields.seq.sequence-group': 'updates',
            'fields.updates.aggregate-function': 'merge_map_with_keytime',
            'fields.updates.ts-field': 'event_time',
            'read.native.enabled': 'false',
            'write.native.enabled': 'false',
        }), False)
    catalog.create_table('default.merge_dst', Schema.from_pyarrow_schema(
        pa.schema([
            pa.field('id', pa.string(), nullable=False),
            pa.field('updates', pa.map_(pa.string(), value_type)),
        ]), options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'deletion-vectors.enabled': 'true',
            'write.native.enabled': 'true',
            'read.native.enabled': 'true',
            'file.format': 'parquet',
        }), False)
    return (catalog.get_table('default.pu_src'),
            catalog.get_table('default.merge_dst'))


def test_native_merge_partial_update_keytime_source_refused(tmp_path):
    """Partial-update keytime sources must not enter Native MERGE."""
    from pypaimon.execution.native.write import native_merge_into as merge_mod
    from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col

    source, target = _persist_partial_update_keytime_merge_tables(tmp_path)
    assert _table_uses_merge_map_keytime(source) is True

    with patch.object(
            merge_mod, 'create_native_write_table'
    ) as create_table, patch.object(
            merge_mod, '_python_materialize_merge_source'
    ) as materialize:
        with pytest.raises(NotImplementedError, match='partial-update'):
            merge_mod.create_native_merge_into(
                target, source, ['id'],
                [WhenMatched.update({'updates': source_col('updates')})],
                [WhenNotMatched('*')], 'job', -1)
    materialize.assert_not_called()
    create_table.assert_not_called()


def test_native_merge_partial_update_refuses_before_native_target(tmp_path):
    """Refusal must happen before the Native target initializer runs."""
    from pypaimon.execution.native.write import native_merge_into as merge_mod
    from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col

    source, target = _persist_partial_update_keytime_merge_tables(tmp_path)
    with patch.object(merge_mod, '_native_row_id_table') as native_target, patch.object(
            merge_mod, 'create_native_write_table'
    ) as create_table, patch.object(
            merge_mod, '_python_materialize_merge_source'
    ) as materialize, patch(
            'pypaimon.execution.python.write.merge_into._build_tables',
            side_effect=AssertionError('Python MERGE must not run')
    ):
        with pytest.raises(NotImplementedError, match='partial-update'):
            merge_mod.create_native_merge_into(
                target, source, ['id'],
                [WhenMatched.update({'updates': source_col('updates')})],
                [WhenNotMatched('*')], 'job', -1)
    native_target.assert_not_called()
    create_table.assert_not_called()
    materialize.assert_not_called()


def test_string_keytime_merge_materialize_uses_query_auth(tmp_path):
    """STRING keytime Arrow materialize goes through the Python query-auth read."""
    from pypaimon.catalog.table_query_auth import TableQueryAuthResult
    from pypaimon.execution.native.write import native_merge_into as merge_mod
    from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col

    catalog, shared_schema, utf16_winner_ts = _persist_string_keytime_utf16_source(tmp_path)
    catalog.create_table('default.merge_dst', Schema.from_pyarrow_schema(
        shared_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'deletion-vectors.enabled': 'true',
            'write.native.enabled': 'true',
            'read.native.enabled': 'true',
            'file.format': 'parquet',
        }), False)
    target = catalog.get_table('default.merge_dst')
    denied = catalog.get_table('default.keytime_src').copy({
        'query-auth.enabled': 'true',
    })
    with patch.object(merge_mod, 'encode_merge_clause') as encode, patch.object(
            merge_mod, 'create_native_write_table'
    ) as create_table:
        with pytest.raises(NotImplementedError, match='auth_table_query'):
            merge_mod.create_native_merge_into(
                target, denied, ['id'],
                [WhenMatched.update({'updates': source_col('updates')})],
                [WhenNotMatched('*')], 'job', -1)
    encode.assert_not_called()
    create_table.assert_not_called()

    seen = []

    def authorize(select):
        seen.append(list(select))
        return TableQueryAuthResult(None, None)

    allowed = catalog.get_table('default.keytime_src').copy({
        'query-auth.enabled': 'true',
    })
    allowed.catalog_environment.table_query_auth = (
        lambda options, identifier: authorize)
    with patch.object(
            merge_mod, 'encode_merge_clause',
            side_effect=lambda *args, **kwargs: object()
    ), patch.object(
            merge_mod, 'create_native_write_table',
            wraps=merge_mod.create_native_write_table
    ) as create_table:
        op = merge_mod.create_native_merge_into(
            target, allowed, ['id'],
            [WhenMatched.update({'updates': source_col('updates')})],
            [WhenNotMatched('*')], 'job', -1)
    assert seen, 'query auth was not consulted during source materialize'
    assert 'updates' in seen[0]
    create_table.assert_not_called()
    assert op is not None
    assert isinstance(op.source, pa.Table)
    row = dict(op.source.to_pylist()[0]['updates'])['k']
    assert row['value'] == 'first'
    assert row['event_time'] == utf16_winner_ts


def test_string_keytime_merge_query_auth_permission_denial_stops_merge(tmp_path):
    """A catalog permission denial must stop before Native MERGE is built."""
    from pypaimon.catalog.catalog_exception import TableNoPermissionException
    from pypaimon.execution.native.write import native_merge_into as merge_mod
    from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col

    catalog, shared_schema, _ = _persist_string_keytime_utf16_source(tmp_path)
    catalog.create_table('default.merge_dst', Schema.from_pyarrow_schema(
        shared_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'deletion-vectors.enabled': 'true',
            'write.native.enabled': 'true',
            'read.native.enabled': 'true',
            'file.format': 'parquet',
        }), False)
    target = catalog.get_table('default.merge_dst')
    source = catalog.get_table('default.keytime_src').copy({
        'query-auth.enabled': 'true',
    })

    def authorize(select):
        raise TableNoPermissionException(source.identifier)

    source.catalog_environment.table_query_auth = (
        lambda options, identifier: authorize)
    with patch.object(merge_mod, 'encode_merge_clause') as encode, patch.object(
            merge_mod, 'create_native_write_table'
    ) as native_source, patch.object(
            merge_mod.NativeTableMergeInto, '__init__',
            side_effect=AssertionError('Native MERGE was constructed')
    ):
        with pytest.raises(TableNoPermissionException, match='No permission'):
            merge_mod.create_native_merge_into(
                target, source, ['id'],
                [WhenMatched.update({'updates': source_col('updates')})],
                [WhenNotMatched('*')], 'job', -1)
    encode.assert_not_called()
    native_source.assert_not_called()


def test_keytime_guard_parses_aggregators_only_for_partial_update():
    """Non-table and non-partial-update sources skip keytime field parsing."""
    from pypaimon.common.options.core_options import MergeEngine
    from pypaimon.common import keytime_merge_guard as guard

    with patch.object(guard, 'table_uses_merge_map_keytime') as parse:
        assert guard.refuse_partial_update_keytime_merge_source(object()) is None
        parse.assert_not_called()

        aggregate = Mock()
        aggregate.new_read_builder = Mock()
        aggregate.snapshot_manager = Mock()
        aggregate.options.merge_engine.return_value = MergeEngine.AGGREGATE
        assert guard.refuse_partial_update_keytime_merge_source(aggregate) is None
        parse.assert_not_called()

        plain = Mock()
        plain.new_read_builder = Mock()
        plain.snapshot_manager = Mock()
        plain.options.merge_engine.return_value = MergeEngine.PARTIAL_UPDATE
        parse.return_value = False
        assert guard.refuse_partial_update_keytime_merge_source(plain) is None
        parse.assert_called_once_with(plain)

        parse.return_value = True
        with pytest.raises(NotImplementedError, match='partial-update'):
            guard.refuse_partial_update_keytime_merge_source(plain)


def test_public_self_merge_skips_keytime_guard_and_keeps_native_gate(tmp_path):
    """Self-merge uses merge_inputs identity and still runs Native target checks."""
    from pypaimon.common.keytime_merge_guard import (
        refuse_partial_update_keytime_merge_source,
    )
    from pypaimon.execution.native.write import native_merge_into as merge_mod
    from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched
    from pypaimon.write.backend_dispatch import (
        create_python_merge_into_operation, prepare_native_merge_into,
    )
    from pypaimon.write.update_dispatch import merge_into as public_merge_into

    source, _ = _persist_partial_update_keytime_merge_tables(tmp_path)
    table = source.copy({'write.native.enabled': 'true'})
    seen = {}

    def spy_guard(candidate):
        seen['guard'] = True
        return refuse_partial_update_keytime_merge_source(candidate)

    def spy_native(*args, **kwargs):
        seen['native'] = prepare_native_merge_into(*args, **kwargs)
        return seen['native']

    def spy_python(*args, **kwargs):
        seen['python'] = True
        return create_python_merge_into_operation(*args, **kwargs)

    with patch(
            'pypaimon.common.keytime_merge_guard.refuse_partial_update_keytime_merge_source',
            side_effect=spy_guard
    ), patch(
            'pypaimon.write.backend_dispatch.prepare_native_merge_into',
            side_effect=spy_native
    ), patch(
            'pypaimon.write.update_dispatch.create_python_merge_into_operation',
            side_effect=spy_python
    ), patch.object(
            merge_mod, '_native_row_id_table',
            wraps=merge_mod._native_row_id_table
    ) as native_target:
        with pytest.raises(Exception) as raised:
            public_merge_into(
                table, table, on=['_ROW_ID'],
                when_matched=[WhenMatched.delete()],
                when_not_matched=[WhenNotMatched('*')],
                commit_user='job')
    assert 'guard' not in seen
    assert seen['native'] is None
    assert seen['python'] is True
    native_target.assert_called()
    assert 'merge_map_with_keytime with merge-engine' not in str(raised.value)


def test_public_merge_into_refuses_partial_update_keytime_source(tmp_path):
    """Public merge_into must not silently fall through to Python MERGE."""
    from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col

    source, target = _persist_partial_update_keytime_merge_tables(tmp_path)

    with patch(
            'pypaimon.write.update_dispatch.create_python_merge_into_operation'
    ) as python_merge, patch(
            'pypaimon.write.backend_dispatch.prepare_native_merge_into',
            return_value=None
    ) as native_merge:
        with pytest.raises(NotImplementedError, match='partial-update'):
            target.new_batch_write_builder().new_update().merge_into(
                source, on=['id'],
                when_matched=[WhenMatched.update(
                    {'updates': source_col('updates')})],
                when_not_matched=[WhenNotMatched('*')])
        # Guard runs before Native/Python selection.
        native_merge.assert_not_called()
        python_merge.assert_not_called()


def test_native_merge_string_keytime_materialize_keeps_source_gates(tmp_path):
    """STRING keytime materialize still applies shared Native source gates."""
    from pypaimon.execution.native.write import native_merge_into as merge_mod
    from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col

    catalog, shared_schema, _ = _persist_string_keytime_utf16_source(tmp_path)
    catalog.create_table('default.merge_dst', Schema.from_pyarrow_schema(
        shared_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'deletion-vectors.enabled': 'true',
            'write.native.enabled': 'true',
            'read.native.enabled': 'true',
            'file.format': 'parquet',
        }), False)
    target = catalog.get_table('default.merge_dst')
    # Shared gate: invalid scan.mode companion options must fail before
    # Python materialize (same as the Native source path).
    source = catalog.get_table('default.keytime_src').copy({
        'scan.mode': 'from-snapshot',
    })

    with patch.object(
            merge_mod, '_python_materialize_merge_source'
    ) as materialize:
        with pytest.raises(ValueError, match='scan.mode'):
            merge_mod.create_native_merge_into(
                target, source, ['id'],
                [WhenMatched.update({'updates': source_col('updates')})],
                [WhenNotMatched('*')], 'job', -1)
    materialize.assert_not_called()


def test_native_merge_string_keytime_source_materializes_python_arrow(tmp_path):
    """Unit: STRING keytime MERGE sources materialize via Python, never Rust table."""
    from pypaimon.execution.native.write import native_merge_into as merge_mod
    from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col

    catalog, shared_schema, utf16_winner_ts = _persist_string_keytime_utf16_source(tmp_path)
    source = catalog.get_table('default.keytime_src')
    assert dict(_public_read(source).to_pylist()[0]['updates'])['k']['value'] == 'first'

    materialized = merge_mod._python_materialize_merge_source(
        source, ['id', 'updates'])
    assert dict(materialized.to_pylist()[0]['updates'])['k']['value'] == 'first'
    assert dict(materialized.to_pylist()[0]['updates'])['k']['event_time'] == utf16_winner_ts

    catalog.create_table('default.merge_dst', Schema.from_pyarrow_schema(
        shared_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'deletion-vectors.enabled': 'true',
            'write.native.enabled': 'true',
            'read.native.enabled': 'true',
            'file.format': 'parquet',
        }), False)
    target = catalog.get_table('default.merge_dst')
    merge_source = catalog.get_table('default.keytime_src')

    with patch.object(
            merge_mod, 'create_native_write_table',
            wraps=merge_mod.create_native_write_table
    ) as create_table, patch.object(
            merge_mod, '_python_materialize_merge_source',
            wraps=merge_mod._python_materialize_merge_source
    ) as materialize, patch.object(
            merge_mod, 'encode_merge_clause',
            side_effect=lambda *args, **kwargs: object()
    ):
        op = merge_mod.create_native_merge_into(
            target, merge_source, ['id'],
            [WhenMatched.update({'updates': source_col('updates')})],
            [WhenNotMatched('*')], 'job', -1)
        assert materialize.called
        create_table.assert_not_called()
        assert op is not None
        assert isinstance(op.source, pa.Table)
        assert list(op.source.column_names) == ['id', 'updates']
        assert dict(op.source.to_pylist()[0]['updates'])['k']['value'] == 'first'


@pytest.mark.skipif(
    not _native_merge_assignment_available(),
    reason='Installed pypaimon-rust lacks MergeAssignment (Native MERGE clauses)')
def test_native_merge_string_keytime_source_commits_utf16_winner(tmp_path):
    """Native MERGE integration: materialize source, Native commit, reopen target."""
    from pypaimon.execution.native.write import native_merge_into as merge_mod
    from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col

    catalog, shared_schema, utf16_winner_ts = _persist_string_keytime_utf16_source(tmp_path)
    catalog.create_table('default.merge_dst', Schema.from_pyarrow_schema(
        shared_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'deletion-vectors.enabled': 'true',
            'write.native.enabled': 'true',
            'read.native.enabled': 'true',
            'file.format': 'parquet',
        }), False)
    target = catalog.get_table('default.merge_dst')
    merge_source = catalog.get_table('default.keytime_src')

    with patch.object(
            merge_mod, '_python_materialize_merge_source',
            wraps=merge_mod._python_materialize_merge_source
    ) as materialize, patch(
            'pypaimon.execution.python.write.merge_into._build_tables',
            side_effect=AssertionError('Python MERGE must not run')
    ):
        builder = target.new_batch_write_builder()
        messages = builder.new_update().merge_into(
            merge_source, on=['id'],
            when_matched=[WhenMatched.update({'updates': source_col('updates')})],
            when_not_matched=[WhenNotMatched('*')])
        assert materialize.called
        builder.new_commit().commit(messages)

    rows = _public_read(catalog.get_table('default.merge_dst')).to_pylist()
    assert len(rows) == 1
    assert dict(rows[0]['updates'])['k']['value'] == 'first'
    assert dict(rows[0]['updates'])['k']['event_time'] == utf16_winner_ts


def test_bigint_keytime_variant_projection_gate_allows_native():
    """VARIANT projection omitting BIGINT keytime maps stays Native-eligible."""
    from pypaimon.common.predicate_builder import PredicateBuilder
    from pypaimon.execution.native.read.table_read import _accessed_field_names
    from pypaimon.read.read_type import reader_adapter

    value_row = RowType(True, [
        DataField(2, 'value', AtomicType('STRING')),
        DataField(3, 'event_time', AtomicType('BIGINT')),
    ])
    fields = [
        DataField(0, 'id', AtomicType('STRING')),
        DataField(1, 'updates', MapType(True, AtomicType('STRING'), value_row)),
        DataField(4, 'payload', AtomicType('VARIANT')),
    ]
    table = _table(fields, _keytime_options())
    assert _table_uses_merge_map_keytime(table) is True
    assert _native_merge_map_keytime_timestamp_supported(table) is True
    assert _native_merge_map_keytime_timestamp_supported(
        table, ['id', 'payload']) is True

    projected = [field for field in fields if field.name != 'updates']
    read = _table_read(table)
    read.read_type = projected
    read._adapter_read_type, _ = reader_adapter(projected, table.fields)
    read.predicate = None
    read._predicate_extra_fields = []
    read._scan_read_type = read._adapter_read_type
    assert 'updates' not in _accessed_field_names(read)

    read.predicate = PredicateBuilder(table.fields).is_not_null('updates')
    read._predicate_extra_fields = [field for field in fields if field.name == 'updates']
    read._scan_read_type = read._adapter_read_type + read._predicate_extra_fields
    assert 'updates' in _accessed_field_names(read)


@pytest.mark.skipif(
    not _native_read_arrow_accepts_parallelism(),
    reason='Installed pypaimon-rust TableRead.read_arrow lacks parallelism=')
def test_bigint_keytime_variant_projection_stays_on_native(tmp_path):
    """BIGINT keytime + VARIANT: Native variant_get returns latest score 9.5."""
    from pypaimon.data.generic_variant import GenericVariant

    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    value_type = pa.struct([
        pa.field('value', pa.string()),
        pa.field('event_time', pa.int64()),
    ])
    payload_type = GenericVariant.to_arrow_array([]).type
    pa_schema = pa.schema([
        pa.field('id', pa.string(), nullable=False),
        pa.field('updates', pa.map_(pa.string(), value_type)),
        pa.field('payload', payload_type),
    ])
    catalog.create_table('default.variant_keytime', Schema.from_pyarrow_schema(
        pa_schema, primary_keys=['id'], options={
            'bucket': '1',
            'merge-engine': 'aggregation',
            'file.format': 'parquet',
            'fields.updates.aggregate-function': 'merge_map_with_keytime',
            'fields.updates.ts-field': 'event_time',
            'fields.payload.aggregate-function': 'last_non_null_value',
            'read.native.enabled': 'true',
            'write.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
        }), False)
    table = catalog.get_table('default.variant_keytime')
    assert _native_merge_map_keytime_timestamp_supported(
        table, ['id', 'payload']) is True

    for score, ts in ((1.0, 1), (9.5, 2)):
        wb = table.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        try:
            w.write_arrow(pa.table({
                'id': pa.array(['1'], type=pa.string()),
                'updates': pa.array(
                    [[('k', {'value': str(score), 'event_time': ts})]],
                    type=pa_schema.field('updates').type),
                'payload': GenericVariant.to_arrow_array([
                    GenericVariant.from_python({'score': score})]),
            }, schema=pa_schema))
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()

    reopened = CatalogFactory.create(
        {'warehouse': str(tmp_path / 'warehouse')}
    ).get_table('default.variant_keytime')
    builder = reopened.new_read_builder().with_projection({
        'id': 'id',
        'score': "variant_get(payload, '$.score', 'float')",
    })
    with patch(
            'pypaimon.execution.python.read.table_read.PythonTableRead._create_split_read',
            side_effect=AssertionError('Native read fell back')):
        rows = builder.new_read().to_arrow(
            builder.new_scan().plan().splits()).to_pylist()
    assert rows == [{'id': '1', 'score': 9.5}]
