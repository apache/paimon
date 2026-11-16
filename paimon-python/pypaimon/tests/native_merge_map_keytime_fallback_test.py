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
    'BOOLEAN', 'BINARY(4)', 'VARBINARY(8)', 'BYTES',
    'TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT',
    'FLOAT', 'DOUBLE',
    'DATE', 'TIME', 'DECIMAL(10, 2)',
    'TIMESTAMP(9)', 'TIMESTAMP_LTZ(9)', 'TIMESTAMP',
])
def test_keytime_falls_back_before_opening_native(ts_type):
    _assert_falls_back_before_native(_table(
        _keytime_fields(_value_row(ts_type)),
        _keytime_options()))


def test_default_ts_field_also_falls_back():
    double_last = _table(
        _keytime_fields(_value_row('DOUBLE')),
        _keytime_options(ts_field=None))
    assert _native_merge_map_keytime_timestamp_supported(double_last) is False

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


def test_catalog_double_falls_back_before_native(tmp_path):
    table, pa_schema, ts_name = _create_keytime_table(
        tmp_path, 'double_fallback', pa.float64())
    assert _native_merge_map_keytime_timestamp_supported(table) is False
    _write_two_versions(table, pa_schema, ts_name, 9.5, 10.25)

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
    assert updates['k']['event_time'] == 10.25


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

    with patch(
            'pypaimon.execution.native.write.native_write.native_runtime_available',
            return_value=True), \
            patch(
                'pypaimon.execution.native.write.native_write.create_native_write_table'
            ) as create_table:
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
