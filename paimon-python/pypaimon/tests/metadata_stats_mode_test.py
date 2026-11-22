# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations
# under the License.

import math
import struct
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python.write.table_update_by_row_id import _RowIdUpdateFileWriter
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.data_types import AtomicType, DataField, VectorType
from pypaimon.table.row.generic_row import GenericRowDeserializer


pytestmark = pytest.mark.python_write


def _write(tmp_path, data, options, primary_keys=None, write_columns=None, fields=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', False)
    name = 'db.stats'
    options = {'file.format': 'parquet', 'write.native.enabled': 'false',
               'commit.native.enabled': 'false', **options}
    if fields is None:
        schema = Schema.from_pyarrow_schema(data.schema, primary_keys=primary_keys or [], options=options)
    else:
        schema = Schema(fields, primary_keys=primary_keys or [], options=options)
    catalog.create_table(name, schema, False)
    table = catalog.get_table(name)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        if write_columns is not None:
            writer.with_write_type(write_columns)
            data = data.select(write_columns)
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table


def _files(table, changelog=False):
    snapshot = table.snapshot_manager().get_latest_snapshot()
    lists = ManifestListManager(table)
    manifests = lists.read_changelog(snapshot) if changelog else lists.read_delta(snapshot)
    manager = ManifestFileManager(table)
    return [entry.file for manifest in manifests
            for entry in manager.read(manifest.file_name, drop_stats=False)
            if entry.kind == 0]


def _bounds(stats):
    return tuple(GenericRowDeserializer.from_bytes(row.data, row.fields).values
                 for row in (stats.min_values, stats.max_values))


@pytest.mark.parametrize('typ', [pa.float32(), pa.float64()], ids=['float', 'double'])
@pytest.mark.parametrize('values, expected_min, expected_max', [
    pytest.param([5.0, math.nan], 5.0, math.nan, id='nan-last'),
    pytest.param([math.nan, 5.0], 5.0, math.nan, id='nan-first'),
    pytest.param([-0.0, +0.0], -0.0, +0.0, id='negative-zero-first'),
    pytest.param([+0.0, -0.0], -0.0, +0.0, id='positive-zero-first'),
    pytest.param([-0.0, -0.0], -0.0, -0.0, id='negative-zero-only'),
    pytest.param([+0.0, +0.0], +0.0, +0.0, id='positive-zero-only'),
    pytest.param([math.inf, None, -math.inf], -math.inf, math.inf, id='infinities'),
    pytest.param([math.inf, math.inf], math.inf, math.inf, id='positive-infinity-only'),
    pytest.param([-math.inf, -math.inf], -math.inf, -math.inf, id='negative-infinity-only'),
    pytest.param([None, math.nan, math.inf], math.inf, math.nan, id='nan-and-infinity'),
    pytest.param([5.0, None, -2.0, None], -2.0, 5.0, id='mixed-numbers-and-nulls'),
    pytest.param([math.nan, -math.nan], math.nan, math.nan, id='all-nan'),
    pytest.param([None, -math.nan, None], math.nan, math.nan, id='null-and-nan'),
    pytest.param([None, None], None, None, id='all-null'),
])
def test_manifest_list_floating_partition_bounds(tmp_path, typ, values, expected_min, expected_max):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', False)
    schema = pa.schema([('id', pa.int32()), ('p', typ), ('seq', pa.int32())])
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['p', 'seq'],
        options={'bucket': '-1', 'write.native.enabled': 'false', 'commit.native.enabled': 'false'}), False)
    table = catalog.get_table('db.t')
    builder = table.new_batch_write_builder()
    messages = []
    # A distinct partition per entry keeps equal zeros and repeated NULLs in the same commit.
    for i, value in enumerate(values):
        writer = builder.new_write()
        try:
            writer.write_arrow(pa.table({'id': [i], 'p': [value], 'seq': [i]}, schema=schema))
            messages.extend(writer.prepare_commit())
        finally:
            writer.close()
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()

    manifest, = ManifestListManager(table).read_delta(table.snapshot_manager().get_latest_snapshot())
    assert manifest.num_added_files == len(values)
    entries = ManifestFileManager(table).read(manifest.file_name)
    assert [entry.partition.get_field(1) for entry in entries] == list(range(len(values)))
    stats = manifest.partition_stats
    minimum, maximum = _bounds(stats)
    assert stats.null_counts == [values.count(None), 0]
    assert (minimum[1], maximum[1]) == (0, len(values) - 1)
    encoding = '>f' if typ == pa.float32() else '>d'
    for actual, expected in ((minimum[0], expected_min), (maximum[0], expected_max)):
        if expected is None:
            assert actual is None
        elif math.isnan(expected):
            assert math.isnan(actual)
        else:
            # Comparing the IEEE bits distinguishes -0.0 from +0.0.
            assert struct.pack(encoding, actual) == struct.pack(encoding, expected)


@pytest.mark.parametrize('mode', [None, 'truncate(16)', 'counts', 'full', 'none'])
def test_append_manifest_stats_modes(tmp_path, mode):
    table = _write(tmp_path, pa.table({
        'id': [3, 1, 2],
        'name': ['alpha-' + 'x' * 12, 'zeta-' + 'y' * 13, None],
    }), {} if mode is None else {'metadata.stats-mode': mode})
    file, = _files(table)
    expected = {
        'truncate(16)': ([1, 'alpha-' + 'x' * 10], [3, 'zeta-' + 'y' * 10 + 'z']),
        'counts': ([None, None], [None, None]),
        'full': ([1, 'alpha-' + 'x' * 12], [3, 'zeta-' + 'y' * 13]),
        'none': ([], []),
    }
    assert _bounds(file.value_stats) == expected[mode or 'truncate(16)']
    assert file.value_stats.null_counts == ([] if mode == 'none' else [0, 1])
    assert file.value_stats_cols == ([] if mode == 'none' else None)


@pytest.mark.parametrize('dense', [True, False])
def test_column_modes_and_truncated_bounds(tmp_path, dense):
    table = _write(tmp_path, pa.table({
        'text': ['\ud7ffsuffix', None],
        'bytes': [b'\x12\xff\x10', b'\x12\xff\x20'],
        'overflow': [b'\xff\x00', None],
        'omitted': [1, 2],
    }), {
        'metadata.stats-mode': 'truncate(1)',
        'metadata.stats-keep-first-n-columns': '1',
        'metadata.stats-dense-store': str(dense).lower(),
        'fields.bytes.stats-mode': 'TRUNCATE(2)',
        'fields.overflow.stats-mode': 'truncate(1)',
    })
    file, = _files(table)
    assert file.value_stats_cols == (['text', 'bytes', 'overflow'] if dense else None)
    tail = [] if dense else [None]
    assert _bounds(file.value_stats) == (
        ['\ud7ff', b'\x12\xff', None] + tail, ['\ue000', b'\x13', None] + tail)
    assert file.value_stats.null_counts == [1, 0, 1] + tail


def test_primary_key_and_changelog_stats_modes(tmp_path):
    table = _write(tmp_path, pa.table({'id': ['aaab', 'aaaz'], 'name': ['alpha', None]}), {
        'bucket': '1', 'changelog-producer': 'input',
        'metadata.stats-mode': 'none', 'metadata.stats-mode.per.level': '0:truncate(3)',
        'changelog-file.stats-mode': 'counts',
    }, primary_keys=['id'])
    file, = _files(table)
    changes, = _files(table, changelog=True)
    assert _bounds(file.key_stats) == (['aaab'], ['aaaz'])
    assert _bounds(file.value_stats) == (['aaa', 'alp'], ['aab', 'alq'])
    assert _bounds(changes.value_stats) == ([None, None], [None, None])
    assert changes.value_stats.null_counts == [0, 1]


def test_partial_row_update_publishes_stats_for_its_written_columns(tmp_path, monkeypatch):
    monkeypatch.setattr(_RowIdUpdateFileWriter, '_ROW_GROUP_MAX_ROWS', 1)
    table = _write(tmp_path, pa.table({
        'id': [1, 2, 3], 'name': ['before'] * 3, 'zero': [5.] * 3, 'mixed': [10.] * 3,
    }), {
        'bucket': '-1', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'metadata.stats-mode': 'truncate(3)', 'fields.name.stats-mode': 'counts',
    })
    original, = _files(table)
    first = original.first_row_id
    builder = table.new_batch_write_builder()
    commit = builder.new_commit()
    try:
        update = builder.new_update().new_update_by_row_id()
        messages = update.update_columns(pa.table({
            '_ROW_ID': [first, first + 1, first + 2], 'name': ['after', None, 'updated'],
            'zero': [-0., 0., -0.], 'mixed': [1., float('nan'), 2.],
        }), ['name', 'zero', 'mixed'])
        commit.commit(messages)
    finally:
        commit.close()
    file, = _files(table)
    assert file.write_cols == ['name', 'zero', 'mixed']
    assert file.value_stats_cols is None
    minimum, maximum = _bounds(file.value_stats)
    assert minimum == [None, 0., None]
    assert maximum == [None, 0., None]
    assert math.copysign(1., minimum[1]) == -1.
    assert math.copysign(1., maximum[1]) == 1.
    assert file.value_stats.null_counts == [1, 0, 0]


@pytest.fixture
def two_rows_per_parquet_group(monkeypatch):
    write_parquet = LocalFileIO.write_parquet

    def two_rows_per_group(self, path, data, **kwargs):
        return write_parquet(self, path, data, row_group_size=2, **kwargs)

    monkeypatch.setattr(LocalFileIO, 'write_parquet', two_rows_per_group)


def test_parquet_writer_metadata_supplies_manifest_bounds(tmp_path, monkeypatch, two_rows_per_parquet_group):
    def unexpected_call(*args, **kwargs):
        pytest.fail('Manifest stats should reuse the metadata from the Parquet writer')

    monkeypatch.setattr(pa.compute, 'min_max', unexpected_call)
    monkeypatch.setattr(pq, 'read_metadata', unexpected_call)
    table = _write(tmp_path, pa.table({
        'id': [3, 2, 9, 1], 'name': [None, None, 'omega', 'alpha'],
        'score': [3.0, 2.5, 8.0, None],
    }), {'metadata.stats-mode': 'truncate(3)'})
    file, = _files(table)
    assert _bounds(file.value_stats) == ([1, 'alp', 2.5], [9, 'omf', 8.0])
    assert file.value_stats.null_counts == [0, 2, 1]


def test_parquet_footer_falls_back_to_arrow_for_incomplete_or_ambiguous_bounds(tmp_path, two_rows_per_parquet_group):
    long_value = 'z' * 5000
    table = _write(tmp_path, pa.table({
        'text': ['alpha', 'beta', long_value, 'gamma'],
        'float_with_nan': [float('nan'), 2.0, 1.0, 4.0],
        'positive_zero': [0.0, None, 0.0, 0.0],
        'a.b': [5, 9, 6, 7],
        'a': [{'b': 40}, {'b': 50}, {'b': 60}, {'b': 70}],
    }), {'metadata.stats-mode': 'full'})
    file, = _files(table)
    minimum, maximum = _bounds(file.value_stats)
    assert minimum == ['alpha', None, 0.0, 5, None]
    assert maximum == [long_value, None, 0.0, 9, None]
    assert math.copysign(1.0, minimum[2]) == 1.0
    assert file.value_stats.null_counts == [0, 0, 1, 0, 0]


@pytest.mark.parametrize('level_options', [
    {'metadata.stats-mode.per.level.0': 'counts'},
    {'metadata.stats-mode.per.level': '0:"counts",1:full',
     'metadata.stats-mode.per.level.0': 'none'},
    {'metadata.stats-mode.per.level': "'0:''counts'''"},
])
def test_primary_key_value_stats_accept_java_level_map_options(tmp_path, level_options):
    table = _write(tmp_path, pa.table({'id': [1, 2], 'v': ['alpha', None]}), {
        'bucket': '1', 'metadata.stats-mode': 'full', **level_options,
    }, primary_keys=['id'])
    file, = _files(table)
    assert _bounds(file.value_stats) == ([None, None], [None, None])
    assert file.value_stats.null_counts == [0, 1]


def test_composite_file_stats_follow_write_column_order(tmp_path):
    table = _write(tmp_path, pa.table({
        'id': pa.array([1, 2], type=pa.int32()), 'name': ['a', 'b'],
        'embedding': pa.array([[1.0] * 4, [2.0] * 4], type=pa.list_(pa.float32(), 4)),
    }), {
        'bucket': '-1', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'vector.file.format': 'parquet', 'metadata.stats-keep-first-n-columns': '1',
    }, write_columns=['name', 'id'])
    file, = _files(table)
    assert file.write_cols == ['name', 'id']
    assert file.value_stats_cols == ['name']
    assert _bounds(file.value_stats) == (['a'], ['b'])


@pytest.mark.parametrize('data_type, value', [
    (pa.date32(), -719200),
    (pa.timestamp('ms'), 253402300800000),
    pytest.param(pa.timestamp('ns'), -(1 << 63), marks=pytest.mark.skipif(
        int(pa.__version__.split('.')[0]) < 14, reason='Parquet ns storage requires PyArrow >= 14')),
])
def test_manifest_stats_do_not_block_valid_arrow_temporal_values(tmp_path, data_type, value):
    table = _write(tmp_path, pa.table({'v': pa.array([value, None], type=data_type)}), {
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
    })
    file, = _files(table)
    assert _bounds(file.value_stats) == ([None], [None])
    assert file.value_stats.null_counts == [1]
    builder = table.new_read_builder()
    data = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    storage_type = pa.int32() if pa.types.is_date32(data_type) else pa.int64()
    assert data.column('v').cast(storage_type).to_pylist() == [value, None]


def test_row_id_update_drops_bounds_when_a_row_group_cannot_supply_them(tmp_path, monkeypatch):
    monkeypatch.setattr(_RowIdUpdateFileWriter, '_ROW_GROUP_MAX_ROWS', 1)
    data_type = pa.timestamp('ms')
    table = _write(tmp_path, pa.table({'v': pa.array([0, 0], type=data_type)}), {
        'bucket': '-1', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
    })
    first = _files(table)[0].first_row_id
    builder = table.new_batch_write_builder()
    commit = builder.new_commit()
    try:
        update = builder.new_update().new_update_by_row_id()
        messages = update.update_columns(pa.table({
            '_ROW_ID': [first, first + 1],
            'v': pa.array([253402300800000, 0], type=data_type),
        }), ['v'])
        commit.commit(messages)
    finally:
        commit.close()
    file, = _files(table)
    assert _bounds(file.value_stats) == ([None], [None])
    assert file.value_stats.null_counts == [0]


@pytest.mark.parametrize('dedicated_vector', [False, True])
def test_row_file_stats_do_not_prune_written_submicrosecond_timestamps(tmp_path, dedicated_vector):
    timestamp_ns = 1735689600000000000
    data = pa.table({
        'v': pa.array([timestamp_ns + 999, None], type=pa.timestamp('ns')),
    })
    options = {'file.format': 'row', 'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'}
    fields = None
    if dedicated_vector:
        data = data.append_column('embedding', pa.array([[1.0] * 4, [2.0] * 4], type=pa.list_(pa.float32(), 4)))
        fields = [DataField(0, 'v', AtomicType('TIMESTAMP(09)')),
                  DataField(1, 'embedding', VectorType(True, AtomicType('FLOAT'), 4))]
        options.update({'bucket': '-1', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                        'vector.file.format': 'parquet'})
    table = _write(tmp_path, data, options, fields=fields)
    file, = [f for f in _files(table) if f.file_name.endswith('.row')]
    assert file.value_stats.null_counts == [1]
    builder = table.new_read_builder().with_projection(['v'])
    builder.with_filter(builder.new_predicate_builder().equal('v', datetime(2025, 1, 1)))
    data = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    assert data.column('v').cast(pa.int64()).to_pylist() == [timestamp_ns]
