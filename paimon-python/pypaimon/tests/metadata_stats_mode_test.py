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

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python.write.table_update_by_row_id import _RowIdUpdateFileWriter
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.table.row.generic_row import GenericRowDeserializer


pytestmark = pytest.mark.python_write


def _write(tmp_path, data, options, primary_keys=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', False)
    name = 'db.stats'
    catalog.create_table(name, Schema.from_pyarrow_schema(
        data.schema, primary_keys=primary_keys or [], options={
            'file.format': 'parquet', 'write.native.enabled': 'false',
            'commit.native.enabled': 'false', **options,
        }), False)
    table = catalog.get_table(name)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
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
