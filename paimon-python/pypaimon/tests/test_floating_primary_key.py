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

import math

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.comparator import compare_values
from pypaimon.read.reader.sort_merge_reader import builtin_key_comparator
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow


pytestmark = [pytest.mark.python_plan, pytest.mark.python_read, pytest.mark.python_write]


@pytest.fixture(params=[pa.float32(), pa.float64()], ids=['FLOAT', 'DOUBLE'])
def floating_table(tmp_path, request):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    arrow_schema = pa.schema([('id', request.param), ('suffix', pa.string()), ('value', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        arrow_schema, primary_keys=['id', 'suffix'], options={
            'bucket': '1', 'file.format': 'parquet', 'primary-key.nullable': 'true',
        }), False)
    return catalog.get_table('default.t'), arrow_schema


def _write(table, schema, rows):
    builder = table.new_batch_write_builder()
    writer, committer = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        committer.commit(writer.prepare_commit())
    finally:
        writer.close()
        committer.close()


def _read(table):
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    return builder.new_read().to_arrow(splits).to_pylist(), splits


def _key(value):
    # Labels describe independent key identities without calling the comparator.
    if value is None:
        return 'null'
    if math.isnan(value):
        return 'nan'
    if value == 0.0:
        return '-zero' if math.copysign(1.0, value) < 0 else '+zero'
    return str(value)


@pytest.mark.parametrize('scalar', [float, np.float16, np.float32, np.float64])
def test_floating_scalar_representations_have_the_same_key_semantics(scalar):
    nan = scalar('nan')
    assert compare_values(scalar(1), nan) == -1
    assert compare_values(nan, scalar(2)) == 1
    assert compare_values(nan, float('nan')) == 0
    assert compare_values(1.0, nan) == -1
    assert compare_values(nan, 1.0) == 1
    assert compare_values(scalar(-0.0), 0.0) == -1
    assert compare_values(scalar(0.0), -0.0) == 1
    assert compare_values(None, nan) == -1

    fields = [DataField(0, 'id', AtomicType('FLOAT'))]
    comparator = builtin_key_comparator(fields)
    assert comparator(GenericRow([scalar(1)], fields), GenericRow([nan], fields)) == -1
    assert comparator(GenericRow([scalar(-0.0)], fields), GenericRow([scalar(0.0)], fields)) == -1


def test_finite_key_survives_nan_in_another_file(floating_table):
    table, schema = floating_table
    _write(table, schema, [{'id': 1.0, 'suffix': 'a', 'value': 'finite'}])
    _write(table, schema, [{'id': float('nan'), 'suffix': 'a', 'value': 'nan'}])
    actual, splits = _read(table)
    assert sum(len(split.files) for split in splits) == 2
    assert sorted(row['value'] for row in actual) == ['finite', 'nan']


def test_floating_keys_sort_fold_and_update_independently(floating_table):
    table, schema = floating_table
    _write(table, schema, [
        {'id': float('nan'), 'suffix': 'a', 'value': 'nan-old'},
        {'id': 0.0, 'suffix': 'b', 'value': 'positive-b'},
        {'id': -0.0, 'suffix': 'a', 'value': 'negative-a'},
        {'id': 1.0, 'suffix': 'a', 'value': 'finite-old'},
        {'id': None, 'suffix': 'b', 'value': 'null-b'},
        {'id': float('nan'), 'suffix': 'b', 'value': 'nan-b'},
        {'id': -0.0, 'suffix': 'b', 'value': 'negative-b'},
        {'id': None, 'suffix': 'a', 'value': 'null-old'},
        {'id': 0.0, 'suffix': 'a', 'value': 'positive-old'},
        {'id': float('nan'), 'suffix': 'a', 'value': 'nan-folded'},
    ])
    expected_keys = [
        ('null', 'a'), ('null', 'b'), ('-zero', 'a'), ('-zero', 'b'),
        ('+zero', 'a'), ('+zero', 'b'), ('1.0', 'a'), ('nan', 'a'), ('nan', 'b'),
    ]
    actual, splits = _read(table)
    assert [(_key(row['id']), row['suffix']) for row in actual] == expected_keys
    files = [file for split in splits for file in split.files]
    assert len(files) == 1
    physical = pq.read_table(files[0].file_path).to_pylist()
    assert [(_key(row['_KEY_id']), row['_KEY_suffix']) for row in physical] == expected_keys
    assert [row['value'] for row in actual if _key(row['id']) == 'nan' and row['suffix'] == 'a'] == ['nan-folded']
    assert files[0].min_key.values == [None, 'a']
    assert math.isnan(files[0].max_key.values[0])
    assert files[0].max_key.values[1] == 'b'

    _write(table, schema, [
        {'id': float('nan'), 'suffix': 'a', 'value': 'nan-new'},
        {'id': 1.0, 'suffix': 'a', 'value': 'finite-new'},
        {'id': 0.0, 'suffix': 'a', 'value': 'positive-new'},
        {'id': None, 'suffix': 'a', 'value': 'null-new'},
    ])
    actual, _ = _read(table)
    assert [(_key(row['id']), row['suffix']) for row in actual] == expected_keys
    assert [row['value'] for row in actual] == [
        'null-new', 'null-b', 'negative-a', 'negative-b', 'positive-new',
        'positive-b', 'finite-new', 'nan-new', 'nan-b',
    ]


def test_nan_file_bound_keeps_finite_versions_together(floating_table):
    table, schema = floating_table
    # A NaN upper bound contains every finite key, regardless of its suffix.
    _write(table, schema, [
        {'id': 1.0, 'suffix': 'a', 'value': 'one'},
        {'id': 2.0, 'suffix': 'z', 'value': 'old'},
        {'id': float('nan'), 'suffix': 'a', 'value': 'nan'},
    ])
    _write(table, schema, [{'id': 2.0, 'suffix': 'z', 'value': 'new'}])
    actual, splits = _read(table)
    assert len(splits) == 1
    assert not splits[0].raw_convertible
    assert [row['value'] for row in actual] == ['one', 'new', 'nan']
