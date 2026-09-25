################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import datetime
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.row.generic_row import GenericRow


pytestmark = pytest.mark.python_write


@pytest.fixture
def make_table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)

    def create(options=None, sequence_type=None, partitioned=False):
        arrow_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('seq', sequence_type if sequence_type is not None else pa.int64()),
            ('seq2', pa.int64()), ('val', pa.string()), ('pt', pa.string()),
        ])
        opts = {'bucket': '1', 'sequence.field': 'seq'}
        opts.update(options or {})
        catalog.create_table('default.t', Schema.from_pyarrow_schema(
            arrow_schema, primary_keys=['pt', 'id'] if partitioned else ['id'],
            partition_keys=['pt'] if partitioned else [], options=opts), False)
        return catalog.get_table('default.t'), arrow_schema

    return create


def write_rows(table, schema, rows, grouping='batch', streaming=False):
    builder = (table.new_stream_write_builder() if streaming
               else table.new_batch_write_builder())
    commit = builder.new_commit()
    groups = [[row] for row in rows] if grouping == 'commits' else [rows]
    try:
        for identifier, group in enumerate(groups, 1):
            writer = builder.new_write()
            try:
                batch = pa.Table.from_pylist(group, schema=schema)
                if grouping == 'chunks':
                    for chunk in batch.to_batches(max_chunksize=1):
                        writer.write_arrow_batch(chunk)
                elif grouping == 'rows':
                    for row in group:
                        writer.write_row(GenericRow(
                            [row.get(field.name) for field in table.fields], table.fields))
                else:
                    writer.write_arrow(batch)
                if streaming:
                    commit.commit(writer.prepare_commit(identifier), identifier)
                else:
                    commit.commit(writer.prepare_commit())
                    commit.close()
                    commit = builder.new_commit()
            finally:
                writer.close()
    finally:
        commit.close()


def read_rows(table, projection=None):
    builder = table.new_read_builder()
    if projection is not None:
        builder.with_projection(projection)
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pylist()


@pytest.mark.parametrize('engine', ['deduplicate', 'partial-update'])
@pytest.mark.parametrize('grouping', ['batch', 'chunks', 'rows', 'commits'])
@pytest.mark.parametrize('order', ['ascending', 'descending'])
@pytest.mark.parametrize('streaming', [False, True])
def test_sequence_order_is_independent_of_write_grouping(
        make_table, engine, grouping, order, streaming):
    table, schema = make_table({'merge-engine': engine, 'sequence.field.sort-order': order})
    rows = [
        {'id': 1, 'seq': 100, 'val': 'high'},
        {'id': 2, 'seq': 100, 'val': 'tie-first'},
        {'id': 3, 'seq': 100, 'val': 'non-null'},
        {'id': 4, 'seq': None, 'val': 'null-first'},
        {'id': 1, 'seq': 50, 'val': 'low'},
        {'id': 2, 'seq': 100, 'val': 'tie-last'},
        {'id': 3, 'seq': None, 'val': 'null'},
        {'id': 4, 'seq': None, 'val': 'null-last'},
    ]
    write_rows(table, schema, rows, grouping, streaming)
    assert read_rows(table, ['id', 'val']) == [
        {'id': 1, 'val': 'high' if order == 'ascending' else 'low'},
        {'id': 2, 'val': 'tie-last'},
        {'id': 3, 'val': 'non-null'},
        {'id': 4, 'val': 'null-last'},
    ]


def test_partial_update_fills_nulls_in_sequence_order(make_table):
    table, schema = make_table({'merge-engine': 'partial-update'})
    write_rows(table, schema, [
        {'id': 1, 'seq': 100, 'seq2': 2, 'val': None},
        {'id': 1, 'seq': 50, 'seq2': 1, 'val': 'filled'},
    ], 'chunks')
    assert read_rows(table, ['id', 'seq', 'seq2', 'val']) == [
        {'id': 1, 'seq': 100, 'seq2': 2, 'val': 'filled'},
    ]


def test_sequence_order_across_buffer_flushes(make_table):
    table, schema = make_table({'target-file-size': '1 b'})
    write_rows(table, schema, [
        {'id': 1, 'seq': 100, 'val': 'high'},
        {'id': 1, 'seq': 50, 'val': 'low'},
    ], 'chunks')
    assert read_rows(table, ['id', 'val']) == [{'id': 1, 'val': 'high'}]


@pytest.mark.parametrize('order,expected', [('ascending', 'high'), ('descending', 'low')])
def test_compound_sequence_in_partitioned_table(make_table, order, expected):
    table, schema = make_table({
        'sequence.field': 'seq,seq2', 'sequence.field.sort-order': order,
    }, partitioned=True)
    rows = []
    for pt in ['a', 'b']:
        rows.extend([
            {'id': 1, 'pt': pt, 'seq': 10, 'seq2': 2, 'val': 'high'},
            {'id': 1, 'pt': pt, 'seq': 9, 'seq2': 99, 'val': 'low'},
            {'id': 1, 'pt': pt, 'seq': 10, 'seq2': 1, 'val': 'middle'},
            {'id': 1, 'pt': pt, 'seq': 10, 'seq2': None, 'val': 'null'},
        ])
    write_rows(table, schema, rows, 'chunks')
    assert sorted(read_rows(table, ['id', 'pt', 'val']), key=lambda row: row['pt']) == [
        {'id': 1, 'pt': pt, 'val': expected} for pt in ['a', 'b']
    ]


@pytest.mark.parametrize('type_,high,low', [
    (pa.decimal128(10, 2), Decimal('100.50'), Decimal('50.25')),
    (pa.timestamp('us'), datetime.datetime(2020, 1, 2), datetime.datetime(2020, 1, 1)),
    (pa.date32(), datetime.date(2020, 1, 2), datetime.date(2020, 1, 1)),
    (pa.time32('ms'), datetime.time(12, 0), datetime.time(1, 0)),
    (pa.float64(), 2.5, -1.0),
    (pa.binary(), b'z', b'a'),
])
def test_typed_sequence_fields(make_table, type_, high, low):
    table, schema = make_table(sequence_type=type_)
    write_rows(table, schema, [
        {'id': 1, 'seq': high, 'val': 'high'},
        {'id': 1, 'seq': low, 'val': 'low'},
    ])
    assert read_rows(table)[0]['seq'] == high
    assert read_rows(table, ['id', 'val']) == [{'id': 1, 'val': 'high'}]


@pytest.mark.parametrize('type_', [pa.float32(), pa.float64()])
@pytest.mark.parametrize('order', ['ascending', 'descending'])
@pytest.mark.parametrize('grouping', ['batch', 'chunks', 'commits'])
@pytest.mark.parametrize('streaming', [False, True])
def test_floating_sequence_order(make_table, type_, order, grouping, streaming):
    table, schema = make_table({
        'sequence.field.sort-order': order,
        'write.native.enabled': 'true', 'read.native.enabled': 'true',
    }, sequence_type=type_)
    # Pairs are in Java Float/Double.compare order, with null always first.
    pairs = [
        (1.0, float('nan')), (float('inf'), float('nan')),
        (float('-inf'), 1.0), (1.0, float('inf')),
        (-0.0, 0.0), (None, float('nan')), (None, 0.0),
    ]
    rows, expected = [], []
    for low, high in pairs:
        for reverse in (False, True):
            key = len(expected)
            pair = [{'id': key, 'seq': low, 'val': 'low'},
                    {'id': key, 'seq': high, 'val': 'high'}]
            rows.extend(reversed(pair) if reverse else pair)
            expected.append({'id': key, 'val': 'high' if low is None or order == 'ascending'
                             else 'low'})
    for values in ((float('nan'), -float('nan')), (None, None), (-0.0, -0.0)):
        key = len(expected)
        rows.extend({'id': key, 'seq': value, 'val': label}
                    for value, label in zip(values, ('first', 'last')))
        expected.append({'id': key, 'val': 'last'})
    write_rows(table, schema, rows, grouping, streaming)
    assert read_rows(table, ['id', 'val']) == expected


@pytest.mark.parametrize('order', ['ascending', 'descending'])
@pytest.mark.parametrize('grouping', ['batch', 'commits'])
def test_nan_ties_compare_next_sequence_field(make_table, order, grouping):
    table, schema = make_table({
        'sequence.field': 'seq,seq2', 'sequence.field.sort-order': order,
        'write.native.enabled': 'true', 'read.native.enabled': 'true',
    }, sequence_type=pa.float64())
    write_rows(table, schema, [
        {'id': 1, 'seq': float('nan'), 'seq2': 2, 'val': 'high'},
        {'id': 1, 'seq': -float('nan'), 'seq2': 1, 'val': 'low'},
    ], grouping)
    assert read_rows(table, ['id', 'val']) == [
        {'id': 1, 'val': 'high' if order == 'ascending' else 'low'}]


@pytest.mark.parametrize('streaming', [False, True])
@pytest.mark.parametrize('direct', [False, True])
@pytest.mark.parametrize('sequence,type_,error', [
    ('missing', pa.int64(), ValueError),
    ('seq,seq', pa.int64(), ValueError),
    ('seq,,seq2', pa.int64(), ValueError),
    ('seq', pa.list_(pa.int64()), NotImplementedError),
])
def test_dynamic_bucket_sequence_rejected_before_index_creation(
        make_table, streaming, direct, sequence, type_, error):
    from pathlib import Path
    from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite

    table, _ = make_table({'bucket': '-1', 'sequence.field': sequence}, sequence_type=type_)
    builder = (table.new_stream_write_builder() if streaming
               else table.new_batch_write_builder())
    writer_class = StreamTableWrite if streaming else BatchTableWrite
    with patch.object(table, 'create_row_key_extractor') as extractor:
        with pytest.raises(error):
            if direct:
                writer_class(table, 'test')
            else:
                builder.new_write()
        extractor.assert_not_called()
    assert table.snapshot_manager().get_latest_snapshot() is None
    assert list(Path(table.table_path).glob('index/*')) == []
    assert list(Path(table.table_path).rglob('*.parquet')) == []
