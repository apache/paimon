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

from decimal import Decimal, localcontext
import operator
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate import Predicate
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.read.reader.filter_record_batch_reader import FilterRecordBatchReader
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange


COMPARISONS = [
    ('equal', operator.eq),
    ('notEqual', operator.ne),
    ('lessThan', operator.lt),
    ('lessOrEqual', operator.le),
    ('greaterThan', operator.gt),
    ('greaterOrEqual', operator.ge),
]


@pytest.mark.parametrize('method,compare', COMPARISONS)
@pytest.mark.parametrize(
    'bound',
    [
        Decimal('2'),
        Decimal('24.000'),
        Decimal('1.001'),
        Decimal('-1.001'),
        Decimal('1E-41'),
        Decimal('1E+100'),
        Decimal('-1E+100'),
        24,
    ],
)
def test_decimal_comparison_matches_python(
    method: str,
    compare: Callable[[Decimal, Union[Decimal, int]], bool],
    bound: Union[Decimal, int],
) -> None:
    decimal_values: List[Optional[Decimal]] = [
        Decimal(value)
        for value in (
            '-24.00',
            '-1.01',
            '-1.00',
            '0.00',
            '1.00',
            '1.01',
            '2.00',
            '24.00',
        )
    ] + [None]
    table = pa.table({'value': pa.array(decimal_values, type=pa.decimal128(15, 2))})
    predicate = Predicate(method, 0, 'value', [bound])
    expected = [
        value for value in decimal_values if value is not None and compare(value, bound)
    ]
    # A small context must not round a high precision bound or column value.
    with localcontext() as context:
        context.prec = 6
        expr = predicate.to_arrow(table.schema)
    actual = ds.Scanner.from_batches(table.to_reader(), filter=expr).to_table()
    assert actual.column('value').to_pylist() == expected


@pytest.mark.parametrize(
    'method,literals,expected',
    [
        ('in', [Decimal('1'), Decimal('2.001'), None], [Decimal('1.00')]),
        ('notIn', [Decimal('1'), None], []),
        (
            'between',
            [Decimal('-1'), Decimal('1.001')],
            [Decimal('-1.00'), Decimal('1.00')],
        ),
        ('notBetween', [Decimal('-1'), Decimal('1.001')], [Decimal('2.00')]),
        (
            'in',
            [Decimal('1E-41'), Decimal('1E+100'), Decimal('1.00')],
            [Decimal('1.00')],
        ),
        (
            'notIn',
            [Decimal('1E-41'), Decimal('1E+100'), Decimal('1.00')],
            [Decimal('-1.00'), Decimal('2.00')],
        ),
    ],
)
def test_decimal_set_and_range_filters(
    method: str, literals: List[Optional[Decimal]], expected: List[Decimal]
) -> None:
    values = [Decimal('-1.00'), Decimal('1.00'), Decimal('2.00'), None]
    table = pa.table({'value': pa.array(values, type=pa.decimal128(15, 2))})
    predicate = Predicate(method, 0, 'value', literals)
    actual = ds.Scanner.from_batches(
        table.to_reader(), filter=predicate.to_arrow(table.schema)
    ).to_table()
    assert actual.column('value').to_pylist() == expected


def test_decimal_large_literal_preserves_precision() -> None:
    values = [Decimal('9007199254740992.00'), Decimal('9007199254740993.00')]
    table = pa.table({'value': pa.array(values, type=pa.decimal128(38, 2))})
    predicate = Predicate('equal', 0, 'value', [Decimal('9007199254740993.00')])
    actual = ds.Scanner.from_batches(
        table.to_reader(), filter=predicate.to_arrow(table.schema)
    ).to_table()
    assert actual.column('value').to_pylist() == [values[1]]


@pytest.mark.parametrize(
    'bound,expected',
    [
        (Decimal('24.000'), [Decimal('1.00'), Decimal('17.00')]),
        (Decimal('24.001'), [Decimal('1.00'), Decimal('17.00'), Decimal('24.00')]),
    ],
)
def test_decimal_bound_preserves_row_group_pruning(
    tmp_path: Path, bound: Decimal, expected: List[Decimal]
) -> None:
    values = [Decimal('1.00'), Decimal('17.00'), Decimal('24.00'), Decimal('50.00')]
    table = pa.table({'value': pa.array(values, type=pa.decimal128(15, 2))})
    path = tmp_path / 'decimals.parquet'
    pq.write_table(table, path, row_group_size=1)
    dataset = ds.dataset(path)
    predicate = Predicate('lessThan', 0, 'value', [bound])
    expression = predicate.to_arrow(table.schema)
    selected_groups = [
        group
        for fragment in dataset.get_fragments()
        for group in fragment.split_by_row_group(filter=expression)
    ]
    assert len(selected_groups) == len(expected)
    assert dataset.to_table(filter=expression).column('value').to_pylist() == expected


def test_decimal_batch_filter_after_normalization(tmp_path: Path) -> None:
    batch = pa.record_batch(
        [
            pa.array(
                [Decimal('1.00'), Decimal('17.00'), Decimal('24.00'), None],
                type=pa.decimal128(15, 2),
            )
        ],
        names=['value'],
    )
    # Exercise the real post-read filter separately from physical file pushdown.
    path = tmp_path / 'batch.parquet'
    pq.write_table(pa.Table.from_batches([batch]), path)
    source = FormatPyArrowReader(
        LocalFileIO(),
        'parquet',
        str(path),
        Schema.from_pyarrow_schema(batch.schema).fields,
        None,
    )
    reader = FilterRecordBatchReader(
        source, Predicate('lessThan', 0, 'value', [Decimal('24')])
    )
    try:
        first = reader.read_arrow_batch()
        assert first is not None
        assert first.column(0).to_pylist() == [Decimal('1.00'), Decimal('17.00')]
        assert reader.read_arrow_batch() is None
    finally:
        reader.close()


def test_decimal_composed_predicate_uses_schema() -> None:
    values = [Decimal('-1.00'), Decimal('0.00'), Decimal('1.00'), Decimal('2.00')]
    table = pa.table({'value': pa.array(values, type=pa.decimal128(15, 2))})
    predicate = Predicate(
        'or',
        None,
        None,
        [
            Predicate(
                'and',
                None,
                None,
                [
                    Predicate('greaterOrEqual', 0, 'value', [Decimal('0')]),
                    Predicate('lessThan', 0, 'value', [Decimal('1.001')]),
                ],
            ),
            Predicate('equal', 0, 'value', [Decimal('2')]),
        ],
    )
    actual = ds.Scanner.from_batches(
        table.to_reader(), filter=predicate.to_arrow(table.schema)
    ).to_table()
    assert actual.column('value').to_pylist() == values[1:]


def test_decimal_filter_after_scale_evolution(tmp_path: Path) -> None:
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', False)
    options = {'file.format': 'parquet', 'scan.native-plan.enabled': 'false'}
    old_schema = pa.schema([('id', pa.int64()), ('value', pa.decimal128(10, 2))])
    catalog.create_table(
        'default.decimals',
        Schema.from_pyarrow_schema(old_schema, options=options),
        False,
    )

    def write_rows(
        rows: List[Dict[str, Union[int, Decimal]]], schema: pa.Schema
    ) -> None:
        table = catalog.get_table('default.decimals')
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()

    write_rows(
        [
            {'id': 0, 'value': Decimal('1.00')},
            {'id': 1, 'value': Decimal('17.00')},
            {'id': 2, 'value': Decimal('24.00')},
        ],
        old_schema,
    )
    catalog.alter_table(
        'default.decimals',
        [SchemaChange.update_column_type('value', AtomicType('DECIMAL(12, 3)'))],
        False,
    )
    new_schema = pa.schema([('id', pa.int64()), ('value', pa.decimal128(12, 3))])
    write_rows(
        [
            {'id': 3, 'value': Decimal('1.001')},
            {'id': 4, 'value': Decimal('17.001')},
            {'id': 5, 'value': Decimal('24.000')},
        ],
        new_schema,
    )

    table = catalog.get_table('default.decimals')
    read_builder = table.new_read_builder()
    predicate = read_builder.new_predicate_builder().less_than(
        'value', Decimal('17.001')
    )
    read_builder.with_filter(predicate).with_projection(['id'])
    actual = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())
    assert actual is not None
    assert sorted(actual.column('id').to_pylist()) == [0, 1, 3]
