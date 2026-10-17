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

"""DECIMAL predicates compare values without rounding literals to column scale."""

from contextlib import ExitStack
from decimal import Decimal, localcontext
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate import Predicate
from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.write.table_update import TableUpdate


pytestmark = pytest.mark.native_plan

_VALUES = [Decimal(value) for value in ['-1.24', '-1.23', '0.00', '1.23', '1.24']] + [None]
_PREDICATES = [
    ('equal', ['1.235'], []),
    ('notEqual', ['1.235'], [0, 1, 2, 3, 4]),
    ('lessThan', ['1.235'], [0, 1, 2, 3]),
    ('lessOrEqual', ['1.235'], [0, 1, 2, 3]),
    ('greaterThan', ['1.235'], [4]),
    ('greaterOrEqual', ['1.235'], [4]),
    ('in', ['1.235', '1.240', None], [4]),
    ('notIn', ['1.235', '1.240'], [0, 1, 2, 3]),
    ('notIn', ['1.235', None], []),
    ('between', ['-1.235', '1.235'], [1, 2, 3]),
    ('notBetween', ['-1.235', '1.235'], [0, 4]),
    ('lessThan', ['123456789'], [0, 1, 2, 3, 4]),
    ('equal', ['1E+37'], []),
    ('greaterThan', ['1E-38'], [3, 4]),
    ('lessThan', ['0E+100'], [0, 1]),
    ('lessThan', ['-1.23500000000000000000000000000000000000000'], [0]),
]


def _predicate(method, literals):
    return Predicate(method, 1, 'amount', [Decimal(value) if value is not None else None for value in literals])


def _table(tmp_path, native=True, evolution=False, indexed=None, precision=10):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('amount', pa.decimal128(precision, 2)), ('value', pa.int32())])
    evolution = evolution or indexed is not None
    options = {'write.native.enabled': str(native).lower(),
               'row-tracking.enabled': str(evolution).lower(),
               'deletion-vectors.enabled': str(evolution).lower(),
               'data-evolution.enabled': str(evolution).lower()}
    if indexed is not None:
        options.update({'global-index.enabled': 'true', 'global-index.search-mode': 'full'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options=options), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        # Separate files make incorrect min/max pruning observable too.
        for start in range(0, len(_VALUES), 2):
            writer.write_arrow(pa.table({'id': [start, start + 1], 'amount': _VALUES[start:start + 2],
                                        'value': [0, 0]}, schema=schema))
            commit.commit(writer.prepare_commit())
            writer.close()
            commit.close()
            if start + 2 < len(_VALUES):
                writer, commit = builder.new_write(), builder.new_commit()
    finally:
        writer.close()
        commit.close()
    if indexed is not None:
        messages = GlobalIndexBuilder(table, 'amount', index_type=indexed).build()
        assert messages
        commit = table.new_batch_write_builder().new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()
    return table


def _read(table, predicate, native_plan, native_read, projection=('id',)):
    table = table.copy({'scan.native-plan.enabled': str(native_plan).lower(),
                        'read.native.enabled': str(native_read).lower()})
    builder = table.new_read_builder().with_projection(list(projection))
    if predicate is not None:
        builder.with_filter(predicate)
    plan = builder.new_scan().plan()
    if native_plan:
        assert plan.stats().native_planned
    reader = builder.new_read()
    with ExitStack() as stack:
        if native_read:
            stack.enter_context(patch.object(reader, '_native_fallback',
                                             side_effect=AssertionError('Python read fallback')))
            stack.enter_context(patch.object(reader, '_create_split_read',
                                             side_effect=AssertionError('Python split reader')))
        rows = reader.to_arrow(plan.splits(), parallelism=1).to_pylist()
    return sorted(rows, key=lambda row: row['id'])


@pytest.mark.parametrize('native_plan,native_read', [(False, False), (True, False), (False, True), (True, True)])
@pytest.mark.parametrize('indexed', [None, 'btree', 'bitmap'])
@pytest.mark.parametrize('precision', [10, 38], ids=['compact', 'wide'])
def test_decimal_predicates_preserve_literal_value_in_scan_and_read(
        tmp_path, native_plan, native_read, indexed, precision):
    table = _table(tmp_path, indexed=indexed, precision=precision)
    # Literal construction and conversion must not depend on Decimal context.
    with localcontext() as context:
        context.prec = 3
        for method, literals, expected in _PREDICATES:
            assert _read(table, _predicate(method, literals), native_plan, native_read) == [
                {'id': i} for i in expected], (method, literals)
        fine = _predicate('lessThan', ['1.235'])
        exact = Predicate('equal', 1, 'amount', [Decimal('1.24')])
        for method, expected in [('and', []), ('or', list(range(5)))]:
            combined = Predicate(method, None, None, [fine, exact])
            assert _read(table, combined, native_plan, native_read) == [{'id': i} for i in expected]


@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['literal', 'callable', 'delete'])
def test_decimal_predicates_select_the_same_rows_for_updates_and_deletes(tmp_path, native, stream, operation):
    table = _table(tmp_path, native=native, evolution=True)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update()
    predicate = _predicate('between', ['-1.235', '1.235'])
    with ExitStack() as stack:
        if native:
            for method in ['_build_predicate_update_table', '_matched_delete_row_ids']:
                stack.enter_context(patch.object(TableUpdate, method,
                                                 side_effect=AssertionError('Python matching fallback')))
        if operation == 'delete':
            messages = update.delete_by_predicate(predicate, 19) if stream else update.delete_by_predicate(predicate)
        else:
            if operation == 'callable':
                def assignment(rows):
                    assert rows.column_names == ['id', '_ROW_ID']
                    return pa.array([i + 10 for i in rows['id'].to_pylist()], type=pa.int32())
                kwargs = {'read_columns': ['id']}
            else:
                assignment, kwargs = 42, {}
            messages = (update.update_by_predicate(predicate, {'value': assignment}, 19, **kwargs) if stream
                        else update.update_by_predicate(predicate, {'value': assignment}, **kwargs))
    commit = builder.new_commit()
    try:
        commit.commit(messages, 19) if stream else commit.commit(messages)
    finally:
        commit.close()
    ids = [0, 4, 5] if operation == 'delete' else list(range(6))
    expected = [{'id': i, 'amount': _VALUES[i], 'value': (
        (i + 10 if operation == 'callable' else 42) if i in (1, 2, 3) else 0)} for i in ids]
    for read_native in [False, True]:
        assert _read(table, None, read_native, read_native, ('id', 'amount', 'value')) == expected
