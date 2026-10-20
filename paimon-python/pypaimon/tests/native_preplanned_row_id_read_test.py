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

"""A Native reader enforces its metadata filters on previously planned splits."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.common.identifier import Identifier
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_plan_rest_test
from pypaimon.utils.range import Range


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
rest_catalog = native_plan_rest_test.rest_catalog
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]
_SCHEMA = pa.schema([('id', pa.int32()), ('value', pa.int32())])


def _append(table, start, schema=_SCHEMA):
    data = [{'id': value, 'value': value * 10} for value in range(start, start + 3)]
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(data, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _view(catalog, evolution, view, native):
    catalog.create_table('default.t', Schema.from_pyarrow_schema(_SCHEMA, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': str(evolution).lower(),
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false', 'read.batch-size': '2',
    }), False)
    table = catalog.get_table('default.t')
    _append(table, 10)
    _append(table, 13)
    catalog.create_tag(table.identifier, 'before_schema_change', 2)
    catalog.create_branch(table.identifier, 'dev', tag_name='before_schema_change')
    catalog.alter_table(table.identifier, [SchemaChange.add_column('extra', AtomicType('INT'))])
    table = catalog.get_table(table.identifier)
    _append(table, 16, _SCHEMA.append(pa.field('extra', pa.int32())))
    if view == 'historical':
        table = table.copy({'scan.snapshot-id': '1'})
        count = 3
    elif view == 'branch':
        table = catalog.get_table(Identifier('default', 't', branch='dev'))
        count = 6
    else:
        count = 9
    assert table.table_schema.id == (1 if view == 'current' else 0)
    return table.copy({'read.native.enabled': str(native).lower()}), count


def _read(table, splits, predicate, native, limit=None):
    builder = table.new_read_builder().with_projection(['id']).with_filter(predicate)
    if limit is not None:
        builder.with_limit(limit)
    read = builder.new_read()
    with ExitStack() as guards:
        if native:
            guards.enter_context(patch.object(read, '_create_split_read',
                                              side_effect=AssertionError('Python read fallback')))
        data = read.to_arrow(splits, parallelism=1)
    assert data.column_names == ['id']
    return sorted(data['id'].to_pylist())


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('evolution', [False, True])
@pytest.mark.parametrize('view', ['current', 'historical', 'branch'])
def test_reader_metadata_filters_apply_to_an_unfiltered_existing_plan(rest_catalog, native, evolution, view):
    catalog, _ = rest_catalog
    table, count = _view(catalog, evolution, view, native)
    plan = table.new_read_builder().new_scan().plan()
    pb = table.new_read_builder().with_projection(['id', 'value', '_ROW_ID', '_SEQUENCE_NUMBER']) \
        .new_predicate_builder()
    cases = [
        (pb.equal('_ROW_ID', 1), [11]),
        (pb.equal('_SEQUENCE_NUMBER', 2), [13, 14, 15]),
        (pb.and_predicates([pb.greater_or_equal('_ROW_ID', 1), pb.less_than('_ROW_ID', 5),
                            pb.greater_than('value', 120)]), [13, 14]),
        (pb.or_predicates([pb.equal('_ROW_ID', 1), pb.equal('value', 140)]), [11, 14]),
        (pb.or_predicates([pb.equal('_SEQUENCE_NUMBER', 1), pb.equal('_ROW_ID', 4)]), [10, 11, 12, 14]),
        (pb.and_predicates([pb.equal('_ROW_ID', 1), pb.equal('_ROW_ID', 2)]), []),
    ]
    for predicate, expected in cases:
        expected = [value for value in expected if value < 10 + count]
        assert _read(table, plan.splits(), predicate, native) == expected
        limited = _read(table, plan.splits(), predicate, native, 1)
        assert len(limited) == min(1, len(expected))
        assert set(limited).issubset(expected)

    if evolution:
        selected = table.new_read_builder().new_scan().with_row_ranges([Range(1, 4)]).plan()
        predicate = pb.or_predicates([pb.equal('_ROW_ID', row_id) for row_id in (0, 2, 4, 5)])
        expected = [value for value in [12, 14] if value < 10 + count]
        assert _read(table, selected.splits(), predicate, native) == expected
