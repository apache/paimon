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

"""Java-compatible user sequence ordering across Python and Native files."""

from contextlib import ExitStack
from decimal import Decimal
import datetime
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.write.native_write import NativeTableWrite


pytestmark = pytest.mark.native_plan


def _table(tmp_path, sequence_type, native, order, engine):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('seq', sequence_type),
                        ('seq2', pa.int32()), ('value', pa.string())])
    options = {'bucket': '1', 'sequence.field': 'seq,seq2',
               'sequence.field.sort-order': order, 'merge-engine': engine,
               'write.native.enabled': str(native).lower()}
    if engine == 'aggregation':
        options['fields.value.aggregate-function'] = 'last_value'
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options=options), False)
    return catalog.get_table('default.t'), schema


def _write(table, groups, native, streaming):
    builder = table.new_stream_write_builder() if streaming else table.new_batch_write_builder()
    commit = builder.new_commit()
    try:
        for identifier, group in enumerate(groups, 1):
            writer = builder.new_write()
            if native:
                assert isinstance(writer, NativeTableWrite)
            try:
                for chunk in group:
                    writer.write_arrow(chunk)
                messages = (writer.prepare_commit(identifier) if streaming
                            else writer.prepare_commit())
                if streaming:
                    commit.commit(messages, identifier)
                else:
                    commit.commit(messages)
                    commit.close()
                    commit = builder.new_commit()
                if native:
                    assert writer._python_writer is None
            finally:
                writer.close()
    finally:
        commit.close()


def _read(table, native, predicate=None, projection=None):
    copy = table.copy({'read.native.enabled': str(native).lower(),
                       'scan.native-plan.enabled': str(native).lower()})
    builder = copy.new_read_builder()
    if predicate is not None:
        builder.with_filter(predicate)
    if projection is not None:
        builder.with_projection(projection)
    splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(read, '_create_split_read',
                                             side_effect=AssertionError('Python read fallback')))
        return read.to_arrow(splits).sort_by('id').to_pylist()


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('streaming', [False, True])
@pytest.mark.parametrize('separate_commits', [False, True])
@pytest.mark.parametrize('engine', ['deduplicate', 'partial-update', 'aggregation'])
@pytest.mark.parametrize('sequence_type,high,low', [
    (pa.int64(), 100, 50), (pa.string(), 'z', 'a'), (pa.binary(), b'z', b'a'),
    (pa.decimal128(10, 2), Decimal('100.50'), Decimal('50.25')),
    (pa.timestamp('us'), datetime.datetime(2020, 1, 2), datetime.datetime(2020, 1, 1)),
    (pa.time32('ms'), datetime.time(12, 0), datetime.time(1, 0)),
])
def test_descending_sequence_dispatch(
        tmp_path, native, streaming, separate_commits, engine, sequence_type, high, low):
    table, schema = _table(tmp_path, sequence_type, native, 'descending', engine)
    batches = []
    for i, value in enumerate([high, low, None]):
        batches.append(pa.table({'id': [1, 2, 3], 'seq': [value, high, None],
                                 'seq2': [0, [2, 1, None][i], None],
                                 'value': [str(i)] * 3}, schema=schema))
    _write(table, [[batch] for batch in batches] if separate_commits else [batches],
           native, streaming)
    expected = [{'id': 1, 'value': '1'}, {'id': 2, 'value': '1'}, {'id': 3, 'value': '2'}]
    for native_read in (False, True):
        # A later NULL must not beat non-NULL, the second sequence field breaks
        # ties, and automatic sequence still resolves entirely equal tuples.
        assert _read(table, native_read, projection=['id', 'value']) == expected
        predicate = PredicateBuilder(table.fields).equal('value', '1')
        assert _read(table, native_read, predicate, ['id', 'value']) == expected[:2]
