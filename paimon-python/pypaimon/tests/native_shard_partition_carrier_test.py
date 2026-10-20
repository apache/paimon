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

"""A full-row shard update may carry its unchanged partition columns."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.write.native_update import NativeShardTableUpdator


pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _append(table, data):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(data)
        builder.new_commit().commit(writer.prepare_commit())
    finally:
        writer.close()


def _tables(catalog, native, historical):
    schema = pa.schema([('p', pa.string()), ('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.carriers', Schema.from_pyarrow_schema(
        schema, partition_keys=['p'], options={
            'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
            'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
        }), False)
    table = catalog.get_table('default.carriers')
    _append(table, pa.table({'p': ['a', 'b'], 'id': [1, 2], 'value': [10, 20]}, schema=schema))
    target = table
    if historical:
        catalog.alter_table('default.carriers', [SchemaChange.add_column('extra', AtomicType('INT'))])
        table = catalog.get_table('default.carriers')
        _append(table, pa.table({'p': ['a'], 'id': [3], 'value': [30], 'extra': [300]},
                                schema=schema.append(pa.field('extra', pa.int32()))))
        target = table.copy({'scan.snapshot-id': '1'})
    return table, target.copy({'write.native.enabled': str(native).lower()})


def _rows(table):
    read = table.new_read_builder().with_projection(table.field_names + ['_ROW_ID'])
    return read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pylist()


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('historical', [False, True], ids=['current', 'historical'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('partition_only', [False, True], ids=['full-row', 'partition-only'])
def test_shards_keep_unchanged_partition_carriers(
        native_rest_catalog, native, historical, stream, partition_only):
    latest, target = _tables(native_rest_catalog, native, historical)
    before = _rows(latest)
    builder = target.new_stream_write_builder() if stream else target.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch('pypaimon.write.table_update.ShardTableUpdator',
                                      side_effect=AssertionError('Python shard fallback')))
        update = builder.new_update()
        if partition_only:
            update.with_update_type(['p'])
        shard = update.new_shard_updator(0, 1)
        if native:
            assert isinstance(shard, NativeShardTableUpdator)
        try:
            with shard.arrow_reader() as reader:
                batches = list(reader)
            assert sum(batch.num_rows for batch in batches) == 2
            for batch in batches:
                if partition_only:
                    output = pa.record_batch([batch.column(batch.schema.get_field_index('p'))], names=['p'])
                else:
                    output = batch.set_column(batch.schema.get_field_index('value'), 'value',
                                              pc.multiply(batch.column(batch.schema.get_field_index('id')), 100))
                shard.update_by_arrow_batch(output)
            messages = shard.prepare_commit()
        finally:
            if native:
                shard.close()
    assert len(messages) == 2
    assert all(message.check_from_snapshot == 1 for message in messages)
    assert all(file.schema_id == target.table_schema.id and file.row_count == 1
               and file.first_row_id is not None for message in messages for file in message.new_files)
    assert sorted(file.first_row_id for message in messages for file in message.new_files) == [0, 1]
    publisher = latest.new_stream_write_builder() if stream else latest.new_batch_write_builder()
    commit = publisher.new_commit()
    try:
        commit.commit(messages, 37) if stream else commit.commit(messages)
    finally:
        commit.close()
    if stream:
        assert latest.snapshot_manager().get_latest_snapshot().commit_identifier == 37
    expected = [dict(row) for row in before]
    if not partition_only:
        for row in expected[:2]:
            row['value'] = row['id'] * 100
    assert _rows(latest) == expected
