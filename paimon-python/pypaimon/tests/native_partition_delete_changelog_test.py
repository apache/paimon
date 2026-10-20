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

"""Whole-partition deletes do not use the row-level changelog writer."""

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.write.native_update import create_native_predicate_delete, create_native_update
from pypaimon.write.table_update import TableUpdate


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust is not installed'))]


def _table(catalog, native=False, guarded=False):
    schema = pa.schema([('id', pa.int32()), ('pt', pa.string()), ('value', pa.int32())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt'], primary_keys=['id', 'pt'], options={
            'bucket': '1', 'changelog-producer': 'input', 'write.native.enabled': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
            'commit.native.enabled': 'false', 'query-auth.enabled': str(guarded).lower(),
        }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist([
            dict(id=1, pt='a', value=10), dict(id=2, pt='b', value=20),
        ], schema=schema))
        messages = writer.prepare_commit()
        assert sum(len(message.changelog_files) for message in messages) == 2
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    return table.copy({'write.native.enabled': str(native).lower()})


def test_only_partition_delete_qualifies_a_native_changelog_table(native_rest_catalog):
    table = _table(native_rest_catalog, True)
    predicates = table.new_read_builder().new_predicate_builder()
    native_table = MagicMock()
    with patch('pypaimon.write.native_update.native_runtime_available', return_value=True), \
            patch('pypaimon.write.native_update.create_native_write_table', return_value=native_table) as create:
        assert create_native_predicate_delete(table, 'partition', predicates.equal('pt', 'a')) is not None
        create.assert_called_once()
        create.reset_mock()
        for predicate in [None, predicates.equal('id', 1), PredicateBuilder.and_predicates([
                predicates.equal('pt', 'a'), predicates.equal('value', 10)]),
                Predicate('equal', 1, 'pt', ['a'], field_path=['value']),
                Predicate('equal', 1, 'pt', ['a'], field_path=['pt', 'nested'])]:
            assert create_native_predicate_delete(table, 'rows', predicate) is None
        assert create_native_update(table, 'rows', ['value']) is None
        create.assert_not_called()

    assert create_native_predicate_delete(object(), 'unsupported', None) is None


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('guarded', [False, True])
def test_input_changelog_partition_delete_keeps_other_partitions(native_rest_catalog, native, guarded):
    table = _table(native_rest_catalog, native, guarded)
    predicate = table.new_read_builder().new_predicate_builder().equal('pt', 'a')
    builder = table.new_batch_write_builder()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(
                TableUpdate, '_delete_by_partition_filter', side_effect=AssertionError('Python delete fallback')))
        messages = builder.new_update().delete_by_predicate(predicate)
    assert [tuple(message.partition) for message in messages] == [('a',)]
    assert sum(len(message.deleted_files) for message in messages) == 1
    assert all(not message.new_files and not message.changelog_files for message in messages)
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist() == [
        dict(id=2, pt='b', value=20)]
