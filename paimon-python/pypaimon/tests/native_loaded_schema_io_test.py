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

"""A catalog-loaded schema retains its authorization after ordinary evolution."""

from contextlib import ExitStack
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.data_evolution_merge_into import WhenMatched
from pypaimon.tests import native_plan_rest_test
from pypaimon.tests.native_authorized_update_test import (
    _ROWS, _SCHEMA, _expected, _files, _native_only, _operate, _restricted, _rows, _table,
)
from pypaimon.write.write_dispatch import NativeTableWrite


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
rest_catalog = native_plan_rest_test.rest_catalog
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _evolved(catalog, native):
    loaded = _table(catalog, False)
    empty = loaded._copy_with_snapshot(None)
    catalog.alter_table('default.t', [SchemaChange.add_column('extra', AtomicType('INT'))])
    latest = catalog.get_table('default.t')
    assert loaded.table_schema.id != latest.table_schema.id
    options = {'write.native.enabled': str(native).lower(), 'commit.native.enabled': str(native).lower()}
    return loaded.copy(options), empty.copy(options), latest


def _commit(table, messages, native):
    commit = table.new_batch_write_builder().new_commit()
    try:
        guard = (patch.object(commit.file_store_commit, 'commit', side_effect=AssertionError('Python commit fallback'))
                 if native else ExitStack())
        with guard:
            commit.commit(messages)
    finally:
        commit.close()


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('operation', ['row-id', 'predicate', 'upsert', 'row-delete', 'shard', 'merge'])
def test_loaded_schema_updates_survive_later_catalog_schema(rest_catalog, native, operation):
    catalog, server = rest_catalog
    loaded, _, latest = _evolved(catalog, native)
    builder = loaded.new_batch_write_builder()
    with patch.object(server, '_table_query_auth_handle', wraps=server._table_query_auth_handle) as authorize:
        with ExitStack() as stack:
            _native_only(stack, native)
            messages = _operate(builder.new_update(), operation)
        assert messages
        assert authorize.call_count >= 1
    # The original, non-time-traveled handle also remains a valid committer.
    _commit(loaded, messages, native)
    assert _rows(latest) == [dict(row, extra=None) for row in _expected(operation)]
    assert latest.snapshot_manager().get_latest_snapshot().schema_id == latest.table_schema.id


@pytest.mark.parametrize('native', _ENGINES)
def test_loaded_schema_append_keeps_original_fields_and_current_snapshot_schema(rest_catalog, native):
    catalog, _ = rest_catalog
    loaded, _, latest = _evolved(catalog, native)
    writer = loaded.new_batch_write_builder().new_write()
    try:
        assert isinstance(writer, NativeTableWrite) == native
        writer.write_arrow(pa.Table.from_pylist([dict(id=4, value=40, name='d')], schema=_SCHEMA))
        messages = writer.prepare_commit()
        assert all(file.schema_id == loaded.table_schema.id for message in messages for file in message.new_files)
        _commit(loaded, messages, native)
    finally:
        writer.close()
    assert _rows(latest) == [dict(row, extra=None) for row in _ROWS + [dict(id=4, value=40, name='d')]]
    assert latest.snapshot_manager().get_latest_snapshot().schema_id == latest.table_schema.id


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('operation', ['predicate', 'shard', 'merge'])
def test_loaded_empty_view_stays_empty_after_catalog_schema_evolution(rest_catalog, native, operation):
    catalog, server = rest_catalog
    _, captured, latest = _evolved(catalog, native)
    update = captured.new_batch_write_builder().new_update()
    assignment = Mock(side_effect=AssertionError('A captured empty view evaluated an assignment'))
    with patch.object(server, '_table_query_auth_handle', wraps=server._table_query_auth_handle) as authorize:
        with ExitStack() as stack:
            _native_only(stack, native)
            if operation == 'merge':
                messages = update.merge_into(pa.Table.from_pylist(_ROWS[:1], schema=_SCHEMA), on=['id'],
                                             when_matched=[WhenMatched.update({'value': assignment})])
            else:
                messages = _operate(update, operation, assignment=assignment)
        assert authorize.call_count >= 1
    assert messages == []
    assignment.assert_not_called()
    assert _rows(latest) == [dict(row, extra=None) for row in _ROWS]


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('policy', ['denied', 'filter', 'mask'])
def test_loaded_schema_still_requires_a_fresh_unrestricted_grant(rest_catalog, native, policy):
    catalog, server = rest_catalog
    loaded, _, latest = _evolved(catalog, native)
    assignment = Mock(side_effect=AssertionError('A restricted update evaluated its assignment'))
    before = _files(latest)
    with _restricted(server, loaded, policy) as authorize:
        with ExitStack() as stack:
            _native_only(stack, native)
            with pytest.raises(Exception, match='[Pp]ermission|denied|unrestricted|restrict|[Mm]ask|query.auth'):
                _operate(loaded.new_batch_write_builder().new_update(), 'predicate', assignment=assignment)
        assert authorize.call_count >= 1
    assignment.assert_not_called()
    assert _files(latest) == before
