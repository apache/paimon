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

"""Public scanner wrappers delegate to one authorized backend operation."""

import json
from unittest.mock import Mock

import pyarrow as pa
import pytest

from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.read.file_scanner import FileScanner
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.tests import read_backend_retention_test as retention

search_table = retention.search_table
_forbid_native = retention._forbid_native

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


def _scanner(builder, method, wrapper, calls, before=None, after=None):
    scan = builder.new_scan()

    def invoke(owner, delegate):
        calls.append(owner)
        # Fail at the first repeated invocation instead of overflowing the
        # Python stack when a broken base delegate dispatches this hook again.
        assert len(calls) < 2, 'Base scanner delegate reentered the public hook'
        if before is not None:
            before(owner)
        result = delegate()
        return result if after is None else after(owner, result)

    if wrapper == 'subclass':
        class WrappedScanner(FileScanner):
            pass

        def hook(self):
            return invoke(self, getattr(super(WrappedScanner, self), method))

        setattr(WrappedScanner, method, hook)
        scanner = WrappedScanner(scan)
    else:
        scanner = scan.file_scanner
        base = getattr(scanner, method)
        setattr(scanner, method, lambda: invoke(scanner, base))
    scan.file_scanner = scanner
    return scan, scanner


def _execute(scan, scanner, method, entry):
    if entry == 'direct':
        return getattr(scanner, method)()
    if method == 'scan':
        return scan.plan()
    if method == 'scan_with_stats':
        return scan.scan_with_stats()
    from pypaimon.read.read_dispatch import plan_files
    return plan_files(scanner)


def _plan(result, method):
    if method != 'scan_with_stats':
        return result
    plan, stats = result
    assert stats is not None
    assert plan.stats() is stats
    return plan


def _table(search_table):
    # Planning remains opted into Native. The custom Python operation must
    # retain its backend even when it calls the ordinary public base method.
    return search_table.copy_without_time_travel({'read.native.enabled': 'false'})


def _assert_rows(builder, result, method, expected_ids=(1, 2)):
    if method == 'plan_files':
        assert result and all(entry.kind == 0 for entry in result)
        assert sum(entry.file.row_count for entry in result) == 2
        return
    plan = _plan(result, method)
    assert plan.snapshot_id == 1
    assert builder.new_read().to_arrow(plan.splits())['id'].to_pylist() == list(expected_ids)


@pytest.mark.parametrize('method', ['scan', 'scan_with_stats', 'plan_files'])
@pytest.mark.parametrize('wrapper', ['subclass', 'instance'])
@pytest.mark.parametrize('entry', ['direct', 'table'])
def test_scanner_wrapper_calls_base_once_on_its_original_owner(search_table, method, wrapper, entry):
    builder = _table(search_table).new_read_builder().with_projection(['id'])
    calls = []
    scan, scanner = _scanner(builder, method, wrapper, calls)
    with _forbid_native():
        result = _execute(scan, scanner, method, entry)
        _assert_rows(builder, result, method)
    assert calls == [scanner]


def _rules(restriction):
    if restriction == 'mask':
        return TableQueryAuthResult(None, {'text': json.dumps({'name': 'NULL'})})
    return TableQueryAuthResult([json.dumps({
        'kind': 'LEAF',
        'transform': {'name': 'FIELD_REF',
                      'fieldRef': {'index': 0, 'name': 'id', 'type': 'INT'}},
        'function': 'EQUAL', 'literals': [2],
    })], None)


@pytest.mark.parametrize('method', ['scan', 'scan_with_stats'])
@pytest.mark.parametrize('wrapper', ['subclass', 'instance'])
@pytest.mark.parametrize('entry', ['direct', 'table'])
@pytest.mark.parametrize('restriction', ['filter', 'mask'])
def test_scanner_base_delegate_keeps_one_query_authorization(
        search_table, method, wrapper, entry, restriction):
    builder = _table(search_table).new_read_builder().with_projection(['id', 'text'])
    calls = []
    scan, scanner = _scanner(builder, method, wrapper, calls)
    rules = _rules(restriction)
    authorize = scan._query_auth_fn = Mock(return_value=rules)
    with _forbid_native():
        result = _execute(scan, scanner, method, entry)
        plan = _plan(result, method)
        assert plan.snapshot_id == 1 and plan.splits()
        for split in plan.splits():
            assert isinstance(split, QueryAuthSplit)
            assert split.auth_result is rules
            assert not isinstance(split.split, QueryAuthSplit)
        rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
    assert rows == ([{'id': 2, 'text': 'second'}] if restriction == 'filter' else [
        {'id': 1, 'text': None}, {'id': 2, 'text': None}])
    authorize.assert_called_once_with(['id', 'text'])
    assert calls == [scanner]


@pytest.mark.parametrize('wrapper', ['subclass', 'instance'])
@pytest.mark.parametrize('entry', ['direct', 'table'])
@pytest.mark.parametrize('restriction', ['filter', 'mask'])
def test_raw_base_delegate_still_requires_unrestricted_authorization(search_table, wrapper, entry, restriction):
    builder = _table(search_table).new_read_builder()
    calls = []
    scan, scanner = _scanner(builder, 'plan_files', wrapper, calls)
    authorize = scan._query_auth_fn = Mock(return_value=_rules(restriction))
    with _forbid_native(), pytest.raises(TableNoPermissionException):
        _execute(scan, scanner, 'plan_files', entry)
    authorize.assert_called_once_with(None)
    assert calls == ([scanner] if entry == 'direct' else [])


@pytest.mark.parametrize('method', ['scan', 'scan_with_stats', 'plan_files'])
@pytest.mark.parametrize('wrapper', ['subclass', 'instance'])
@pytest.mark.parametrize('entry', ['direct', 'table'])
def test_scanner_wrapper_can_reconfigure_limit_before_calling_base(search_table, method, wrapper, entry):
    table = _table(search_table).copy_without_time_travel({
        'deletion-vectors.enabled': 'false', 'source.split.target-size': '1 b'})
    retention._write(table.copy_without_time_travel({'scan.native-plan.enabled': 'false'}), pa.table({
        'id': pa.array([3], pa.int32()), 'text': ['third'],
        'embedding': pa.array([[2., 0.]], pa.list_(pa.float32())),
    }))
    builder = table.new_read_builder().with_projection(['id'])
    calls = []
    scan, scanner = _scanner(builder, method, wrapper, calls, before=lambda owner: setattr(owner, 'limit', 1))
    with _forbid_native():
        result = _execute(scan, scanner, method, entry)
        plan = None if method == 'plan_files' else _plan(result, method)
        if plan is None:
            # Raw metadata ignores LIMIT and retains both complete files.
            assert len(result) == 2 and sum(entry.file.row_count for entry in result) == 3
        else:
            # A stale delegate would keep both files. Java metadata LIMIT
            # selects only the first split, then the reader limits its rows.
            assert len(plan.splits()) == 1
            assert sum(len(split.files) for split in plan.splits()) == 1
            limited = builder.with_limit(scanner.limit).new_read().to_arrow(plan.splits())
            assert limited['id'].to_pylist() == [1]
            assert plan.snapshot_id == 2
    assert scanner.limit == 1 and calls == [scanner]


@pytest.mark.parametrize('wrapper', ['subclass', 'instance'])
@pytest.mark.parametrize('entry', ['direct', 'table'])
@pytest.mark.parametrize('failure_stage', ['before-base', 'after-base'])
def test_failed_wrapper_does_not_leave_its_base_delegate_active(search_table, wrapper, entry, failure_stage):
    builder = _table(search_table).new_read_builder().with_projection(['id'])
    calls = []
    failure = OSError('custom scanner failed once')
    pending = [True]

    def fail_once(owner, result=None):
        if pending[0]:
            pending[0] = False
            raise failure
        return result

    scan, scanner = _scanner(
        builder, 'scan', wrapper, calls,
        before=fail_once if failure_stage == 'before-base' else None,
        after=fail_once if failure_stage == 'after-base' else None)
    with _forbid_native():
        with pytest.raises(OSError) as caught:
            _execute(scan, scanner, 'scan', entry)
        assert caught.value is failure
        assert calls == [scanner]
        calls.clear()
        result = _execute(scan, scanner, 'scan', entry)
        _assert_rows(builder, result, 'scan')
    assert calls == [scanner]


def test_base_delegation_keeps_cross_method_hooks_and_one_authorization(search_table):
    builder = _table(search_table).new_read_builder().with_projection(['id'])
    scan = builder.new_scan()
    calls = []

    class WrappedScanner(FileScanner):
        def _record(self, method):
            calls.append((method, self))
            assert sum(name == method for name, _ in calls) == 1

        def scan(self):
            self._record('scan')
            return super().scan()

        def scan_with_stats(self):
            self._record('scan_with_stats')
            return super().scan_with_stats()

        def plan_files(self):
            self._record('plan_files')
            return super().plan_files()

    scanner = WrappedScanner(scan)
    scan.file_scanner = scanner
    scan.with_chunk_shuffle(7, 1)
    authorize = scan._query_auth_fn = Mock(return_value=None)
    with _forbid_native():
        plan, stats = scan.scan_with_stats()
        rows = builder.new_read().to_arrow(plan.splits())['id'].to_pylist()
    assert sorted(rows) == [1, 2]
    assert plan.snapshot_id == 1 and plan.stats() is stats
    assert [name for name, _ in calls] == ['scan_with_stats', 'scan', 'plan_files']
    assert all(owner is scanner for _, owner in calls)
    authorize.assert_called_once_with(['id'])


def test_row_scan_wrapper_cannot_reuse_projected_grant_for_raw_files(search_table):
    builder = _table(search_table).new_read_builder().with_projection(['id'])
    calls = []

    def inspect_raw_files(owner):
        with pytest.raises(TableNoPermissionException):
            FileScanner.plan_files(owner)

    scan, scanner = _scanner(builder, 'scan', 'subclass', calls, before=inspect_raw_files)
    selections = []

    def authorize(select):
        selections.append(select)
        # The selected id column is unrestricted; access to every column is
        # not. A row-query grant cannot authorize the raw-file metadata API.
        return None if select == ['id'] else _rules('mask')

    scan._query_auth_fn = authorize
    with _forbid_native():
        result = scan.plan()
        _assert_rows(builder, result, 'scan')
    assert selections == [['id'], None]
    assert calls == [scanner]


@pytest.mark.parametrize('entry', ['direct', 'table'])
def test_wrapper_cannot_reuse_grant_after_adding_a_protected_filter_field(search_table, entry):
    builder = _table(search_table).new_read_builder().with_projection(['id'])
    predicate = builder.table.new_read_builder().new_predicate_builder().equal('text', 'second')
    calls = []
    scan, scanner = _scanner(
        builder, 'scan', 'subclass', calls,
        before=lambda owner: setattr(owner, 'predicate', predicate))
    selections = []
    failure = TableNoPermissionException(builder.table.identifier)

    def authorize(select):
        selections.append(select)
        if 'text' in select:
            raise failure
        return None

    scan._query_auth_fn = authorize
    with _forbid_native(), pytest.raises(TableNoPermissionException) as caught:
        _execute(scan, scanner, 'scan', entry)
    assert caught.value is failure
    assert selections == ([['id', 'text']] if entry == 'direct' else [['id'], ['id', 'text']])
    assert calls == [scanner]


def test_base_delegate_applies_the_grant_for_its_expanded_query(search_table):
    builder = _table(search_table).new_read_builder().with_projection(['id'])
    predicate = builder.table.new_read_builder().new_predicate_builder().equal('text', 'second')
    calls = []
    scan, scanner = _scanner(
        builder, 'scan', 'subclass', calls,
        before=lambda owner: setattr(owner, 'predicate', predicate))
    selections = []
    rules = _rules('filter')

    def authorize(select):
        selections.append(select)
        return None if select == ['id'] else rules

    scan._query_auth_fn = authorize
    with _forbid_native():
        result = scan.plan()
        assert result.splits()
        for split in result.splits():
            assert isinstance(split, QueryAuthSplit) and split.auth_result is rules
            assert not isinstance(split.split, QueryAuthSplit)
        assert builder.new_read().to_arrow(result.splits())['id'].to_pylist() == [2]
    assert selections == [['id'], ['id', 'text']]
    assert calls == [scanner]
