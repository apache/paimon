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

"""Java BucketSelector semantics through REST, normal and explain planning."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.read.native_plan import native_runtime_available
from pypaimon.tests.native_scan_stats_test import _append, _create
from pypaimon.tests import native_plan_rest_test

rest_catalog = native_plan_rest_test.rest_catalog

pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust planner required')]


def _selected(table, predicate, native, stats=True, partition_filter=None, limit=None):
    reader = table.copy({'scan.native-plan.enabled': str(native).lower(),
                         'read.native.enabled': str(native).lower()}).new_read_builder()
    reader.with_filter(predicate)
    if limit is not None:
        reader.with_limit(limit)
    if partition_filter is not None:
        reader.with_partition_filter(partition_filter)
    scan = reader.new_scan()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(scan.file_scanner, 'scan',
                                             side_effect=AssertionError('Native scan fell back')))
            stack.enter_context(patch.object(scan.file_scanner, 'scan_with_stats',
                                             side_effect=AssertionError('Native statistics fell back')))
            stack.enter_context(patch('pypaimon.read.table_read.TableRead._create_split_read',
                                      side_effect=AssertionError('Native read fell back')))
        if stats:
            plan, counters = scan.scan_with_stats()
            assert plan.scan_stats() is counters
        else:
            plan, counters = scan.plan(), None
        rows = reader.new_read().to_arrow(plan.splits()).to_pylist()
    files = sorted((tuple(split.partition.values), split.bucket,
                    tuple(file.file_name for file in split.files)) for split in plan.splits())
    return files, sorted(rows, key=lambda row: tuple(row[name] for name in sorted(row))), counters


def _predicate(pb, case):
    equal = lambda name, value: pb.equal(name, value)
    and_ = pb.and_predicates
    or_ = pb.or_predicates
    if case == 'or':
        return or_([equal('id', 2), equal('id', 3)])
    if case == 'intersection':
        return and_([pb.is_in('id', [1, 2, 3]), pb.is_in('id', [2, 3, 4])])
    if case == 'partition-or':
        return or_([and_([equal('pt', 1), equal('id', 3)]),
                    and_([equal('pt', 2), equal('id', 7)])])
    if case == 'unconstrained-branch':
        return or_([and_([equal('pt', 1), equal('id', 3)]), equal('pt', 2)])
    if case == 'unsafe-or':
        return or_([equal('id', 3), equal('value', 0)])
    if case == 'safe-and':
        return and_([equal('id', 3), or_([equal('id', 3), equal('value', 0)])])
    if case == 'contradiction':
        return and_([equal('id', 2), equal('id', 3)])
    raise AssertionError(case)


@pytest.mark.parametrize('primary_key', [False, True], ids=['append', 'pk'])
@pytest.mark.parametrize('case', ['or', 'intersection', 'partition-or', 'unconstrained-branch',
                                  'unsafe-or', 'safe-and', 'contradiction'])
def test_native_bucket_funnel_matches_java_compatible_python(rest_catalog, primary_key, case):
    catalog, _ = rest_catalog
    schema = pa.schema([('pt', pa.int32()), ('id', pa.int32()), ('value', pa.int32())])
    table, _ = _create(catalog, schema, ['pt'], ['pt', 'id'] if primary_key else None,
                       {'bucket': '4', 'bucket-key': 'id', 'manifest.sidecar.enabled': 'true'})
    rows = [{'pt': pt, 'id': index, 'value': index} for pt in [1, 2] for index in range(64)]
    _append(table, rows, schema)
    predicate = _predicate(table.new_read_builder().new_predicate_builder(), case)
    files, expected, python = _selected(table, predicate, False)
    actual_files, actual, native = _selected(table, predicate, True)
    assert actual == expected
    assert actual_files == files
    assert native.entries_potential_total == python.entries_potential_total == 8
    assert native.entries_after_partition == python.entries_after_partition
    assert native.entries_after_bucket == python.entries_after_bucket
    assert native.entries_after_stats == python.entries_after_stats
    assert _selected(table, predicate, True, stats=False)[:2] == (actual_files, actual)
    if case == 'partition-or':
        assert native.entries_after_bucket == 2
    elif case == 'unconstrained-branch':
        assert native.entries_after_bucket == 5
    elif case in ('unsafe-or', 'contradiction'):
        assert native.entries_after_bucket == 8
    else:
        assert native.entries_after_bucket < 8


@pytest.mark.parametrize('case', ['one-key-or', 'intersection', 'mixed-or'])
def test_native_composite_bucket_keys_preserve_java_clause_boundaries(rest_catalog, case):
    catalog, _ = rest_catalog
    schema = pa.schema([('pt', pa.int32()), ('a', pa.int32()), ('b', pa.int32()), ('value', pa.int32())])
    table, _ = _create(catalog, schema, ['pt'], ['pt', 'a', 'b'], {'bucket': '8', 'bucket-key': 'a,b'})
    _append(table, [{'pt': pt, 'a': a, 'b': b, 'value': a + b}
                    for pt in [1, 2] for a in range(16) for b in range(16)], schema)
    pb = table.new_read_builder().new_predicate_builder()
    if case == 'one-key-or':
        predicate = pb.and_predicates([pb.or_predicates([pb.equal('a', 1), pb.equal('a', 2)]), pb.equal('b', 3)])
    elif case == 'intersection':
        predicate = pb.and_predicates([pb.is_in('a', [1, 2, 3]), pb.is_in('a', [2, 3, 4]), pb.equal('b', 3)])
    else:
        predicate = pb.or_predicates([pb.equal('a', 1), pb.equal('b', 3)])
    files, expected, python = _selected(table, predicate, False)
    actual_files, actual, native = _selected(table, predicate, True)
    assert (actual_files, actual) == (files, expected)
    assert native.entries_after_bucket == python.entries_after_bucket
    assert native.entries_after_stats == python.entries_after_stats
    if case == 'mixed-or':
        assert native.entries_after_bucket == 16
    else:
        assert native.entries_after_bucket <= 4


def test_explicit_partition_filter_does_not_replace_partition_values_in_bucket_predicate(rest_catalog):
    catalog, _ = rest_catalog
    schema = pa.schema([('pt', pa.int32()), ('id', pa.int32()), ('value', pa.int32())])
    table, _ = _create(catalog, schema, ['pt'], ['pt', 'id'], {'bucket': '4', 'bucket-key': 'id'})
    _append(table, [{'pt': pt, 'id': index, 'value': index} for pt in [1, 2] for index in range(64)], schema)
    pb = table.new_read_builder().new_predicate_builder()
    predicate = _predicate(pb, 'partition-or')
    selector = pb.equal('pt', 2)
    files, expected, python = _selected(table, predicate, False, partition_filter=selector)
    actual_files, actual, native = _selected(table, predicate, True, partition_filter=selector)
    assert (actual_files, actual) == (files, expected)
    assert actual == [{'pt': 2, 'id': 7, 'value': 7}]
    assert native.entries_after_partition == python.entries_after_partition == 4
    assert native.entries_after_bucket == python.entries_after_bucket == 1


@pytest.mark.parametrize('masked', ['pt', 'value'])
def test_native_bucket_specialization_excludes_masked_inputs_and_keeps_safe_conjuncts(rest_catalog, masked):
    import json
    from pypaimon.api.api_response import AuthTableQueryResponse
    from pypaimon.schema.schema_change import SchemaChange
    catalog, server = rest_catalog
    schema = pa.schema([('pt', pa.string()), ('id', pa.int32()), ('value', pa.string())])
    table, _ = _create(catalog, schema, ['pt'], ['pt', 'id'], {'bucket': '4', 'bucket-key': 'id'})
    _append(table, [{'pt': pt, 'id': index, 'value': str(index)}
                    for pt in ['a', 'b'] for index in range(64)], schema)
    catalog.alter_table(table.identifier, [SchemaChange.set_option('query-auth.enabled', 'true')])
    table = catalog.get_table(table.identifier)
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        column_masking={masked: json.dumps({'name': 'CONCAT', 'inputs': ['visible']})})
    pb = table.new_read_builder().new_predicate_builder()
    if masked == 'pt':
        # Raw partition 'a' would select id=7, losing the matching id=3 after masking.
        predicate = pb.or_predicates([
            pb.and_predicates([pb.equal('pt', 'a'), pb.equal('id', 7)]),
            pb.and_predicates([pb.equal('pt', 'visible'), pb.equal('id', 3)]),
        ])
    else:
        predicate = pb.and_predicates([pb.equal('id', 3), pb.equal('value', 'visible')])
    files, expected, python = _selected(table, predicate, False)
    actual_files, actual, native = _selected(table, predicate, True)
    assert (actual_files, actual) == (files, expected)
    assert [row['id'] for row in actual] == [3, 3]
    assert all(row[masked] == 'visible' for row in actual)
    assert native.entries_after_bucket == python.entries_after_bucket == (8 if masked == 'pt' else 2)
    assert _selected(table, predicate, True, stats=False)[:2] == (actual_files, actual)


@pytest.mark.parametrize('limit', [0, 1])
def test_native_correlated_bucket_statistics_precede_limit(rest_catalog, limit):
    catalog, _ = rest_catalog
    schema = pa.schema([('pt', pa.int32()), ('id', pa.int32()), ('value', pa.int32())])
    table, _ = _create(catalog, schema, ['pt'], ['pt', 'id'], {'bucket': '4', 'bucket-key': 'id'})
    _append(table, [{'pt': pt, 'id': index, 'value': index} for pt in [1, 2] for index in range(64)], schema)
    predicate = _predicate(table.new_read_builder().new_predicate_builder(), 'partition-or')
    files, actual, stats = _selected(table, predicate, True, limit=limit)
    assert stats.entries_potential_total == stats.entries_after_partition == 8
    assert stats.entries_after_bucket == 2
    assert len(actual) == limit
    matches = [{'pt': 1, 'id': 3, 'value': 3}, {'pt': 2, 'id': 7, 'value': 7}]
    assert all(row in matches for row in actual)
    plain_files, plain_rows, _ = _selected(table, predicate, True, stats=False, limit=limit)
    assert plain_files == files
    assert len(plain_rows) == limit
    # PK bucket scans have no row order, so LIMIT may select either matching row.
    assert all(row in matches for row in plain_rows)


@pytest.mark.parametrize('changelog', [False, True])
def test_native_incremental_bucket_specialization_preserves_physical_events(rest_catalog, changelog):
    import json
    catalog, _ = rest_catalog
    schema = pa.schema([('pt', pa.int32()), ('id', pa.int32()), ('value', pa.int32())])
    options = {'bucket': '4', 'bucket-key': 'id'}
    if changelog:
        options['changelog-producer'] = 'input'
    table, _ = _create(catalog, schema, ['pt'], ['pt', 'id'], options)
    for timestamp in [1000, 2000]:
        _append(table, [{'pt': pt, 'id': index, 'value': timestamp}
                        for pt in [1, 2] for index in range(64)], schema)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        path = table.snapshot_manager().get_snapshot_path(snapshot.id)
        metadata = json.loads(table.file_io.read_file_utf8(path))
        metadata['timeMillis'] = timestamp
        table.file_io.overwrite_file_utf8(path, json.dumps(metadata))
    table = table.copy({'incremental-between-timestamp': '0,2000'})
    predicate = _predicate(table.new_read_builder().new_predicate_builder(), 'partition-or')
    files, expected, python = _selected(table, predicate, False)
    actual_files, actual, native = _selected(table, predicate, True)
    assert (actual_files, actual) == (files, expected)
    assert len(actual) == 4
    assert native.entries_after_partition == python.entries_after_partition == 16
    assert native.entries_after_bucket == python.entries_after_bucket == 4
    assert native.entries_after_stats == python.entries_after_stats
    assert _selected(table, predicate, True, stats=False)[:2] == (actual_files, actual)
