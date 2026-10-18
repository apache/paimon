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

"""Native partition metadata uses the same manifest view and Java casts as Python."""

from datetime import date, datetime, time
from decimal import Decimal
import json
import os
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.index.deletion_vector_meta import DeletionVectorMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.tests import native_plan_rest_test
from pypaimon.tests.native_system_table_test import _canonical, _read, _system


rest_catalog = native_plan_rest_test.rest_catalog

pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Native system-table reader required')]


def _create(catalog, schema, partition_keys, options=None):
    settings = {'write.native.enabled': 'false', 'commit.native.enabled': 'false'}
    settings.update(options or {})
    catalog.create_table('default.part_metadata', Schema.from_pyarrow_schema(
        schema, partition_keys=partition_keys, options=settings), False)
    return catalog.get_table('default.part_metadata')


def _append(table, rows, schema):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _assert_parity(table):
    expected = _read(_system(table, 'partitions', False))
    native = _system(table, 'partitions', True)
    with patch.object(type(native), '_build_arrow_table',
                      side_effect=AssertionError('Python partition aggregation ran')):
        actual = _read(native)
    assert actual.schema == expected.schema
    assert _canonical(actual, 'partitions') == _canonical(expected, 'partitions')
    return actual.to_pylist()


@pytest.mark.parametrize('typ,value,expected', [
    (pa.bool_(), True, 'true'),
    (pa.decimal128(10, 2), Decimal('1.20'), '1.20'),
    (pa.date32(), date(2024, 2, 29), '2024-02-29'),
    (pa.time32('ms'), time(12, 34, 56, 123000), '12:34:56'),
    (pa.timestamp('us'), datetime(2024, 2, 29, 12, 34, 56, 123456), '2024-02-29 12:34:56.123456'),
    (pa.string(), 'a/b=c', 'a/b=c'),
    (pa.float32(), 1.401298464324817e-45, '1.4E-45'),
    (pa.float64(), 5e-324, '4.9E-324'),
    (pa.binary(), b'hello', 'hello'),
    (pa.binary(), b'\xed\xa0\x80', '\ufffd'),
    (pa.string(), '\u00a0', '\u00a0'),
    (pa.string(), '\u0085', '\u0085'),
    (pa.string(), '\u001c', 'MISSING'),
])
def test_partition_values_are_java_casts(rest_catalog, typ, value, expected):
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', typ)])
    table = _create(catalog, schema, ['p'], {
        'partition.default-name': 'MISSING', 'partition.legacy-name': 'false'})
    _append(table, [{'id': 1, 'p': value}, {'id': 2, 'p': None}], schema)
    rows = _assert_parity(table)
    assert sorted(row['partition'] for row in rows) == sorted(['p=' + expected, 'p=MISSING'])
    assert [row['total_buckets'] for row in rows] == [-1, -1]
    assert [row['deleted_record_count'] for row in rows] == [0, 0]


def test_unpartitioned_metadata_uses_empty_string(rest_catalog):
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32())])
    table = _create(catalog, schema, [])
    _append(table, [{'id': 1}, {'id': 2}], schema)
    rows = _assert_parity(table)
    assert len(rows) == 1
    assert rows[0]['partition'] == ''
    assert rows[0]['record_count'] == 2


def test_declared_bucket_count_does_not_count_only_occupied_buckets(rest_catalog):
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', pa.string())])
    table = _create(catalog, schema, ['p'], {'bucket': '8', 'bucket-key': 'id'})
    _append(table, [{'id': 1, 'p': 'one'}], schema)
    rows = _assert_parity(table)
    assert rows[0]['file_count'] == 1
    assert rows[0]['total_buckets'] == 8


def test_rendered_partition_collisions_are_not_aggregated(rest_catalog):
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', pa.string()), ('p2', pa.string())])
    table = _create(catalog, schema, ['p', 'p2'])
    _append(table, [{'id': 1, 'p': 'a/p2=b', 'p2': 'c'},
                    {'id': 2, 'p': 'a', 'p2': 'b/p2=c'}], schema)
    rows = _assert_parity(table)
    assert [row['partition'] for row in rows] == ['p=a/p2=b/p2=c'] * 2
    assert [row['record_count'] for row in rows] == [1, 1]


def test_deletion_vector_counts_read_avro_metadata_and_keep_unknown(rest_catalog):
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', pa.string())])
    table = _create(catalog, schema, ['p'], {'deletion-vectors.enabled': 'true'})
    _append(table, [{'id': 1, 'p': 'known'}, {'id': 2, 'p': 'unknown'},
                    {'id': 3, 'p': 'zero'}], schema)
    entries = []
    for partition, bucket, counts in [
            ('known', 0, [1, 2]), ('known', 1, [3]),
            ('unknown', 0, [None]), ('unknown', 1, [5])]:
        ranges = {str(i): DeletionVectorMeta(str(i), 0, 1, count) for i, count in enumerate(counts)}
        entries.append(IndexManifestEntry(
            0, GenericRow([partition], table.partition_keys_fields), bucket,
            IndexFileMeta('DELETION_VECTORS', partition + str(bucket), 1, 1, dv_ranges=ranges)))
    # Only manifest metadata is needed; no deletion-vector payload is opened.
    table = _with_index_entries(table, entries)
    rows = _assert_parity(table)
    assert {row['partition']: row['deleted_record_count'] for row in rows} == {
        'p=known': 6, 'p=unknown': None, 'p=zero': 0}


def _with_index_entries(table, entries):
    name = IndexManifestFile(table).write(entries)
    snapshot = table.snapshot_manager().get_latest_snapshot()
    path = table.snapshot_manager().snapshot_dir + '/snapshot-' + str(snapshot.id)
    with table.file_io.new_input_stream(path) as stream:
        data = json.loads(stream.read())
    data['indexManifest'] = name
    table.file_io.overwrite_file_utf8(path, json.dumps(data))
    return table._copy_with_snapshot(table.snapshot_manager().get_latest_snapshot())


@pytest.mark.parametrize('typ,value,canonical,legacy', [
    (pa.string(), 'MISSING', None, True),
    (pa.string(), 'MISSING', None, False),
    (pa.string(), '\x1c', None, True),
    (pa.string(), '\x1c', None, False),
    # Java byte[].toString has no stable legacy spec; exercise its SQL cast.
    (pa.binary(), b'\xed\xa0\x80', '\ufffd'.encode('utf-8'), False),
    (pa.time32('ms'), time(12, 34, 56, 123000), time(12, 34, 56), True),
    (pa.time32('ms'), time(12, 34, 56, 123000), time(12, 34, 56), False),
])
def test_deletion_lookup_uses_java_partition_spec_round_trip(rest_catalog, typ, value, canonical, legacy):
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', typ)])
    table = _create(catalog, schema, ['p'], {
        'deletion-vectors.enabled': 'true', 'partition.default-name': 'MISSING',
        'partition.legacy-name': str(legacy).lower()})
    _append(table, [{'id': i, 'p': value} for i in range(3)]
            + [{'id': i + 3, 'p': canonical} for i in range(3)], schema)
    entries = []
    for partition, count in [(value, 1), (canonical, 3)]:
        ranges = {'file': DeletionVectorMeta('file', 0, 1, count)}
        entries.append(IndexManifestEntry(
            0, GenericRow([partition], table.partition_keys_fields), 0,
            IndexFileMeta('DELETION_VECTORS', str(count), 1, 1, dv_ranges=ranges)))
    table = _with_index_entries(table, entries)
    rows = _assert_parity(table)
    assert len(rows) == 2
    # A legacy TIME spec is an integer millisecond count, so Java keeps its
    # sub-second digits even when the declared TIME precision is zero.
    expected = [1, 3] if legacy and pa.types.is_time(typ) else [3, 3]
    assert sorted(row['deleted_record_count'] for row in rows) == expected


def test_default_legacy_time_partition_keeps_deletion_counts(rest_catalog):
    from pypaimon.tests.deletion_vector_path_test import _delete
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', pa.time32('ms'))])
    table = _create(catalog, schema, ['p'], {
        'deletion-vectors.enabled': 'true', 'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true'})
    _append(table, [{'id': i, 'p': time(12, 34, 56, 120000)} for i in range(2)], schema)
    _delete(table, [0])
    rows = _assert_parity(table)
    assert [(row['partition'], row['deleted_record_count']) for row in rows] == [('p=12:34:56', 1)]


@pytest.mark.parametrize('legacy,instant,expected,deleted', [
    (True, '2024-01-01T00:00:00+00:00', '2024-01-01 00:00:00.000', 0),
    (False, '2024-01-01T00:00:00+00:00', '2023-12-31 19:00:00.000', 1),
    (True, '2024-11-03T06:30:00+00:00', '2024-11-03 06:30:00.000', 0),
    (False, '2024-11-03T06:30:00+00:00', '2024-11-03 01:30:00.000', 0),
])
def test_temporal_ltz_partition_specs_match_java_in_new_york(rest_catalog, legacy, instant, expected, deleted):
    from pypaimon.tests.deletion_vector_path_test import _delete
    # Native caches the process system zone. CI runs this golden case in a
    # separate process initialized with New York, as for temporal cast tests.
    if os.environ.get('TZ') != 'America/New_York':
        pytest.skip('Java golden values require TZ=America/New_York')
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', pa.timestamp('ms', tz='UTC'))])
    table = _create(catalog, schema, ['p'], {
        'deletion-vectors.enabled': 'true', 'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true', 'partition.legacy-name': str(legacy).lower()})
    _append(table, [{'id': i, 'p': datetime.fromisoformat(instant)} for i in range(2)], schema)
    _delete(table, [0])
    rows = _assert_parity(table)
    assert [(row['partition'], row['deleted_record_count']) for row in rows] == [('p=' + expected, deleted)]


@pytest.mark.parametrize('native', [False, True])
def test_partition_split_is_captured_before_later_commits(rest_catalog, native):
    catalog, _ = rest_catalog
    schema = pa.schema([('id', pa.int32()), ('p', pa.string())])
    table = _create(catalog, schema, ['p'])
    _append(table, [{'id': 1, 'p': 'old'}], schema)
    system = _system(table, 'partitions', native)
    builder = system.new_read_builder()
    splits = builder.new_scan().plan().splits()
    _append(table, [{'id': 2, 'p': 'new'}], schema)
    rows = builder.new_read().to_arrow(splits).to_pylist()
    assert [row['partition'] for row in rows] == ['p=old']
    assert len(_assert_parity(table)) == 2
