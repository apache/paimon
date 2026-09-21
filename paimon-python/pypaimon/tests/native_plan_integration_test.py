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

import datetime
import importlib.util
import io
import json
import os
import pickle
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.read.native_plan import (
    native_family_search_modes_available, native_method_available, native_read,
    native_reader_available, native_split_bridge_available,
    native_split_from_python,
)
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.row.blob import BlobDescriptor, BlobRef
from pypaimon.utils.range import Range


def _has_native_planner():
    try:
        from pypaimon_rust.datafusion import PaimonCatalog, Split
    except Exception:
        return False
    return hasattr(PaimonCatalog, 'get_table') and hasattr(Split, 'serialize')


def _has_native_row_ranges():
    try:
        from pypaimon_rust.datafusion import ReadBuilder
    except ImportError:
        return False
    return hasattr(ReadBuilder, 'with_row_ranges')


def _mosaic_supports_nested_row():
    if importlib.util.find_spec('mosaic') is None:
        return False

    import mosaic

    schema = pa.schema([('nested', pa.struct([('value', pa.int32())]))])
    table = pa.Table.from_pylist([{'nested': {'value': 1}}], schema=schema)
    try:
        mosaic.write_table(table, io.BytesIO())
    except RuntimeError as error:
        if 'unsupported DataType: Struct' in str(error):
            return False
        raise
    return True


@pytest.mark.native_plan
@unittest.skipUnless(_has_native_planner(),
                     "pypaimon_rust with split-planning API not installed")
class NativePlanIntegrationTest(unittest.TestCase):
    """Live round-trip guarding the cross-language SplitSerializer against drift:
    plan via pypaimon_rust, decode, and require the same rows as the normal plan.
    The golden unit tests only prove self-consistency; this proves byte-compat
    with the real producer."""

    def setUp(self):
        self.cat = CatalogFactory.create({'warehouse': tempfile.mkdtemp(prefix='np_it_')})
        self.cat.create_database('default', True)
        self.schema = pa.schema([('k', pa.int64()), ('v', pa.string())])

    def _write(self, name, rows):
        t = self.cat.get_table('default.%s' % name)
        wb = t.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        w.write_arrow(pa.Table.from_pylist(rows, schema=self.schema))
        c.commit(w.prepare_commit())
        w.close()
        c.close()

    def _plan_and_read(self, name, native):
        t = self.cat.get_table('default.%s' % name)
        if native:
            t = t.copy({'scan.native-plan.enabled': 'true'})
        rb = t.new_read_builder()
        plan = rb.new_scan().plan()
        rows = rb.new_read().to_arrow(plan.splits()).to_pylist()
        return plan.snapshot_id, sorted(rows, key=lambda r: r['k'])

    def _assert_matches(self, name, expect_native=True):
        sid_n, rows_n = self._plan_and_read(name, native=False)
        sid_r, rows_r = self._plan_and_read(name, native=True)
        self.assertEqual(rows_r, rows_n)
        self.assertEqual(sid_r, sid_n)   # snapshot id preserved through native plan
        self.assertIsNotNone(sid_r)
        # Guard against a false green where native silently fell back to Python: assert the
        # native planner was (or was not) actually used, as expected for this table.
        native_table = self.cat.get_table('default.%s' % name).copy(
            {'scan.native-plan.enabled': 'true'})
        self.assertEqual(
            native_table.new_read_builder().explain().native_planned, expect_native)

    @staticmethod
    def _native_rows(builder, plan):
        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError('Python reader was used')):
            return builder.new_read().to_arrow(plan.splits()).to_pylist()

    def test_primary_key_matches_normal_plan(self):
        self.cat.create_table('default.pk_t', Schema.from_pyarrow_schema(
            self.schema, primary_keys=['k'], options={'bucket': '1'}), False)
        self._write('pk_t', [{'k': 1, 'v': 'a1'}, {'k': 2, 'v': 'b1'}])
        self._write('pk_t', [{'k': 2, 'v': 'b2'}, {'k': 3, 'v': 'c1'}])  # k=2 updated
        self._assert_matches('pk_t')

    def test_pk_equal_to_partition_key_falls_back(self):
        # Rust rejects empty trimmed PK schemas; Python must retain its supported behavior.
        self.cat.create_table('default.pkpart_t', Schema.from_pyarrow_schema(
            self.schema, partition_keys=['k'], primary_keys=['k'], options={'bucket': '1'}), False)
        self._write('pkpart_t', [{'k': 1, 'v': 'a1'}, {'k': 2, 'v': 'b1'}])
        self._write('pkpart_t', [{'k': 2, 'v': 'b2'}])
        native_table = self.cat.get_table('default.pkpart_t').copy(
            {'scan.native-plan.enabled': 'true'})
        self.assertFalse(native_table.new_read_builder().explain().native_planned)
        with self.assertRaises(ValueError):
            rb = native_table.new_read_builder()
            rb.new_read().to_arrow(rb.new_scan().plan().splits())

    def test_copy_removed_persisted_scan_option_uses_native(self):
        # The resolved schema must replace, rather than merge, persisted options.
        self.cat.create_table('default.snapopt_t', Schema.from_pyarrow_schema(
            self.schema, options={'scan.snapshot-id': '1'}), False)
        self._write('snapopt_t', [{'k': 1, 'v': 'a'}])   # snapshot 1
        self._write('snapopt_t', [{'k': 2, 'v': 'b'}])   # snapshot 2
        native = self.cat.get_table('default.snapopt_t').copy(
            {'scan.snapshot-id': None, 'scan.native-plan.enabled': 'true'})
        builder = native.new_read_builder()
        scan = builder.new_scan()
        with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native fallback')):
            plan = scan.plan()
        self.assertEqual(plan.snapshot_id, 2)
        self.assertEqual(sorted(builder.new_read().to_arrow(plan.splits()).to_pylist(),
                                key=lambda row: row['k']),
                         [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])

    def test_first_row_batch_scan_uses_native_plan(self):
        self.cat.create_table('default.fr_t', Schema.from_pyarrow_schema(
            self.schema, primary_keys=['k'],
            options={'bucket': '1', 'merge-engine': 'first-row'}), False)
        self._write('fr_t', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])
        self._write('fr_t', [{'k': 1, 'v': 'X'}, {'k': 3, 'v': 'c'}])  # k=1 stays 'a'
        self._assert_matches('fr_t')
        self.assertEqual(self._plan_and_read('fr_t', native=False)[1], [])

    def test_append_matches_normal_plan(self):
        self.cat.create_table(
            'default.ap_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('ap_t', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])
        self._write('ap_t', [{'k': 3, 'v': 'c'}])
        self._assert_matches('ap_t')

    @unittest.skipUnless(native_reader_available(),
                         "pypaimon-rust native reader API not installed")
    def test_native_read_bypasses_python_split_reader(self):
        self.cat.create_table(
            'default.native_read_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('native_read_t', [
            {'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}, {'k': 3, 'v': 'c'}])

        table = self.cat.get_table('default.native_read_t').copy(
            {'read.native.enabled': 'true'})
        builder = table.new_read_builder().with_projection(['k']).with_limit(2)
        builder.with_filter(builder.new_predicate_builder().greater_or_equal('k', 2))
        plan = builder.new_scan().plan()
        splits = [pickle.loads(pickle.dumps(split)) for split in plan.splits()]

        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError('Python reader was used')):
            read = builder.new_read()
            rows = read.to_arrow(splits).to_pylist()
            streamed = pa.Table.from_batches(
                list(read.to_arrow_batch_reader(splits))).to_pylist()

        self.assertEqual(rows, [{'k': 2}, {'k': 3}])
        self.assertEqual(streamed, rows)
        self.assertTrue(builder.explain().native_planned)

    @unittest.skipUnless(native_split_bridge_available(),
                         "pypaimon-rust split bridge API not installed")
    def test_native_read_bridges_python_planned_splits(self):
        self.cat.create_table(
            'default.python_plan_native_read',
            Schema.from_pyarrow_schema(self.schema), False)
        self._write('python_plan_native_read', [
            {'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}, {'k': 3, 'v': 'c'}])
        table = self.cat.get_table('default.python_plan_native_read').copy({
            'read.native.enabled': 'true',
        })
        builder = table.new_read_builder().with_projection(['k'])
        scan = builder.new_scan()
        # Force the capability branch that motivates this bridge: Python owns
        # planning, while Rust still performs the physical read.
        with patch.object(scan, '_native_plan_supported', return_value=False):
            plan = scan.plan()
        self.assertTrue(plan.splits())
        self.assertTrue(all(
            getattr(split, '_native_split', None) is None
            for split in plan.splits()))

        with patch(
                'pypaimon.read.native_plan.native_split_from_python',
                wraps=native_split_from_python) as bridge, patch(
                    'pypaimon.read.native_plan.native_read',
                    wraps=native_read) as rust_read, patch(
                        'pypaimon.read.table_read.TableRead._create_split_read',
                        side_effect=AssertionError('Python reader was used')):
            rows = builder.new_read().to_arrow(plan.splits()).to_pylist()

        self.assertEqual(rows, [{'k': 1}, {'k': 2}, {'k': 3}])
        self.assertEqual(bridge.call_count, len(plan.splits()))
        self.assertGreaterEqual(rust_read.call_count, 1)

    @unittest.skipUnless(native_split_bridge_available(),
                         "pypaimon-rust split bridge API not installed")
    def test_native_read_bridges_python_indexed_data_evolution_split(self):
        self.cat.create_table(
            'default.python_indexed_native_read',
            Schema.from_pyarrow_schema(self.schema, options={
                'data-evolution.enabled': 'true',
                'row-tracking.enabled': 'true',
            }), False)
        self._write('python_indexed_native_read', [
            {'k': 10, 'v': 'a'}, {'k': 20, 'v': 'b'}, {'k': 30, 'v': 'c'}])
        table = self.cat.get_table('default.python_indexed_native_read').copy({
            'read.native.enabled': 'true',
        })
        builder = table.new_read_builder()
        scan = builder.new_scan().with_row_ranges([Range(1, 1)])
        with patch.object(scan, '_native_plan_supported', return_value=False):
            plan = scan.plan()

        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError('Python reader was used')), patch(
                    'pypaimon.read.native_plan.native_read',
                    wraps=native_read) as rust_read:
            rows = builder.new_read().to_arrow(plan.splits()).to_pylist()

        self.assertEqual(rows, [{'k': 20, 'v': 'b'}])
        self.assertGreaterEqual(rust_read.call_count, 1)

    def test_query_auth_uses_python_plan_and_reader(self):
        self.cat.create_table(
            'default.query_auth_native_read',
            Schema.from_pyarrow_schema(self.schema), False)
        self._write('query_auth_native_read', [
            {'k': 1, 'v': 'sales'},
            {'k': 2, 'v': 'eng'},
            {'k': 3, 'v': 'eng'},
        ])
        table = self.cat.get_table('default.query_auth_native_read').copy({
            'read.native.enabled': 'true',
        })
        auth_filter = json.dumps({
            'kind': 'LEAF',
            'transform': {
                'name': 'FIELD_REF',
                'fieldRef': {'index': 1, 'name': 'v', 'type': 'STRING'},
            },
            'function': 'EQUAL',
            'literals': ['eng'],
        })
        auth = TableQueryAuthResult(
            [auth_filter], {'k': json.dumps({'name': 'NULL'})})
        table.catalog_environment.table_query_auth = (
            lambda options, identifier: lambda select: auth)
        builder = table.new_read_builder().with_projection(['k']).with_limit(1)
        plan = builder.new_scan().plan()
        self.assertTrue(all(
            split.__class__.__name__ == 'QueryAuthSplit'
            for split in plan.splits()))

        with patch(
                'pypaimon.read.native_plan.native_read',
                wraps=native_read) as rust_read:
            rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
            streamed = builder.new_read().to_arrow_batch_reader(
                plan.splits()).read_all().to_pylist()

        self.assertEqual(rows, [{'k': None}])
        self.assertEqual(streamed, rows)
        rust_read.assert_not_called()

    @unittest.skipUnless(native_reader_available(),
                         "pypaimon-rust native reader API not installed")
    def test_native_read_uses_effective_split_parallelism(self):
        schema = pa.schema([
            ('k', pa.int64()),
            ('v', pa.string()),
            ('dt', pa.string()),
        ])
        self.cat.create_table('default.native_parallel_t',
                              Schema.from_pyarrow_schema(
                                  schema,
                                  partition_keys=['dt']), False)
        table = self.cat.get_table('default.native_parallel_t')
        write_builder = table.new_batch_write_builder()
        write = write_builder.new_write()
        write.write_arrow(pa.Table.from_pylist([
            {'k': 1, 'v': 'a', 'dt': 'p1'},
            {'k': 2, 'v': 'b', 'dt': 'p2'},
            {'k': 3, 'v': 'c', 'dt': 'p3'},
            {'k': 4, 'v': 'd', 'dt': 'p4'},
        ], schema=schema))
        write_builder.new_commit().commit(write.prepare_commit())
        write.close()

        native_table = table.copy({
            'read.native.enabled': 'true',
            'read.parallelism': '2',
        })
        builder = native_table.new_read_builder()
        plan = builder.new_scan().plan()
        self.assertEqual(len(plan.splits()), 4)

        with patch('pypaimon.read.native_plan.native_read',
                   wraps=native_read) as rust_reads, \
                patch(
                    'pypaimon.read.table_read.TableRead._create_split_read',
                    side_effect=AssertionError('Python reader was used')):
            rows = builder.new_read().to_arrow(plan.splits()).to_pylist()

        self.assertEqual(sorted(rows, key=lambda row: row['k']), [
            {'k': 1, 'v': 'a', 'dt': 'p1'},
            {'k': 2, 'v': 'b', 'dt': 'p2'},
            {'k': 3, 'v': 'c', 'dt': 'p3'},
            {'k': 4, 'v': 'd', 'dt': 'p4'},
        ])
        self.assertEqual(rust_reads.call_count, 2)

        with patch('pypaimon.read.native_plan.native_read',
                   wraps=native_read) as rust_reads, \
                patch(
                    'pypaimon.read.table_read.TableRead._create_split_read',
                    side_effect=AssertionError('Python reader was used')):
            streamed = builder.new_read().to_arrow_batch_reader(
                plan.splits()).read_all().to_pylist()

        self.assertEqual(sorted(streamed, key=lambda row: row['k']), [
            {'k': 1, 'v': 'a', 'dt': 'p1'},
            {'k': 2, 'v': 'b', 'dt': 'p2'},
            {'k': 3, 'v': 'c', 'dt': 'p3'},
            {'k': 4, 'v': 'd', 'dt': 'p4'},
        ])
        self.assertEqual(rust_reads.call_count, 2)

    @unittest.skipUnless(native_reader_available(),
                         "pypaimon-rust native reader API not installed")
    def test_native_read_primary_key_matches_python(self):
        self.cat.create_table('default.native_read_pk', Schema.from_pyarrow_schema(
            self.schema, primary_keys=['k'], options={'bucket': '1'}), False)
        self._write('native_read_pk', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'old'}])
        self._write('native_read_pk', [{'k': 2, 'v': 'new'}, {'k': 3, 'v': 'c'}])

        normal = self._plan_and_read('native_read_pk', native=False)[1]
        table = self.cat.get_table('default.native_read_pk').copy(
            {'read.native.enabled': 'true'})
        builder = table.new_read_builder()
        plan = builder.new_scan().plan()
        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError('Python reader was used')):
            native_read_instance = builder.new_read()
            native = sorted(
                native_read_instance.to_arrow(plan.splits()).to_pylist(),
                key=lambda row: row['k'])
            native_read_instance.include_row_kind = True
            with_row_kind = native_read_instance.to_arrow(
                plan.splits()).to_pylist()

        self.assertEqual(native, normal)
        self.assertEqual(
            {row['_row_kind'] for row in with_row_kind}, {'+I'})
        self.assertEqual(
            sorted(({key: value for key, value in row.items()
                     if key != '_row_kind'} for row in with_row_kind),
                   key=lambda row: row['k']),
            normal,
        )

    @unittest.skipUnless(
        native_method_available('ReadBuilder', 'with_nested_projection'),
        "pypaimon-rust nested native reader API not installed")
    def test_native_read_nested_rows_and_map_keys_across_formats(self):
        base_fields = [
            ('id', pa.int64()),
            ('payload', pa.struct([
                ('details', pa.struct([
                    ('score', pa.int32()),
                    ('label', pa.string()),
                ])),
                ('ignored', pa.string()),
            ])),
        ]
        rows = [
            {'id': 1,
             'payload': {'details': {'score': 7, 'label': 'a'}, 'ignored': 'x'},
             'attrs': {'selected': 10, 'other': 11},
             'top.with.dot': 'first'},
            {'id': 2, 'payload': None, 'attrs': None, 'top.with.dot': 'second'},
            {'id': 3,
             'payload': {'details': None, 'ignored': 'z'},
             'attrs': {},
             'top.with.dot': 'third'},
        ]
        formats = ['parquet', 'orc', 'avro', 'row']
        if _mosaic_supports_nested_row():
            formats.append('mosaic')
        for file_format in formats:
            with self.subTest(file_format=file_format):
                # PyPaimon's Avro writer has no MAP conversion; it still covers
                # recursive ROW pruning. Other formats also exercise literal
                # MAP-key extraction from the native batch.
                include_map = file_format != 'avro'
                include_dotted_name = file_format != 'avro'
                nested_schema = pa.schema(
                    base_fields
                    + ([('attrs', pa.map_(pa.string(), pa.int32()))]
                       if include_map else [])
                    + ([('top.with.dot', pa.string())]
                       if include_dotted_name else []))
                projection = ['payload.details.score']
                if include_map:
                    projection.append("attrs['selected']")
                if include_dotted_name:
                    projection.append('top.with.dot')
                projection.append('id')
                expected = [
                    {'payload_details_score': 7,
                     'top.with.dot': 'first', 'id': 1},
                    {'payload_details_score': None,
                     'top.with.dot': 'second', 'id': 2},
                    {'payload_details_score': None,
                     'top.with.dot': 'third', 'id': 3},
                ]
                if include_map:
                    for expected_row, value in zip(expected, [10, None, None]):
                        expected_row['attrs_selected'] = value
                if not include_dotted_name:
                    for expected_row in expected:
                        expected_row.pop('top.with.dot')
                name = 'native_nested_' + file_format
                options = {'file.format': file_format, 'bucket': '-1'}
                if file_format == 'parquet':
                    options.update({
                        'fields.attrs.map.storage-layout': 'shared-shredding',
                        'fields.attrs.map.shared-shredding.max-columns': '2',
                    })
                self.cat.create_table(
                    'default.' + name,
                    Schema.from_pyarrow_schema(
                        nested_schema, options=options),
                    False,
                )
                table = self.cat.get_table('default.' + name)
                write_builder = table.new_batch_write_builder()
                write = write_builder.new_write()
                write.write_arrow(pa.Table.from_pylist(rows, schema=nested_schema))
                write_builder.new_commit().commit(write.prepare_commit())
                write.close()

                native_table = table.copy({
                    'scan.native-plan.enabled': 'true',
                    'read.native.enabled': 'true',
                })
                builder = native_table.new_read_builder().with_projection(
                    projection)
                plan = builder.new_scan().plan()
                with patch(
                        'pypaimon.read.table_read.TableRead._create_split_read',
                        side_effect=AssertionError(
                            'nested native read fell back to Python')):
                    actual = builder.new_read().to_arrow(
                        plan.splits(), parallelism=2).to_pylist()

                self.assertEqual(actual, expected)

    @unittest.skipUnless(
        native_method_available('ReadBuilder', 'with_nested_projection'),
        "pypaimon-rust nested native reader API not installed")
    def test_native_read_nested_projection_after_primary_key_merge(self):
        nested_schema = pa.schema([
            ('id', pa.int64()),
            ('payload', pa.struct([
                ('score', pa.int32()),
                ('ignored', pa.string()),
            ])),
        ])
        self.cat.create_table(
            'default.native_nested_pk',
            Schema.from_pyarrow_schema(
                nested_schema, primary_keys=['id'], options={'bucket': '1'}),
            False,
        )
        table = self.cat.get_table('default.native_nested_pk')
        for rows in (
            [{'id': 1, 'payload': {'score': 1, 'ignored': 'old'}},
             {'id': 2, 'payload': {'score': 2, 'ignored': 'keep'}}],
            [{'id': 1, 'payload': {'score': 10, 'ignored': 'new'}}],
        ):
            write_builder = table.new_batch_write_builder()
            write = write_builder.new_write()
            write.write_arrow(pa.Table.from_pylist(rows, schema=nested_schema))
            write_builder.new_commit().commit(write.prepare_commit())
            write.close()

        native_table = table.copy({
            'scan.native-plan.enabled': 'true',
            'read.native.enabled': 'true',
        })
        builder = native_table.new_read_builder().with_projection(
            ['id', 'payload.score'])
        plan = builder.new_scan().plan()
        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError('nested PK native read fell back')):
            actual = sorted(
                builder.new_read().to_arrow(plan.splits()).to_pylist(),
                key=lambda row: row['id'])

        self.assertEqual(actual, [
            {'id': 1, 'payload_score': 10},
            {'id': 2, 'payload_score': 2},
        ])

    @unittest.skipUnless(
        native_method_available('ReadBuilder', 'with_nested_projection'),
        "pypaimon-rust nested native reader API not installed")
    def test_native_nested_projection_after_partial_update_merge(self):
        nested_schema = pa.schema([
            ('id', pa.int64()),
            ('payload', pa.struct([
                ('score', pa.int32()),
                ('ignored', pa.string()),
            ])),
        ])
        self.cat.create_table(
            'default.native_nested_partial_update',
            Schema.from_pyarrow_schema(
                nested_schema,
                primary_keys=['id'],
                options={'bucket': '1', 'merge-engine': 'partial-update'},
            ),
            False,
        )
        table = self.cat.get_table('default.native_nested_partial_update')
        for rows in (
            [{'id': 1, 'payload': {'score': 1, 'ignored': 'old'}},
             {'id': 2, 'payload': {'score': 2, 'ignored': 'keep'}}],
            [{'id': 1, 'payload': {'score': 10, 'ignored': 'new'}}],
        ):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_write.write_arrow(pa.Table.from_pylist(rows, schema=nested_schema))
            write_builder.new_commit().commit(table_write.prepare_commit())
            table_write.close()

        native_table = table.copy({
            'scan.native-plan.enabled': 'true',
            'read.native.enabled': 'true',
        })
        builder = native_table.new_read_builder().with_projection(
            ['id', 'payload.score'])
        plan = builder.new_scan().plan()
        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError(
                    'nested partial-update native read fell back')):
            actual = sorted(
                builder.new_read().to_arrow(plan.splits()).to_pylist(),
                key=lambda row: row['id'])

        self.assertEqual(actual, [
            {'id': 1, 'payload_score': 10},
            {'id': 2, 'payload_score': 2},
        ])

    @unittest.skipUnless(
        native_method_available('ReadBuilder', 'with_nested_projection'),
        "pypaimon-rust nested native reader API not installed")
    def test_native_nested_projection_across_schema_rename_filter_and_limit(self):
        name = 'default.native_nested_evolution'
        old_schema = pa.schema([
            ('id', pa.int64()),
            ('payload', pa.struct([
                ('old_score', pa.int32()),
                ('ignored', pa.string()),
            ])),
        ])
        self.cat.create_table(
            name,
            Schema.from_pyarrow_schema(old_schema, options={
                'bucket': '-1',
                'data-evolution.enabled': 'true',
                'row-tracking.enabled': 'true',
                'file.format': 'parquet',
            }),
            False,
        )

        def write(table, schema, rows):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_write.write_arrow(pa.Table.from_pylist(rows, schema=schema))
            write_builder.new_commit().commit(table_write.prepare_commit())
            table_write.close()

        table = self.cat.get_table(name)
        write(table, old_schema, [
            {'id': 1, 'payload': {'old_score': 10, 'ignored': 'old-1'}},
            {'id': 2, 'payload': {'old_score': 20, 'ignored': 'old-2'}},
        ])
        self.cat.alter_table(
            name,
            [SchemaChange.rename_column(['payload', 'old_score'], 'score')],
            False,
        )
        new_schema = pa.schema([
            ('id', pa.int64()),
            ('payload', pa.struct([
                ('score', pa.int32()),
                ('ignored', pa.string()),
            ])),
        ])
        table = self.cat.get_table(name)
        write(table, new_schema, [
            {'id': 3, 'payload': {'score': 30, 'ignored': 'new-3'}},
            {'id': 4, 'payload': None},
        ])

        native_table = table.copy({
            'scan.native-plan.enabled': 'true',
            'read.native.enabled': 'true',
            'read.parallelism': '3',
        })
        builder = native_table.new_read_builder().with_projection(
            ['payload.score', 'id'])
        predicate = builder.new_predicate_builder().greater_or_equal('id', 2)
        builder.with_filter(predicate)
        plan = builder.new_scan().plan()
        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError(
                    'evolved nested native read fell back to Python')):
            actual = sorted(
                builder.new_read().to_arrow(
                    plan.splits(), parallelism=3).to_pylist(),
                key=lambda row: row['id'],
            )

        self.assertEqual(actual, [
            {'payload_score': 20, 'id': 2},
            {'payload_score': 30, 'id': 3},
            {'payload_score': None, 'id': 4},
        ])

        limited = native_table.new_read_builder().with_projection(
            ['payload.score', 'id']).with_limit(2)
        limited_plan = limited.new_scan().plan()
        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError(
                    'limited nested native read fell back to Python')):
            limited_rows = limited.new_read().to_arrow(
                limited_plan.splits(), parallelism=3).to_pylist()
        self.assertEqual(len(limited_rows), 2)
        self.assertTrue(all(set(row) == {'payload_score', 'id'}
                            for row in limited_rows))

    def test_append_distribution_matches_interleaved_partition_buckets(self):
        self.schema = pa.schema([('k', pa.int64()), ('v', pa.string()), ('p', pa.string())])
        self.cat.create_table('default.interleaved_t', Schema.from_pyarrow_schema(
            self.schema, partition_keys=['p'], options={'bucket': '5', 'bucket-key': 'k'}), False)
        partitions = ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p1']
        self._write('interleaved_t', [
            {'k': 1001 + i, 'v': 'first', 'p': partition} for i, partition in enumerate(partitions)])
        self._write('interleaved_t', [
            {'k': 1005 + i, 'v': 'second', 'p': partition}
            for i, partition in enumerate(['p2', 'p1', 'p2', 'p2'])])

        def read(native, shard=None, slice_=None, limit=None):
            table = self.cat.get_table('default.interleaved_t').copy(
                {'scan.native-plan.enabled': str(native).lower()})
            builder = table.new_read_builder()
            if limit is not None:
                builder.with_limit(limit)
            scan = builder.new_scan()
            if shard is not None:
                scan.with_shard(*shard)
            if slice_ is not None:
                scan.with_slice(*slice_)
            if native:
                with patch.object(scan.file_scanner, 'scan', side_effect=AssertionError('native plan fell back')):
                    plan = scan.plan()
            else:
                plan = scan.plan()
            # Parallel reads with a limit can return any subset; compare planned order serially.
            rows = builder.new_read().to_arrow(plan.splits(), parallelism=1).to_pylist()
            return plan.snapshot_id, sorted(rows, key=lambda row: (row['k'], row['v'], row['p']))

        selections = [{'shard': (i, 3)} for i in range(3)] + [
            {'slice_': (0, 6)}, {'slice_': (4, 10)}, {'slice_': (10, 18)},
            {'slice_': (0, 99)}, {'shard': (1, 3), 'limit': 2}, {'slice_': (4, 10), 'limit': 2},
        ]
        for selection in selections:
            with self.subTest(selection=selection):
                self.assertEqual(read(False, **selection), read(True, **selection))

    @unittest.skipUnless(native_family_search_modes_available(),
                         "pypaimon-rust 0.4+ required")
    def test_dynamic_family_search_mode_uses_native_plan(self):
        self.cat.create_table(
            'default.search_mode_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('search_mode_t', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])

        table = self.cat.get_table('default.search_mode_t').copy({
            'scan.native-plan.enabled': 'true',
            'scalar-index.search-mode': 'full',
        })
        builder = table.new_read_builder()
        plan = builder.new_scan().plan()

        self.assertEqual(
            sorted(builder.new_read().to_arrow(plan.splits()).to_pylist(),
                   key=lambda row: row['k']),
            [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}],
        )
        self.assertTrue(builder.explain().native_planned)

    def test_data_evolution_blob_projection_filter_limit(self):
        schema = pa.schema([
            ('k', pa.int64()),
            ('v', pa.string()),
            ('media.camera', pa.large_binary()),
        ])
        self.cat.create_table('default.de_t', Schema.from_pyarrow_schema(
            schema, options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
            }), False)
        table = self.cat.get_table('default.de_t')
        write_builder = table.new_batch_write_builder()
        write = write_builder.new_write()
        write.write_arrow(pa.Table.from_pylist([
            {'k': 1, 'v': 'a', 'media.camera': b'a'},
            {'k': 2, 'v': 'b', 'media.camera': b'b'},
            {'k': 3, 'v': 'c', 'media.camera': b'c'},
        ], schema=schema))
        write_builder.new_commit().commit(write.prepare_commit())
        write.close()

        update_builder = table.new_batch_write_builder()
        update = update_builder.new_update().with_update_type(['v'])
        messages = update.update_by_arrow_with_row_id(pa.Table.from_pydict({
            '_ROW_ID': pa.array([1], type=pa.int64()),
            'v': pa.array(['b2'], type=pa.string()),
        }))
        update_builder.new_commit().commit(messages)

        self._assert_matches('de_t')

        native_table = self.cat.get_table('default.de_t').copy(
            {'read.native.enabled': 'true'})
        predicate = native_table.new_read_builder().new_predicate_builder().equal(
            'v', 'b2')
        builder = (native_table.new_read_builder()
                   .with_projection(['k'])
                   .with_filter(predicate)
                   .with_limit(1))
        plan = builder.new_scan().plan()
        rows = self._native_rows(builder, plan)

        self.assertEqual(rows, [{'k': 2}])
        self.assertTrue(builder.explain().native_planned)

        blob_builder = (native_table.new_read_builder()
                        .with_projection(['media.camera'])
                        .with_limit(1))
        blob_plan = blob_builder.new_scan().plan()
        # A dotted top-level field currently uses PyPaimon's nested-projection
        # machinery, so this part deliberately exercises the documented
        # Python fallback while the scalar projection above is native.
        blob_rows = blob_builder.new_read().to_arrow(
            blob_plan.splits()).to_pylist()
        self.assertEqual(blob_rows, [{'media.camera': b'a'}])
        self.assertTrue(any(
            data_file.file_name.endswith('.blob')
            for split in blob_plan.splits()
            for data_file in split.files
        ))

        descriptor_table = native_table.copy({'blob-as-descriptor': 'true'})
        descriptor_builder = (
            descriptor_table.new_read_builder()
            .with_projection(['media.camera'])
            .with_limit(1))
        descriptor_plan = descriptor_builder.new_scan().plan()
        descriptor_rows = descriptor_builder.new_read().to_arrow(
            descriptor_plan.splits()).to_pylist()
        self.assertEqual(len(descriptor_plan.splits()), 1)
        self.assertEqual(
            BlobDescriptor.deserialize(descriptor_rows[0]['media.camera']).length,
            1,
        )
        self.assertTrue(descriptor_builder.explain().native_planned)

    @unittest.skipUnless(native_reader_available(),
                         "pypaimon-rust native reader API not installed")
    def test_native_read_data_evolution_partial_appends(self):
        schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string()),
        ])
        self.cat.create_table('default.de_order_t', Schema.from_pyarrow_schema(
            schema, options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
            }), False)
        table = self.cat.get_table('default.de_order_t')
        write_builder = table.new_batch_write_builder()

        base = write_builder.new_write()
        base.write_arrow(pa.Table.from_pydict({
            'f0': [0, 1],
            'f1': ['z', 'a'],
            'f2': ['q', 'b'],
        }, schema=schema))
        write_builder.new_commit().commit(base.prepare_commit())
        base.close()

        left = write_builder.new_write().with_write_type(['f0', 'f1'])
        right = write_builder.new_write().with_write_type(['f2'])
        left.write_arrow(pa.Table.from_pydict(
            {'f0': [2], 'f1': ['x']},
            schema=pa.schema([('f0', pa.int32()), ('f1', pa.string())])))
        right.write_arrow(pa.Table.from_pydict(
            {'f2': ['y']}, schema=pa.schema([('f2', pa.string())])))
        messages = left.prepare_commit() + right.prepare_commit()
        for message in messages:
            for data_file in message.new_files:
                data_file.first_row_id = 2
        write_builder.new_commit().commit(messages)
        left.close()
        right.close()

        native_table = table.copy({'read.native.enabled': 'true'})
        builder = native_table.new_read_builder()
        plan = builder.new_scan().plan()
        rows = self._native_rows(builder, plan)

        self.assertEqual(sorted(rows, key=lambda row: row['f0']), [
            {'f0': 0, 'f1': 'z', 'f2': 'q'},
            {'f0': 1, 'f1': 'a', 'f2': 'b'},
            {'f0': 2, 'f1': 'x', 'f2': 'y'},
        ])
        self.assertTrue(builder.explain().native_planned)

    @unittest.skipUnless(native_reader_available(),
                         "pypaimon-rust native reader API not installed")
    def test_native_read_data_evolution_blob_parallelism(self):
        schema = pa.schema([
            ('id', pa.int32()),
            ('img', pa.large_binary()),
        ])
        self.cat.create_table('default.native_blob_t', Schema.from_pyarrow_schema(
            schema, options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
            }), False)
        table = self.cat.get_table('default.native_blob_t')
        payloads = [b'a', b'bb', b'ccc']
        write = table.new_batch_write_builder().new_write()
        write.write_arrow(pa.Table.from_pydict({
            'id': [1, 2, 3],
            'img': payloads,
        }, schema=schema))
        table.new_batch_write_builder().new_commit().commit(write.prepare_commit())
        write.close()

        native_table = table.copy({'read.native.enabled': 'true'})
        builder = native_table.new_read_builder().with_projection(['id', 'img'])
        plan = builder.new_scan().plan()
        self.assertTrue(any(
            data_file.file_name.endswith('.blob')
            for split in plan.splits()
            for data_file in split.files
        ))

        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError('Python reader was used')):
            table_result = builder.new_read().to_arrow(
                plan.splits(), parallelism=1, blob_parallelism=3)
            batch_table = builder.new_read().to_arrow_batch_reader(
                plan.splits(), blob_parallelism=2).read_all()

        expected = {'id': [1, 2, 3], 'img': payloads}
        self.assertEqual(table_result.schema, schema)
        self.assertEqual(batch_table.schema, schema)
        self.assertEqual(table_result.to_pydict(), expected)
        self.assertEqual(batch_table.to_pydict(), expected)
        self.assertTrue(builder.explain().native_planned)

    @unittest.skipUnless(native_reader_available(),
                         "pypaimon-rust native reader API not installed")
    def test_native_read_data_evolution_nested_blobs(self):
        schema = pa.schema([
            ('id', pa.int32()),
            ('images', pa.list_(pa.large_binary())),
            ('attributes', pa.map_(pa.string(), pa.large_binary())),
        ])
        self.cat.create_table(
            'default.native_nested_blob_t',
            Schema.from_pyarrow_schema(schema, options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
            }),
            False,
        )
        table = self.cat.get_table('default.native_nested_blob_t')
        expected = {
            'id': [1, 2, 3],
            'images': [[b'a', None, b'ccc'], [], None],
            'attributes': [
                [('left', b'x'), ('right', None)],
                [],
                None,
            ],
        }
        write = table.new_batch_write_builder().new_write()
        write.write_arrow(pa.Table.from_pydict(expected, schema=schema))
        table.new_batch_write_builder().new_commit().commit(
            write.prepare_commit())
        write.close()

        native_table = table.copy({'read.native.enabled': 'true'})
        builder = native_table.new_read_builder()
        plan = builder.new_scan().plan()
        self.assertTrue(any(
            data_file.file_name.endswith('.blob')
            for split in plan.splits()
            for data_file in split.files
        ))

        with patch(
                'pypaimon.read.table_read.TableRead._create_split_read',
                side_effect=AssertionError('Python reader was used')):
            result = builder.new_read().to_arrow(
                plan.splits(), parallelism=1, blob_parallelism=3)
            streamed = builder.new_read().to_arrow_batch_reader(
                plan.splits(), blob_parallelism=2).read_all()

        self.assertEqual(result.schema, schema)
        self.assertEqual(streamed.schema, schema)
        self.assertEqual(result.to_pydict(), expected)
        self.assertEqual(streamed.to_pydict(), expected)
        self.assertTrue(builder.explain().native_planned)

    @unittest.skipUnless(native_reader_available(),
                         "pypaimon-rust native reader API not installed")
    def test_native_read_pruning_limit_defers_blob_payload_io(self):
        schema = pa.schema([
            ('id', pa.int32()),
            ('payload', pa.large_binary()),
        ])
        self.cat.create_table('default.native_blob_limit_t',
                              Schema.from_pyarrow_schema(schema, options={
                                  'row-tracking.enabled': 'true',
                                  'data-evolution.enabled': 'true',
                              }), False)
        table = self.cat.get_table('default.native_blob_limit_t')
        write = table.new_batch_write_builder().new_write()
        write.write_arrow(pa.Table.from_pydict({
            'id': [1, 2, 3],
            'payload': [b'a', b'bb', b'ccc'],
        }, schema=schema))
        table.new_batch_write_builder().new_commit().commit(write.prepare_commit())
        write.close()

        native_table = table.copy({'read.native.enabled': 'true'})
        builder = native_table.new_read_builder().with_limit(1)
        plan = builder.new_scan().plan()
        fetched = []
        original_to_data = BlobRef.to_data

        def tracked_to_data(blob):
            fetched.append(blob)
            return original_to_data(blob)

        with patch.object(BlobRef, 'to_data', tracked_to_data), patch(
                'pypaimon.read.native_plan.native_read',
                return_value=[]) as native:
            result = builder.new_read().to_arrow(
                plan.splits(), parallelism=1)

        native.assert_not_called()
        self.assertEqual(result.to_pydict(), {'id': [1], 'payload': [b'a']})
        self.assertEqual(len(fetched), 1)

    @unittest.skipUnless(native_reader_available(),
                         "pypaimon-rust native reader API not installed")
    def test_native_read_pruning_limit_defers_descriptor_blob_payload_io(self):
        schema = pa.schema([
            ('id', pa.int32()),
            ('payload', pa.large_binary()),
        ])
        with tempfile.TemporaryDirectory() as payload_dir:
            first_path = os.path.join(payload_dir, 'first')
            with open(first_path, 'wb') as output:
                output.write(b'first')
            missing_path = os.path.join(payload_dir, 'missing')
            descriptors = [
                BlobDescriptor(
                    'file://' + first_path, 0, 5).serialize(),
                BlobDescriptor(
                    'file://' + missing_path, 0, 7).serialize(),
            ]

            self.cat.create_table(
                'default.native_descriptor_limit_t',
                Schema.from_pyarrow_schema(schema, options={
                    'row-tracking.enabled': 'true',
                    'data-evolution.enabled': 'true',
                    'blob-descriptor-field': 'payload',
                }), False)
            table = self.cat.get_table('default.native_descriptor_limit_t')
            write = table.new_batch_write_builder().new_write()
            write.write_arrow(pa.Table.from_pydict({
                'id': [1, 2],
                'payload': descriptors,
            }, schema=schema))
            table.new_batch_write_builder().new_commit().commit(
                write.prepare_commit())
            write.close()

            native_table = table.copy({'read.native.enabled': 'true'})
            builder = native_table.new_read_builder().with_limit(1)
            plan = builder.new_scan().plan()
            fetched = []
            original_to_data = BlobRef.to_data

            def tracked_to_data(blob):
                fetched.append(blob)
                return original_to_data(blob)

            with patch.object(BlobRef, 'to_data', tracked_to_data), patch(
                    'pypaimon.read.native_plan.native_read',
                    return_value=[]) as native:
                result = builder.new_read().to_arrow(plan.splits())

            native.assert_not_called()
            self.assertEqual(
                result.to_pydict(), {'id': [1], 'payload': [b'first']})
            self.assertEqual(len(fetched), 1)

    @unittest.skipUnless(native_reader_available(),
                         "pypaimon-rust native reader API not installed")
    def test_native_read_supports_precision_zero_timestamps(self):
        cases = [
            ('timestamp', pa.timestamp('s'), datetime.datetime(1970, 1, 1)),
            ('timestamp_ltz', pa.timestamp('s', tz='UTC'),
             datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)),
        ]
        for name, timestamp_type, first_value in cases:
            with self.subTest(name=name):
                schema = pa.schema([
                    ('id', pa.int32()),
                    ('ts', timestamp_type),
                ])
                table_name = 'native_%s_zero_t' % name
                self.cat.create_table(
                    'default.%s' % table_name,
                    Schema.from_pyarrow_schema(schema), False)
                table = self.cat.get_table('default.%s' % table_name)
                write = table.new_batch_write_builder().new_write()
                write.write_arrow(pa.Table.from_pydict({
                    'id': [1, 2],
                    'ts': [first_value, first_value],
                }, schema=schema))
                table.new_batch_write_builder().new_commit().commit(
                    write.prepare_commit())
                write.close()

                native_table = table.copy({'read.native.enabled': 'true'})
                builder = native_table.new_read_builder()
                plan = builder.new_scan().plan()
                with patch(
                        'pypaimon.read.native_plan.native_read',
                        wraps=native_read) as native, patch(
                        'pypaimon.read.table_read.TableRead._create_split_read',
                        side_effect=AssertionError('Python reader was used')):
                    result = builder.new_read().to_arrow(plan.splits())

                native.assert_called_once()
                self.assertEqual(result.schema, schema)
                self.assertEqual(result.num_rows, 2)

    @unittest.skipUnless(_has_native_row_ranges(),
                         "pypaimon_rust row-range API not installed")
    def test_data_evolution_global_index_row_ranges(self):
        self.cat.create_table('default.de_range_t', Schema.from_pyarrow_schema(
            self.schema, options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
            }), False)
        self._write('de_range_t', [
            {'k': 1, 'v': 'a'},
            {'k': 2, 'v': 'b'},
            {'k': 3, 'v': 'c'},
        ])
        table = self.cat.get_table('default.de_range_t').copy(
            {'read.native.enabled': 'true'})
        builder = table.new_read_builder()
        scan = builder.new_scan().with_global_index_result(
            GlobalIndexResult.from_range(Range(1, 1)))

        self.assertTrue(scan._native_plan_supported())
        with patch.object(
                scan.file_scanner, 'scan', side_effect=AssertionError("fallback")):
            plan = scan.plan()
        rows = self._native_rows(builder, plan)

        self.assertEqual(rows, [{'k': 2, 'v': 'b'}])
        self.assertEqual(
            [(range_.from_, range_.to)
             for range_ in plan.splits()[0].row_ranges()],
            [(1, 1)],
        )

        empty_scan = builder.new_scan().with_global_index_result(
            GlobalIndexResult.create_empty())
        with patch.object(
                empty_scan.file_scanner, 'scan',
                side_effect=AssertionError("fallback")):
            empty_plan = empty_scan.plan()
        self.assertEqual(empty_plan.splits(), [])

    def test_filter_is_pushed_to_native_plan(self):
        options = {
            'source.split.target-size': '1b',
            'source.split.open-file-cost': '1b',
        }
        self.cat.create_table('default.filter_t', Schema.from_pyarrow_schema(
            self.schema, options=options), False)
        for k in range(1, 4):
            self._write('filter_t', [{'k': k, 'v': 'v%d' % k}])

        table = self.cat.get_table('default.filter_t')
        normal_builder = table.new_read_builder()
        predicate = normal_builder.new_predicate_builder().equal('k', 2)
        normal_builder.with_filter(predicate)
        normal_plan = normal_builder.new_scan().plan()

        native_builder = table.copy(
            {'scan.native-plan.enabled': 'true'}).new_read_builder()
        predicate = native_builder.new_predicate_builder().equal('k', 2)
        native_builder.with_filter(predicate)
        native_plan = native_builder.new_scan().plan()
        rows = native_builder.new_read().to_arrow(native_plan.splits()).to_pylist()

        self.assertEqual(rows, [{'k': 2, 'v': 'v2'}])
        self.assertEqual(len(native_plan.splits()), len(normal_plan.splits()))
        self.assertTrue(native_builder.explain().native_planned)

    def test_limit_is_pushed_to_native_plan(self):
        options = {
            'source.split.target-size': '1b',
            'source.split.open-file-cost': '1b',
        }
        self.cat.create_table('default.limit_t', Schema.from_pyarrow_schema(
            self.schema, options=options), False)
        for k in range(1, 4):
            self._write('limit_t', [{'k': k, 'v': 'v%d' % k}])

        table = self.cat.get_table('default.limit_t')
        normal = table.new_read_builder().with_limit(1).new_scan().plan()
        native_builder = table.copy(
            {'scan.native-plan.enabled': 'true'}).new_read_builder().with_limit(1)
        native = native_builder.new_scan().plan()
        rows = native_builder.new_read().to_arrow(native.splits()).to_pylist()

        self.assertEqual(len(rows), 1)
        self.assertEqual(len(native.splits()), len(normal.splits()))
        self.assertEqual(len(native.splits()), 1)
        self.assertTrue(native_builder.explain().native_planned)

    def test_snapshot_time_travel_matches_normal_plan(self):
        self.cat.create_table(
            'default.travel_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('travel_t', [{'k': 1, 'v': 'a'}])
        self._write('travel_t', [{'k': 2, 'v': 'b'}])
        options = {'scan.snapshot-id': '1'}

        normal_table = self.cat.get_table('default.travel_t').copy(options)
        normal_builder = normal_table.new_read_builder()
        normal_plan = normal_builder.new_scan().plan()
        normal_rows = normal_builder.new_read().to_arrow(
            normal_plan.splits()).to_pylist()

        native_table = normal_table.copy({'scan.native-plan.enabled': 'true'})
        native_builder = native_table.new_read_builder()
        native_plan = native_builder.new_scan().plan()
        native_rows = native_builder.new_read().to_arrow(
            native_plan.splits()).to_pylist()

        self.assertEqual(native_plan.snapshot_id, 1)
        self.assertEqual(native_rows, normal_rows)
        self.assertEqual(native_rows, [{'k': 1, 'v': 'a'}])
        self.assertTrue(native_builder.explain().native_planned)

    def test_dynamic_split_target_size_matches_normal_plan(self):
        self.cat.create_table(
            'default.split_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('split_t', [{'k': 1, 'v': 'a'}])
        self._write('split_t', [{'k': 2, 'v': 'b'}])
        options = {'source.split.target-size': '1b'}
        normal_table = self.cat.get_table('default.split_t').copy(options)
        native_table = normal_table.copy({'scan.native-plan.enabled': 'true'})

        normal = normal_table.new_read_builder().new_scan().plan()
        native = native_table.new_read_builder().explain()

        self.assertTrue(native.native_planned)
        self.assertEqual(native.split_count, len(normal.splits()))
        self.assertGreater(native.split_count, 1)

    def test_dynamic_split_open_file_cost_matches_normal_plan(self):
        stored_options = {
            'source.split.target-size': '128mb',
            'source.split.open-file-cost': '1b',
        }
        self.cat.create_table('default.open_cost_t', Schema.from_pyarrow_schema(
            self.schema, options=stored_options), False)
        self._write('open_cost_t', [{'k': 1, 'v': 'a'}])
        self._write('open_cost_t', [{'k': 2, 'v': 'b'}])
        self._write('open_cost_t', [{'k': 3, 'v': 'c'}])
        base_table = self.cat.get_table('default.open_cost_t')
        normal_table = base_table.copy({'source.split.open-file-cost': '64mb'})
        native_table = normal_table.copy({'scan.native-plan.enabled': 'true'})

        baseline = base_table.new_read_builder().new_scan().plan()
        normal = normal_table.new_read_builder().new_scan().plan()
        native = native_table.new_read_builder().explain()

        self.assertEqual(len(baseline.splits()), 1)
        self.assertGreater(len(normal.splits()), len(baseline.splits()))
        self.assertTrue(native.native_planned)
        self.assertEqual(native.split_count, len(normal.splits()))

    def test_dynamic_split_option_reset_matches_normal_plan(self):
        stored_options = {
            'source.split.target-size': '1b',
            'source.split.open-file-cost': '1b',
        }
        self.cat.create_table('default.split_reset_t', Schema.from_pyarrow_schema(
            self.schema, options=stored_options), False)
        self._write('split_reset_t', [{'k': 1, 'v': 'a'}])
        self._write('split_reset_t', [{'k': 2, 'v': 'b'}])
        reset_options = {
            'source.split.target-size': None,
            'source.split.open-file-cost': None,
        }
        normal_table = self.cat.get_table('default.split_reset_t').copy(reset_options)
        native_table = normal_table.copy({'scan.native-plan.enabled': 'true'})

        normal = normal_table.new_read_builder().new_scan().plan()
        native = native_table.new_read_builder().explain()

        self.assertTrue(native.native_planned)
        self.assertEqual(native.split_count, len(normal.splits()))
        self.assertEqual(native.split_count, 1)

    def test_partitioned_table_matches_normal_plan(self):
        # Native decoding restores PyPaimon's legacy unescaped partition path.
        schema = pa.schema([('k', pa.int64()), ('p', pa.string())])
        self.cat.create_table('default.pt_t', Schema.from_pyarrow_schema(
            schema, partition_keys=['p']), False)
        t = self.cat.get_table('default.pt_t')
        wb = t.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        w.write_arrow(pa.Table.from_pylist(
            [{'k': 1, 'p': 'a/b'}, {'k': 2, 'p': 'a/b'}, {'k': 3, 'p': 'c'}],
            schema=schema))
        c.commit(w.prepare_commit())
        w.close()
        c.close()

        self._assert_matches('pt_t')

        native_table = t.copy({
            'scan.native-plan.enabled': 'true',
            'read.native.enabled': 'true',
        })
        builder = native_table.new_read_builder()
        plan = builder.new_scan().plan()
        with patch('pypaimon.read.native_plan.native_read',
                   wraps=native_read) as read:
            rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
        self.assertEqual(read.call_count, len(plan.splits()))
        self.assertEqual(sorted(rows, key=lambda row: row['k']), [
            {'k': 1, 'p': 'a/b'},
            {'k': 2, 'p': 'a/b'},
            {'k': 3, 'p': 'c'},
        ])

    def test_explain_reflects_native_plan(self):
        self.cat.create_table(
            'default.ex_t', Schema.from_pyarrow_schema(self.schema), False)
        self._write('ex_t', [{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}])
        normal = self.cat.get_table('default.ex_t').new_read_builder().explain()
        native = self.cat.get_table('default.ex_t').copy(
            {'scan.native-plan.enabled': 'true'}).new_read_builder().explain()
        self.assertFalse(normal.native_planned)
        self.assertTrue(native.native_planned)
        self.assertEqual(native.split_count, normal.split_count)
        self.assertEqual(native.snapshot_id, normal.snapshot_id)
        self.assertIn('native', str(native))   # render shows the Planner line

    @unittest.skipUnless(native_method_available('Plan', 'snapshot_id'),
                         "pypaimon_rust snapshot metadata API not installed")
    def test_native_explain_reports_split_metadata_without_pruning_counters(self):
        self.cat.create_table(
            'default.explain_metadata_t', Schema.from_pyarrow_schema(
                self.schema, options={'metadata.stats-mode': 'full'}), False)
        self._write('explain_metadata_t', [{'k': 1, 'v': 'a'}])
        self._write('explain_metadata_t', [{'k': 8, 'v': 'b'}])
        table = self.cat.get_table('default.explain_metadata_t').copy(
            {'scan.native-plan.enabled': 'true'})
        builder = table.new_read_builder()
        builder.with_filter(builder.new_predicate_builder().equal('k', 8))
        result = builder.explain()
        self.assertTrue(result.native_planned)
        self.assertEqual(result.snapshot_id, 2)
        self.assertEqual(result.split_count, 1)
        self.assertEqual(result.file_count, 1)
        self.assertIn('pruning not tracked', str(result))
        self.assertIsNone(result.file_skipping)
        builder.with_filter(builder.new_predicate_builder().equal('k', 99))
        empty = builder.explain()
        self.assertTrue(empty.native_planned)
        self.assertEqual(empty.snapshot_id, 2)
        self.assertEqual(empty.split_count, 0)

    def test_empty_table_explain_preserves_native_metadata(self):
        self.cat.create_table(
            'default.empty_t', Schema.from_pyarrow_schema(self.schema), False)
        normal = self.cat.get_table('default.empty_t').new_read_builder().explain()
        native = self.cat.get_table('default.empty_t').copy(
            {'scan.native-plan.enabled': 'true'}).new_read_builder().explain()

        self.assertEqual(native.native_planned,
                         native_method_available('Plan', 'snapshot_id'))
        self.assertEqual(native.snapshot_id, normal.snapshot_id)
        self.assertEqual(native.split_count, 0)
        self.assertEqual('Planner:' in str(native), native.native_planned)


if __name__ == '__main__':
    unittest.main()
