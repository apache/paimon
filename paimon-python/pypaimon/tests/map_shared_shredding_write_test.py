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

import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon import CatalogFactory, Schema
from pypaimon.data.map_shared_shredding import (
    is_shared_shredding,
    parse_shared_shredding_metadata,
)


class MapSharedShreddingWriteTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.catalog = CatalogFactory.create({"warehouse": self.temp_dir})
        self.catalog.create_database("default", True)
        self.arrow_schema = pa.schema([
            pa.field("id", pa.int32()),
            pa.field("metrics", pa.map_(pa.string(), pa.int64())),
        ])

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_write_and_read_parquet(self):
        expected = [
            [("hot", 10), ("warm", 20), ("overflow", 30)],
            [("hot", None), ("new", 40)],
            [],
            None,
        ]
        data = pa.Table.from_pydict({
            "id": [1, 2, 3, 4],
            "metrics": expected,
        }, schema=self.arrow_schema)

        table = self._create_table("parquet", max_columns=2)
        messages = self._write(table, data)

        physical_field = pq.read_schema(
            messages[0].new_files[0].file_path).field("metrics")
        self.assertTrue(pa.types.is_struct(physical_field.type))
        self.assertTrue(is_shared_shredding(physical_field))
        name_by_id, num_columns = \
            parse_shared_shredding_metadata(physical_field)
        self.assertEqual(2, num_columns)
        self.assertEqual(
            {"hot", "warm", "overflow", "new"},
            set(name_by_id.values()),
        )

        read_builder = table.new_read_builder()
        result = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits())
        self.assertEqual(expected, result.column("metrics").to_pylist())

        selected = table.new_read_builder().with_projection([
            "id", "metrics['hot']", "metrics['overflow']",
        ])
        result = selected.new_read().to_arrow(
            selected.new_scan().plan().splits())
        self.assertEqual(
            [10, None, None, None],
            result.column("metrics_hot").to_pylist(),
        )
        self.assertEqual(
            [30, None, None, None],
            result.column("metrics_overflow").to_pylist(),
        )

    def test_row_id_update_preserves_shared_shredding(self):
        table = self._create_table('parquet', 2, {
            'data-evolution.enabled': 'true',
            'row-tracking.enabled': 'true',
        })
        self._write(table, pa.Table.from_pydict({
            'id': [1, 2],
            'metrics': [[('hot', 1)], [('warm', 2)]],
        }, schema=self.arrow_schema))

        read_builder = table.new_read_builder().with_projection(['id', '_ROW_ID'])
        rows = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits())
        row_ids = dict(zip(rows.column('id').to_pylist(),
                           rows.column('_ROW_ID').to_pylist()))
        row_id = row_ids[2]
        update = pa.Table.from_pydict({
            '_ROW_ID': [row_id],
            'metrics': [[('hot', 99), ('new', 7)]],
        }, schema=pa.schema([
            ('_ROW_ID', pa.int64()),
            self.arrow_schema.field('metrics'),
        ]))

        builder = table.new_batch_write_builder()
        messages = builder.new_update().with_update_type(
            ['metrics']).update_by_arrow_with_row_id(update)
        self.assertEqual(1, len(messages[0].new_files))
        overlay = messages[0].new_files[0]
        self.assertTrue(is_shared_shredding(
            pq.read_schema(overlay.file_path).field('metrics')))
        commit = builder.new_commit()
        commit.commit(messages)
        commit.close()

        read_builder = table.new_read_builder().with_projection(['id', 'metrics'])
        result = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits())
        self.assertEqual(
            {1: [('hot', 1)], 2: [('hot', 99), ('new', 7)]},
            dict(zip(result.column('id').to_pylist(),
                     result.column('metrics').to_pylist())),
        )

    def test_reject_orc(self):
        with self.assertRaisesRegex(
                ValueError,
                "PyPaimon MAP shared-shredding writes only support parquet"):
            writer = self._create_table(
                "orc", max_columns=2).new_batch_write_builder().new_write()
            writer.write_arrow(pa.Table.from_pydict({
                "id": [1],
                "metrics": [[("key", 1)]],
            }, schema=self.arrow_schema))

    def test_non_nullable_values(self):
        for value_type, value in [
                (pa.int64(), 1),
                (pa.struct([pa.field('score', pa.int64(), nullable=False)]),
                 {'score': 1})]:
            with self.subTest(value_type=value_type):
                self.arrow_schema = pa.schema([
                    pa.field('id', pa.int32()),
                    pa.field('metrics', pa.map_(
                        pa.string(), pa.field('value', value_type, nullable=False))),
                ])
                expected = [[('a', value)], [], None]
                data = pa.Table.from_pydict({
                    'id': [1, 2, 3], 'metrics': expected,
                }, schema=self.arrow_schema)
                table = self._create_table('parquet', 2)
                messages = self._write(table, data)
                field = pq.read_schema(messages[0].new_files[0].file_path).field('metrics')
                self.assertTrue(field.type['__col_0'].nullable)
                self.assertTrue(field.type['__col_1'].nullable)
                reader = table.new_read_builder()
                result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
                # Arrow 6 cannot convert non-nullable MAP values to scalars.
                actual = result.column('metrics').cast(pa.map_(pa.string(), value_type))
                self.assertEqual(expected, actual.to_pylist())

    def test_nested_non_nullable_map_values_in_subprocess(self):
        # A native Arrow assertion aborts the process, not a Python exception.
        result = subprocess.run([
            sys.executable, '-c',
            'from pypaimon.tests.map_shared_shredding_write_test import '
            'MapSharedShreddingWriteTest\n'
            'test = MapSharedShreddingWriteTest()\n'
            'test.setUp()\n'
            'try:\n'
            '    test.check_nested_non_nullable_map_values()\n'
            'finally:\n'
            '    test.tearDown()\n',
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        self.assertEqual(0, result.returncode, result.stderr.decode('utf-8', 'replace'))

    def check_nested_non_nullable_map_values(self):
        from pypaimon.write.map_shared_shredding_writer import _to_python_values

        inner = pa.map_(pa.string(), pa.field('value', pa.int64(), nullable=False))
        for value_type, value in [
                (inner, [('x', 1)]),
                (pa.struct([pa.field('nested', inner)]), {'nested': [('x', 1)]}),
                (pa.list_(inner), [[('x', 1)], [], None])]:
            self.arrow_schema = pa.schema([
                pa.field('id', pa.int32()),
                pa.field('metrics', pa.map_(pa.string(), value_type)),
            ])
            expected = [[('a', value)], [], None, [('b', value)]]
            array = pa.array([[('unused', value)]] + expected + [[]],
                             type=self.arrow_schema.field('metrics').type)
            # Keep nonzero offsets and multiple chunks in the conversion check.
            chunks = [array.slice(1, 2), array.slice(3, 2)]
            self.assertEqual(expected, [v for chunk in chunks
                                        for v in _to_python_values(chunk)])
            data = pa.Table.from_arrays([
                pa.array([1, 2, 3, 4], type=pa.int32()),
                pa.chunked_array(chunks),
            ], schema=self.arrow_schema)
            table = self._create_table('parquet', 2)
            messages = self._write(table, data)
            self.assertTrue(is_shared_shredding(pq.read_schema(
                messages[0].new_files[0].file_path).field('metrics')))
            reader = table.new_read_builder()
            result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
            self.assertEqual(expected, [v for chunk in result.column('metrics').chunks
                                        for v in _to_python_values(chunk)])

    def test_dedicated_columns_preserve_shredding(self):
        for blob, vector in [(True, False), (False, True), (True, True)]:
            with self.subTest(blob=blob, vector=vector):
                fields = [pa.field('id', pa.int32()),
                          pa.field('metrics', pa.map_(pa.string(), pa.int64()))]
                values = {'id': [1, 2], 'metrics': [[('a', 1)], []]}
                options = {'row-tracking.enabled': 'true',
                           'data-evolution.enabled': 'true'}
                if blob:
                    fields.append(pa.field('payload', pa.large_binary()))
                    values['payload'] = [b'one', b'two']
                if vector:
                    fields.append(pa.field('embedding', pa.list_(pa.float32(), 2)))
                    values['embedding'] = [[1., 2.], [3., 4.]]
                    options['vector.file.format'] = 'parquet'
                self.arrow_schema = pa.schema(fields)
                table = self._create_table('parquet', 2, options)
                messages = self._write(table, pa.Table.from_pydict(
                    values, schema=self.arrow_schema))
                normal_files = [f for m in messages for f in m.new_files
                                if f.file_name.endswith('.parquet')
                                and '.vector.' not in f.file_name]
                self.assertTrue(normal_files)
                for file in normal_files:
                    self.assertTrue(is_shared_shredding(
                        pq.read_schema(file.file_path).field('metrics')))
                reader = table.new_read_builder().with_projection(["metrics['a']"])
                result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
                self.assertEqual([1, None], result.column('metrics_a').to_pylist())

    def test_reject_default_layout_on_non_map(self):
        table = self._create_table('parquet', 2, {
            'fields.metrics.map.storage-layout': 'default',
            'fields.id.map.storage-layout': 'default',
        })
        with self.assertRaisesRegex(ValueError, 'its type is not MAP'):
            self._write(table, pa.Table.from_pydict({
                'id': [1], 'metrics': [[('a', 1)]],
            }, schema=self.arrow_schema))

    def test_reject_char_keys(self):
        from pypaimon.schema.data_types import AtomicType, DataField, MapType
        from pypaimon.write.map_shared_shredding_writer import MapSharedShreddingWriter

        with self.assertRaisesRegex(ValueError, 'STRING keys'):
            MapSharedShreddingWriter._validate_field(DataField(
                0, 'metrics', MapType(True, AtomicType('CHAR(4)', False), AtomicType('BIGINT'))))

    def test_streams_physical_batches(self):
        from pypaimon.write.map_shared_shredding_writer import _MapFieldConverter

        table = self._create_table('parquet', 256)
        data = pa.Table.from_pydict({
            'id': list(range(5000)), 'metrics': [[('a', 1)]] * 4999 + [[('late', 2)]],
        }, schema=self.arrow_schema)
        convert = _MapFieldConverter.convert
        write_table = pq.ParquetWriter.write_table
        pending = []
        sizes = []

        def convert_batch(converter, column):
            self.assertFalse(pending, 'physical batches were retained before writing')
            pending.append(len(column))
            sizes.append(len(column))
            return convert(converter, column)

        def write_batch(writer, physical, *args, **kwargs):
            self.assertEqual([physical.num_rows], pending)
            pending.clear()
            return write_table(writer, physical, *args, **kwargs)

        with patch.object(_MapFieldConverter, 'convert', convert_batch), \
                patch.object(pq.ParquetWriter, 'write_table', write_batch):
            messages = self._write(table, data)
        self.assertEqual(5000, sum(sizes))
        self.assertLessEqual(max(sizes), 1024)
        self.assertEqual(1, len(messages[0].new_files))
        reader = table.new_read_builder().with_projection(["metrics['a']", "metrics['late']"])
        result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
        self.assertEqual([1] * 4999 + [None], result.column('metrics_a').to_pylist())
        self.assertEqual([None] * 4999 + [2], result.column('metrics_late').to_pylist())

    def test_failed_stream_removes_partial_file(self):
        from pypaimon.write.map_shared_shredding_writer import MapSharedShreddingWriter

        table = self._create_table('parquet', 2)
        converter = MapSharedShreddingWriter(table.fields, table.options, 'parquet', None)
        data = pa.Table.from_pydict({
            'id': list(range(2048)), 'metrics': [[('a', 1)]] * 2048,
        }, schema=self.arrow_schema)
        path = self.temp_dir + '/partial.parquet'
        write_table = pq.ParquetWriter.write_table
        calls = []

        def fail_second_batch(writer, physical, *args, **kwargs):
            calls.append(physical.num_rows)
            if len(calls) == 2:
                raise OSError('injected write failure')
            return write_table(writer, physical, *args, **kwargs)

        with patch.object(pq.ParquetWriter, 'write_table', fail_second_batch):
            with self.assertRaisesRegex(OSError, 'injected write failure'):
                converter.write_parquet(table.file_io, path, data, 'zstd', 1)
        self.assertEqual(2, len(calls))
        self.assertFalse(table.file_io.exists(path))

    def test_row_groups_do_not_follow_input_calls(self):
        from pypaimon.table.row.generic_row import GenericRow

        for count in (1000, 2500):
            layouts = []
            for by_row in (False, True):
                table = self._create_table('parquet', 256)
                builder = table.new_batch_write_builder()
                writer = builder.new_write()
                if by_row:
                    for i in range(count):
                        writer.write_row(GenericRow([i, [('a', i)]], table.fields))
                else:
                    writer.write_arrow(pa.Table.from_pydict({
                        'id': list(range(count)),
                        'metrics': [[('a', i)] for i in range(count)],
                    }, schema=self.arrow_schema))
                messages = writer.prepare_commit()
                builder.new_commit().commit(messages)
                writer.close()
                files = [f for m in messages for f in m.new_files]
                self.assertEqual(1, len(files))
                metadata = pq.read_metadata(files[0].file_path)
                layouts.append([metadata.row_group(i).num_rows
                                for i in range(metadata.num_row_groups)])
                self.assertEqual((count + 1023) // 1024, metadata.num_row_groups)
                reader = table.new_read_builder().with_projection(['id', "metrics['a']"])
                result = reader.new_read().to_arrow(reader.new_scan().plan().splits())
                self.assertEqual(list(range(count)), result.column('id').to_pylist())
                self.assertEqual(list(range(count)), result.column('metrics_a').to_pylist())
            self.assertEqual(layouts[0], layouts[1])

    def test_reject_postpone_with_fixed_output_bucket(self):
        from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter

        table = self._create_table('parquet', 2, {'bucket': '-2'})
        with self.assertRaisesRegex(ValueError, 'postpone bucket'):
            AppendOnlyDataWriter(table, (), 0, 0, table.options)

    def test_adapts_physical_column_count_between_files(self):
        table = self._create_table(
            "parquet", max_columns=4,
            extra_options={
                "data-evolution.enabled": "true",
                "row-tracking.enabled": "true",
                "target-file-row-num": "2",
            },
        )
        data = pa.Table.from_pydict({
            "id": [1, 2, 3, 4],
            "metrics": [
                [("a", 1)],
                [("a", 2)],
                [("a", 3), ("b", 4)],
                [("b", 5)],
            ],
        }, schema=self.arrow_schema)

        messages = self._write(table, data)
        files = [file for message in messages for file in message.new_files]
        self.assertEqual(2, len(files))
        counts = []
        for file in files:
            field = pq.read_schema(file.file_path).field("metrics")
            counts.append(parse_shared_shredding_metadata(field)[1])
        self.assertEqual([4, 1], counts)

        read_builder = table.new_read_builder()
        result = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits())
        self.assertEqual(4, result.num_rows)

    def _create_table(
            self, file_format, max_columns, extra_options=None):
        options = {
            "file.format": file_format,
            "fields.metrics.map.storage-layout": "shared-shredding",
            "fields.metrics.map.shared-shredding.max-columns": str(max_columns),
        }
        options.update(extra_options or {})
        name = "default.map_write_{}_{}".format(
            file_format, len(self.catalog.list_tables("default")))
        self.catalog.create_table(
            name,
            Schema.from_pyarrow_schema(self.arrow_schema, options=options),
            False,
        )
        return self.catalog.get_table(name)

    @staticmethod
    def _write(table, data):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(data)
        messages = writer.prepare_commit()
        builder.new_commit().commit(messages)
        writer.close()
        return messages

if __name__ == "__main__":
    unittest.main()
