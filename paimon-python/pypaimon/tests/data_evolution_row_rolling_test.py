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

import os
import shutil
import tempfile
import unittest
import uuid

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.uri_reader import FileUriReader
from pypaimon.table.row.blob import Blob, BlobDescriptor, VideoFrameDescriptor


class DataEvolutionRowRollingTest(unittest.TestCase):
    """Row-count based data file rolling (target-file-row-num) for
    data-evolution append tables."""

    pa_schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
    ])
    blob_schema = pa.schema([
        ('id', pa.int32()),
        ('payload', pa.large_binary()),
    ])
    multi_video_schema = pa.schema([
        ('id', pa.int32()),
        ('camera_a', pa.large_binary()),
        ('camera_b', pa.large_binary()),
    ])
    vector_schema = pa.schema([
        ('id', pa.int32()),
        ('embedding', pa.list_(pa.float32(), 3)),
    ])
    blob_vector_schema = pa.schema([
        ('id', pa.int32()),
        ('payload', pa.large_binary()),
        ('embedding', pa.list_(pa.float32(), 3)),
    ])
    de_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
    }

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = CatalogFactory.create(
            {'warehouse': os.path.join(cls.tempdir, 'warehouse')})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create(self, options):
        name = f'default.roll_{uuid.uuid4().hex[:8]}'
        self.catalog.create_table(
            name, Schema.from_pyarrow_schema(self.pa_schema, options=options),
            False)
        return self.catalog.get_table(name)

    def _create_with_schema(self, pa_schema, options):
        name = f'default.roll_{uuid.uuid4().hex[:8]}'
        self.catalog.create_table(
            name, Schema.from_pyarrow_schema(pa_schema, options=options),
            False)
        return self.catalog.get_table(name)

    def _rows(self, n):
        return pa.Table.from_pydict(
            {'id': list(range(n)), 'name': [f'n{i}' for i in range(n)]},
            schema=self.pa_schema)

    def _blob_rows(self, n):
        return pa.Table.from_pydict(
            {
                'id': list(range(n)),
                'payload': [f'blob-{i}'.encode() for i in range(n)],
            },
            schema=self.blob_schema)

    def _vector_rows(self, n):
        return pa.Table.from_pydict(
            {
                'id': list(range(n)),
                'embedding': [
                    [float(i), float(i + 1), float(i + 2)]
                    for i in range(n)
                ],
            },
            schema=self.vector_schema)

    def _blob_vector_rows(self, n):
        return pa.Table.from_pydict(
            {
                'id': list(range(n)),
                'payload': [f'blob-{i}'.encode() for i in range(n)],
                'embedding': [
                    [float(i), float(i + 1), float(i + 2)]
                    for i in range(n)
                ],
            },
            schema=self.blob_vector_schema)

    def _write_files(self, table, data):
        """Write one Arrow table and return the committed DataFileMeta list."""
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tw.write_arrow(data)
        msgs = tw.prepare_commit()
        files = [f for m in msgs for f in m.new_files]
        wb.new_commit().commit(msgs)
        tw.close()
        return files

    def _read_ids(self, table):
        rb = table.new_read_builder().with_projection(['id'])
        return sorted(
            rb.new_read().to_arrow(rb.new_scan().plan().splits())
            ['id'].to_pylist())

    def test_rolls_when_row_count_exceeds_limit(self):
        table = self._create({**self.de_options, 'target-file-row-num': '3'})
        files = self._write_files(table, self._rows(10))
        # 10 rows, limit 3 -> 3 full files + a 1-row remainder.
        self.assertEqual([1, 3, 3, 3], sorted(f.row_count for f in files))
        self.assertEqual(list(range(10)), self._read_ids(table))

    def test_exact_multiple_rolls_evenly(self):
        table = self._create({**self.de_options, 'target-file-row-num': '3'})
        files = self._write_files(table, self._rows(6))
        self.assertEqual([3, 3], sorted(f.row_count for f in files))
        self.assertEqual(list(range(6)), self._read_ids(table))

    def test_below_limit_is_single_file(self):
        table = self._create({**self.de_options, 'target-file-row-num': '100'})
        files = self._write_files(table, self._rows(10))
        self.assertEqual([10], [f.row_count for f in files])

    def test_unset_option_does_not_roll_by_rows(self):
        table = self._create(self.de_options)
        files = self._write_files(table, self._rows(50))
        # No row limit -> a small batch stays one file (size rolling only).
        self.assertEqual([50], [f.row_count for f in files])

    def test_oversized_row_rolls_by_itself(self):
        # Each row exceeds target-file-size: the size trigger rolls every row by
        # itself even though target-file-row-num is larger.
        table = self._create({
            **self.de_options,
            'target-file-row-num': '3',
            'target-file-size': '100 b',
        })
        big = 'x' * 500
        data = pa.Table.from_pydict(
            {'id': list(range(4)), 'name': [big] * 4}, schema=self.pa_schema)
        files = self._write_files(table, data)
        self.assertEqual([1, 1, 1, 1], [f.row_count for f in files])
        self.assertEqual(list(range(4)), self._read_ids(table))

    def test_non_de_table_still_fails_fast(self):
        table = self._create({'target-file-row-num': '3'})
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        with self.assertRaisesRegex(
                NotImplementedError, 'row-count based file rolling'):
            tw.write_arrow(self._rows(4))

    def test_blob_writer_supports_target_file_row_num(self):
        table = self._create_with_schema(
            self.blob_schema,
            {**self.de_options, 'target-file-row-num': '3'})

        files = self._write_files(table, self._blob_rows(7))

        data_rows = sorted(
            f.row_count for f in files
            if not f.file_name.endswith('.blob'))
        blob_rows = sorted(
            f.row_count for f in files
            if f.file_name.endswith('.blob'))
        self.assertEqual([1, 3, 3], data_rows)
        self.assertEqual([1, 3, 3], blob_rows)
        self.assertEqual(list(range(7)), self._read_ids(table))

    def test_blob_writer_is_reused_across_prepare_commit_rounds(self):
        table = self._create_with_schema(
            self.blob_schema,
            {**self.de_options, 'target-file-row-num': '2'})
        write_builder = table.new_stream_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        messages = []

        for commit_identifier, start in enumerate(range(0, 6, 2)):
            rows = pa.Table.from_pydict(
                {
                    'id': [start, start + 1],
                    'payload': [
                        f'blob-{start}'.encode(),
                        f'blob-{start + 1}'.encode(),
                    ],
                },
                schema=self.blob_schema,
            )
            writer.write_arrow(rows)
            messages = writer.prepare_commit(commit_identifier)
            sidecars = [
                file
                for message in messages
                for file in message.new_files
                if file.file_name.endswith('.blob')
            ]
            self.assertEqual(
                start + 2, sum(file.row_count for file in sidecars))

        commit.commit(messages, commit_identifier)

        writer.close()
        commit.close()
        read_builder = table.new_read_builder()
        result = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits()).sort_by('id')
        self.assertEqual(list(range(6)), result['id'].to_pylist())
        self.assertEqual(
            [f'blob-{row}'.encode() for row in range(6)],
            result['payload'].to_pylist(),
        )

    def test_video_writer_rolls_between_payload_groups(self):
        first = os.path.join(self.tempdir, 'first.mp4')
        second = os.path.join(self.tempdir, 'second.mp4')
        with open(first, 'wb') as output:
            output.write(b'first-video')
        with open(second, 'wb') as output:
            output.write(b'second-video')
        first_descriptor = BlobDescriptor(first, 0, len(b'first-video'))
        second_descriptor = BlobDescriptor(second, 0, len(b'second-video'))

        table = self._create_with_schema(
            self.blob_schema,
            {
                **self.de_options,
                'target-file-row-num': '1',
                'video-frame-field': 'payload',
            },
        )
        data = pa.Table.from_pydict(
            {
                'id': list(range(5)),
                'payload': [
                    VideoFrameDescriptor(
                        first_descriptor.uri,
                        first_descriptor.offset,
                        first_descriptor.length,
                        frame,
                    ).serialize()
                    for frame in range(3)
                ] + [
                    VideoFrameDescriptor(
                        second_descriptor.uri,
                        second_descriptor.offset,
                        second_descriptor.length,
                        frame,
                    ).serialize()
                    for frame in range(2)
                ],
            },
            schema=self.blob_schema,
        )

        files = self._write_files(table, data)

        video_rows = sorted(
            f.row_count for f in files if f.file_name.endswith('.video')
        )
        normal_rows = sorted(
            f.row_count for f in files if not f.file_name.endswith('.video')
        )
        self.assertEqual([2, 3], video_rows)
        self.assertEqual([1, 1, 1, 1, 1], normal_rows)
        self.assertEqual(list(range(5)), self._read_ids(table))

    def test_multiple_video_fields_roll_independently(self):
        paths = [os.path.join(self.tempdir, f'camera-{index}.mp4')
                 for index in range(4)]
        payloads = [f'camera-video-{index}'.encode() for index in range(4)]
        for path, payload in zip(paths, payloads):
            with open(path, 'wb') as output:
                output.write(payload)
        descriptors = [
            BlobDescriptor(path, 0, len(payload))
            for path, payload in zip(paths, payloads)
        ]

        table = self._create_with_schema(
            self.multi_video_schema,
            {
                **self.de_options,
                'target-file-row-num': '2',
                'video-frame-field': 'camera_a,camera_b',
                'blob-as-descriptor': 'true',
            },
        )
        rows = pa.Table.from_pydict(
            {
                'id': list(range(6)),
                'camera_a': [
                    VideoFrameDescriptor(
                        descriptors[0 if row < 4 else 1].uri,
                        0,
                        descriptors[0 if row < 4 else 1].length,
                        row if row < 4 else row - 4,
                    ).serialize()
                    for row in range(6)
                ],
                'camera_b': [
                    VideoFrameDescriptor(
                        descriptors[2 if row < 2 else 3].uri,
                        0,
                        descriptors[2 if row < 2 else 3].length,
                        row if row < 2 else row - 2,
                    ).serialize()
                    for row in range(6)
                ],
            },
            schema=self.multi_video_schema,
        )

        files = self._write_files(table, rows)
        self.assertEqual(
            [2, 2, 2],
            sorted(f.row_count for f in files
                   if not f.file_name.endswith('.video')),
        )
        video_files = [f for f in files if f.file_name.endswith('.video')]
        self.assertEqual(4, len(video_files))
        self.assertEqual(
            {'camera_a': 6, 'camera_b': 6},
            {
                field: sum(f.row_count for f in video_files
                           if f.write_cols == [field])
                for field in ('camera_a', 'camera_b')
            },
        )

        descriptor_table = table.copy({'blob-as-descriptor': 'true'})
        read = descriptor_table.new_read_builder()
        result = read.new_read().to_arrow(read.new_scan().plan().splits())
        self.assertEqual(list(range(6)), result['id'].to_pylist())
        for row in range(6):
            camera_a = VideoFrameDescriptor.deserialize(
                result['camera_a'][row].as_py())
            camera_b = VideoFrameDescriptor.deserialize(
                result['camera_b'][row].as_py())
            self.assertEqual(row if row < 4 else row - 4, camera_a.frame_index)
            self.assertEqual(row if row < 2 else row - 2, camera_b.frame_index)

    def test_blob_consumer_descriptors_survive_abort_after_rolling(self):
        table = self._create_with_schema(
            self.blob_schema,
            {**self.de_options, 'target-file-row-num': '3'})
        descriptors = []

        def consume(_, descriptor):
            if descriptor is not None:
                descriptors.append(descriptor)
            return True

        writer = table.new_batch_write_builder().new_write()
        writer.with_blob_consumer(consume)
        writer.write_arrow(self._blob_rows(7))
        writer.abort()

        self.assertEqual(7, len(descriptors))
        uri_reader = FileUriReader(table.file_io)
        for index, descriptor in enumerate(descriptors):
            self.assertEqual(
                f'blob-{index}'.encode(),
                Blob.from_descriptor(uri_reader, descriptor).to_data())

    def test_vector_writer_supports_target_file_row_num(self):
        table = self._create_with_schema(
            self.vector_schema,
            {
                **self.de_options,
                'target-file-row-num': '3',
                'vector.file.format': 'parquet',
            })

        files = self._write_files(table, self._vector_rows(7))

        data_rows = sorted(
            f.row_count for f in files
            if '.vector.' not in f.file_name)
        vector_rows = sorted(
            f.row_count for f in files
            if '.vector.' in f.file_name)
        self.assertEqual([1, 3, 3], data_rows)
        self.assertEqual([1, 3, 3], vector_rows)
        self.assertEqual(list(range(7)), self._read_ids(table))

    def test_dedicated_writer_rolls_blob_and_vector_together(self):
        table = self._create_with_schema(
            self.blob_vector_schema,
            {
                **self.de_options,
                'target-file-row-num': '3',
                'vector.file.format': 'parquet',
            })

        files = self._write_files(table, self._blob_vector_rows(7))

        data_rows = sorted(
            f.row_count for f in files
            if not f.file_name.endswith('.blob') and '.vector.' not in f.file_name)
        blob_rows = sorted(
            f.row_count for f in files
            if f.file_name.endswith('.blob'))
        vector_rows = sorted(
            f.row_count for f in files
            if '.vector.' in f.file_name)
        self.assertEqual([1, 3, 3], data_rows)
        self.assertEqual([1, 3, 3], blob_rows)
        self.assertEqual([1, 3, 3], vector_rows)
        self.assertEqual(list(range(7)), self._read_ids(table))


if __name__ == '__main__':
    unittest.main()
