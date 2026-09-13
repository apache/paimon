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
import unittest
from unittest.mock import Mock, patch

import numpy as np
import pyarrow as pa

from pypaimon.globalindex.create_global_index import GlobalIndexBuilder, _write_vector_batch
from pypaimon.globalindex.vindex.vindex_vector_index_writer import VindexVectorIndexWriter
from pypaimon.schema.data_types import ArrayType, AtomicType
from pypaimon.utils.range import Range


class VindexBatchWriteTest(unittest.TestCase):

    def _writer(self):
        writer = VindexVectorIndexWriter(
            Mock(), '/unused', ArrayType(True, AtomicType('FLOAT')),
            'ivf-flat', {'ivf-flat.dimension': '2'}, 'embedding')
        self.addCleanup(writer.close)
        return writer

    def _contents(self, writer):
        writer._close_temp_files()
        contents = []
        for path in (writer._row_id_temp_path, writer._vector_temp_path):
            if path is None:
                contents.append(b'')
            else:
                with open(path, 'rb') as stream:
                    contents.append(stream.read())
        return writer._row_count, writer._vector_count, contents

    def _assert_parity(self, vectors, row_ids, fast=True):
        scalar, batch = self._writer(), self._writer()
        for vector, row_id in zip(vectors.to_pylist(), row_ids.to_pylist()):
            scalar.write(vector, row_id)
        with patch.object(batch, 'write', wraps=batch.write) as write:
            batch.write_batch(vectors, row_ids)
            if fast:
                write.assert_not_called()
        self.assertEqual(self._contents(scalar), self._contents(batch))

    def test_sliced_list_large_list_and_fixed_size_list(self):
        for array_type in (pa.list_(pa.float32()), pa.large_list(pa.float32()),
                           pa.list_(pa.float32(), 2)):
            with self.subTest(array_type=array_type):
                vectors = pa.array([
                    [99, 99], [1, 2], None, [-0.0, 1e-40], [3, 4], [88, 88],
                ], type=array_type).slice(1, 4)
                row_ids = pa.array([77, 0, 3, 8, 2 ** 62, 99], type=pa.int64()).slice(1, 4)
                self._assert_parity(vectors, row_ids)

    def test_null_parent_ignores_invalid_child_values(self):
        values = pa.array([1, 2, float('nan'), None, 3, 4], type=pa.float32())
        vectors = pa.Array.from_buffers(pa.list_(pa.float32()), 3, [
            pa.py_buffer(b'\x05'),
            pa.py_buffer(np.array([0, 2, 4, 6], dtype=np.int32)),
        ], children=[values])
        self._assert_parity(vectors, pa.array([0, 1, 2], type=pa.int64()))

    def test_empty_and_all_null_batches_do_not_create_files(self):
        for data in ([], [None, None]):
            with self.subTest(data=data):
                writer = self._writer()
                writer.write_batch(pa.array(data, type=pa.list_(pa.float32())),
                                   pa.array(range(len(data)), type=pa.int64()))
                self.assertEqual((len(data), 0, [b'', b'']), self._contents(writer))

    def test_multiple_batches_and_scalar_writes_can_be_interleaved(self):
        writer = self._writer()
        writer.write([1, 2], 0)
        writer.write_batch(pa.array([[3, 4], None], type=pa.list_(pa.float32())),
                           pa.array([2, 3], type=pa.int64()))
        writer.write([5, 6], 4)
        writer.write_batch(pa.array([[7, 8]], type=pa.list_(pa.float32(), 2)),
                           pa.array([6], type=pa.int64()))
        count, valid_count, contents = self._contents(writer)
        self.assertEqual((5, 4), (count, valid_count))
        self.assertEqual([0, 2, 4, 6], np.frombuffer(contents[0], dtype=np.int64).tolist())
        self.assertEqual(list(range(1, 9)), np.frombuffer(contents[1], dtype=np.float32).tolist())

    def test_invalid_vectors_preserve_scalar_error_and_written_prefix(self):
        cases = [
            [[1, 2], [3]],
            [[1, 2], [None, 4]],
            [[1, 2], [float('nan'), 4]],
            [[1, 2], [3, float('inf')]],
            [[1, 2], [3, float('-inf')]],
            [[float('nan'), 2], [3]],
            [[1, 2], [float('nan'), None]],
            [[1, 2], [None, float('nan')]],
        ]
        for data in cases:
            with self.subTest(data=data):
                vectors = pa.array(data, type=pa.list_(pa.float32()))
                ids = pa.array([5, 9], type=pa.int64())
                scalar, batch = self._writer(), self._writer()
                with self.assertRaises(ValueError) as old_error:
                    for vector, row_id in zip(vectors.to_pylist(), ids.to_pylist()):
                        scalar.write(vector, row_id)
                with self.assertRaises(ValueError) as new_error:
                    batch.write_batch(vectors, ids)
                self.assertEqual(str(old_error.exception), str(new_error.exception))
                self.assertEqual(self._contents(scalar), self._contents(batch))

    def test_float64_and_non_int64_ids_fall_back_to_scalar(self):
        self._assert_parity(
            pa.array([[1.1, 2.2], None], type=pa.list_(pa.float64())),
            pa.array([0, 1], type=pa.int64()), fast=False)
        self._assert_parity(
            pa.array([[1, 2], [3, 4]], type=pa.list_(pa.float32())),
            pa.array([0, 1], type=pa.int32()), fast=False)

    def test_batch_length_and_null_row_id_validation(self):
        vectors = pa.array([[1, 2]], type=pa.list_(pa.float32()))
        writer = self._writer()
        with self.assertRaisesRegex(ValueError, 'batch lengths differ'):
            writer.write_batch(vectors, pa.array([], type=pa.int64()))
        with self.assertRaisesRegex(ValueError, '_ROW_ID is null'):
            writer.write_batch(vectors, pa.array([None], type=pa.int64()))
        self.assertEqual((0, 0, [b'', b'']), self._contents(writer))

    def test_close_removes_batch_files_and_rejects_further_writes(self):
        writer = self._writer()
        vectors = pa.array([[1, 2]], type=pa.list_(pa.float32()))
        ids = pa.array([0], type=pa.int64())
        writer.write_batch(vectors, ids)
        paths = writer._row_id_temp_path, writer._vector_temp_path
        writer.close()
        self.assertTrue(all(not os.path.exists(path) for path in paths))
        with self.assertRaisesRegex(RuntimeError, 'already closed'):
            writer.write_batch(vectors, ids)

    def test_builder_filters_ranges_before_vector_validation(self):
        vectors = pa.array([[float('nan'), 0], [1, 2], None, [3, 4], [5]],
                           type=pa.list_(pa.float32()))
        ids = pa.array([9, 10, 11, 19, 20], type=pa.int64())
        batch = pa.RecordBatch.from_arrays([vectors, ids], ['embedding', '_ROW_ID'])
        writer = self._writer()
        _write_vector_batch(writer, batch, 'embedding', Range(10, 19))
        count, valid_count, contents = self._contents(writer)
        self.assertEqual((3, 2), (count, valid_count))
        self.assertEqual([0, 9], np.frombuffer(contents[0], dtype=np.int64).tolist())
        self.assertEqual([1, 2, 3, 4], np.frombuffer(contents[1], dtype=np.float32).tolist())
        batch = pa.RecordBatch.from_arrays([
            pa.array([[1, 2]], type=pa.list_(pa.float32())),
            pa.array([None], type=pa.int64()),
        ], ['embedding', '_ROW_ID'])
        with self.assertRaisesRegex(ValueError, '_ROW_ID is null'):
            _write_vector_batch(self._writer(), batch, 'embedding', Range(10, 19))

    def test_null_row_ids_are_rejected_before_writing_any_batch(self):
        builder = object.__new__(GlobalIndexBuilder)
        builder._core_options = Mock()
        builder._core_options.global_index_row_count_per_shard.return_value = 10
        builder._index_type = 'ivf-flat'
        builder._index_columns = ['embedding']
        writer = Mock()
        builder._create_generic_index_writer = Mock(return_value=writer)
        read = Mock()
        read.to_arrow.return_value = pa.table({
            'embedding': pa.array([[1], [3, 4]], type=pa.list_(pa.float32())),
            '_ROW_ID': pa.array([0, None], type=pa.int64()),
        })
        module = 'pypaimon.globalindex.create_global_index'
        with patch(module + '._split_by_global_index_shard', return_value=[
            (Mock(), Range(0, 9)),
        ]), patch(module + '.ADD_BATCH_SIZE', 1):
            with self.assertRaisesRegex(ValueError, '_ROW_ID is null'):
                builder._build_generic_index([], [], Mock(), read, '/unused')
        writer.write_batch.assert_not_called()
        writer.finish.assert_not_called()
        writer.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
