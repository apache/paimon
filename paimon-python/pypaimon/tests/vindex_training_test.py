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

import math
import os
import tempfile
import unittest
from unittest import mock

import numpy as np

from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.globalindex.vindex.vindex_vector_index_writer import (
    VindexVectorIndexWriter, _iter_training_batches,
)
from pypaimon.schema.data_types import ArrayType, AtomicType


class VindexTrainingTest(unittest.TestCase):

    def test_batches_preserve_sample_positions_and_bound_reads(self):
        vectors = np.arange(10003 * 3, dtype=np.float32).reshape(-1, 3)
        with tempfile.TemporaryFile() as stream:
            vectors.tofile(stream)
            stream.flush()
            for ratio in (1.0, 0.999, 0.37, 0.01, 1e-8):
                for batch_size in (1, 17, 1000):
                    with self.subTest(ratio=ratio, batch_size=batch_size):
                        with mock.patch.object(np, "fromfile", wraps=np.fromfile) as read:
                            batches = list(_iter_training_batches(
                                np, stream, len(vectors), 3, ratio, batch_size))
                        count = max(1, math.ceil(len(vectors) * ratio))
                        indexes = np.arange(count) * len(vectors) // count
                        np.testing.assert_array_equal(vectors[indexes], np.concatenate(batches))
                        self.assertTrue(all(b.flags.c_contiguous for b in batches))
                        self.assertTrue(all(len(b) <= batch_size for b in batches))
                        self.assertTrue(all(c[1]["count"] <= batch_size * 3
                                            for c in read.call_args_list))

    def test_training_failure_closes_trainer_and_removes_temp_files(self):
        with tempfile.TemporaryDirectory() as directory:
            writer = self._writer(directory, {})
            writer.write([1.0] * 8, 0)
            paths = [writer._vector_temp_path, writer._row_id_temp_path]
            for phase in ("add_training_vectors", "finish_training"):
                trainer = mock.MagicMock()
                trainer.__enter__.return_value = trainer
                getattr(trainer, phase).side_effect = RuntimeError("training failed")
                module = mock.Mock()
                module.VectorIndexTrainer.create.return_value = trainer
                with mock.patch.dict("sys.modules", {"paimon_vindex": module}):
                    with self.assertRaisesRegex(RuntimeError, "training failed"):
                        writer.finish()
                trainer.__exit__.assert_called_once()
                self.assertTrue(all(not os.path.exists(path) for path in paths))
                self.assertFalse(os.path.exists(writer._file_path()))
                writer = self._writer(directory, {})
                writer.write([1.0] * 8, 0)
                paths = [writer._vector_temp_path, writer._row_id_temp_path]
            writer.close()

    def test_native_streamed_build_matches_one_shot(self):
        try:
            from paimon_vindex import VectorIndexTrainer, VectorIndexWriter
        except ImportError:
            self.skipTest("paimon-vindex is not installed")
        vectors = np.random.default_rng(42).standard_normal((2049, 8)).astype(np.float32)
        with tempfile.TemporaryDirectory() as directory:
            for ratio in (1.0, 0.37):
                for index_type in ("ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann"):
                    with self.subTest(ratio=ratio, index_type=index_type):
                        options = {index_type + ".train.sample-ratio": str(ratio)}
                        if index_type != "diskann":
                            options[index_type + ".nlist"] = "16"
                        writer = self._writer(directory, options, index_type)
                        writer.write(None, 0)
                        for i, vector in enumerate(vectors):
                            writer.write(vector, i + 1)
                        reference_path = os.path.join(directory, "reference")
                        count = math.ceil(len(vectors) * ratio)
                        sample = vectors[np.arange(count) * len(vectors) // count]
                        with VectorIndexTrainer.train(writer._training_options(), sample) as training:
                            with VectorIndexWriter(training) as native:
                                native.add_vectors(np.arange(1, len(vectors) + 1), vectors)
                                with open(reference_path, "wb") as output:
                                    native.write(output)
                        with mock.patch(
                            "pypaimon.globalindex.vindex.vindex_vector_index_writer.ADD_BATCH_SIZE", 127
                        ):
                            result = writer.finish()
                        self.assertEqual(1, len(result))
                        with open(reference_path, "rb") as reference, open(writer._file_path(), "rb") as actual:
                            self.assertEqual(reference.read(), actual.read())

    @staticmethod
    def _writer(directory, options, index_type="ivf-flat"):
        options = dict(options)
        options[index_type + ".dimension"] = "8"
        return VindexVectorIndexWriter(
            LocalFileIO(), directory, ArrayType(True, AtomicType("FLOAT")),
            index_type, options, "embedding")
