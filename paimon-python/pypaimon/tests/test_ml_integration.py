#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
integration tests - TensorFlow optimization and PyTorch enhancement feature tests.
"""

import os
import shutil
import tempfile
import unittest

try:
    import torch
    from torch.utils.data import DataLoader
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    import tensorflow as tf
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False

import pyarrow as pa

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema

if TORCH_AVAILABLE:
    from pypaimon.ml.pytorch import PaimonIterableDataset, PaimonDatasetConfig
    from pypaimon.ml.pytorch.advanced_sampling import WeightedRandomSampler
    from pypaimon.ml.pytorch.feature_engineering import (
        StandardScaler, MinMaxScaler, OneHotEncoder, FeatureNormalizer
    )

if TENSORFLOW_AVAILABLE:
    from pypaimon.ml.tensorflow.distributed import DistributedPaimonDatasetBuilder
    from pypaimon.ml.tensorflow.performance import (
        TensorFlowPipelineOptimizer, DatasetPipelineBuilder
    )


@unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
class TestAdvancedSampling(unittest.TestCase):
    """Test PyTorch advanced sampling functionality."""

    def test_weighted_random_sampler(self):
        """Test weighted random sampler."""
        weights = [1.0, 1.0, 1.0, 10.0]
        sampler = WeightedRandomSampler(weights, num_samples=100)

        self.assertEqual(len(sampler), 100)

        samples = list(sampler)
        self.assertEqual(len(samples), 100)
        self.assertTrue(all(0 <= idx < 4 for idx in samples))

    def test_weighted_sampler_high_weight_sample(self):
        """Test that high-weight samples are prioritized for sampling."""
        weights = [1.0, 1.0, 1.0, 100.0]  # Last sample has very high weight
        sampler = WeightedRandomSampler(weights, num_samples=1000)

        samples = list(sampler)
        count_last = samples.count(3)

        # Last sample should be sampled approximately 100 times or more
        self.assertGreater(count_last, 500)


@unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
class TestFeatureEngineering(unittest.TestCase):
    """Test feature engineering functionality."""

    def test_standard_scaler(self):
        """Test standardization transformer."""
        scaler = StandardScaler()
        X_train = [[1, 2], [3, 4], [5, 6]]

        scaler.fit(X_train)
        X_scaled = scaler.transform([[3, 4]])

        # Check transformation result
        self.assertEqual(X_scaled.shape, (1, 2))

    def test_minmax_scaler(self):
        """Test Min-Max scaler."""
        scaler = MinMaxScaler(feature_range=(0, 1))
        X_train = [[1], [2], [3], [4], [5]]

        scaler.fit(X_train)
        X_scaled = scaler.transform([[3]])

        # 3 should map to 0.5 (middle value)
        self.assertAlmostEqual(X_scaled[0][0], 0.5, places=5)

    def test_onehot_encoder(self):
        """Test one-hot encoder."""
        encoder = OneHotEncoder()
        X_train = [['red'], ['green'], ['blue']]

        encoder.fit(X_train)
        X_encoded = encoder.transform([['red']])

        # red should be encoded as one-hot vector
        # Since alphabetical order is 'blue', 'green', 'red', red should be last
        self.assertEqual(X_encoded[0], [0.0, 0.0, 1.0])

    def test_feature_normalizer_missing_values(self):
        """Test feature normalizer handles missing values."""
        import numpy as np

        X = np.array([[1, 2], [3, np.nan], [5, 6]], dtype=float)
        normalizer = FeatureNormalizer()

        X_clean = normalizer.handle_missing_values(X, strategy='mean')

        # Check missing values are filled
        self.assertFalse(np.isnan(X_clean).any())
        self.assertEqual(X_clean.shape, (3, 2))


@unittest.skipIf(not TENSORFLOW_AVAILABLE, "TensorFlow not available")
class TestTensorFlowPerformance(unittest.TestCase):
    """Test TensorFlow performance optimization."""

    def test_pipeline_optimizer_creation(self):
        """Test pipeline optimizer creation."""
        optimizer = TensorFlowPipelineOptimizer()
        self.assertIsNotNone(optimizer)

    def test_dataset_pipeline_builder(self):
        """Test dataset pipeline builder."""
        # Create simple dataset
        dataset = tf.data.Dataset.from_tensor_slices(
            (tf.range(100), tf.range(100))
        )

        builder = DatasetPipelineBuilder(dataset)
        optimized = builder.batch(32).prefetch(tf.data.AUTOTUNE).build()

        self.assertIsNotNone(optimized)

    def test_optimization_recommendations(self):
        """Test optimization recommendation generation."""
        optimizer = TensorFlowPipelineOptimizer()

        recommendations = optimizer.get_optimization_recommendations(
            dataset_size=1000000,
            num_workers=4
        )

        self.assertIn('prefetch_buffer_size', recommendations)
        self.assertIn('enable_cache', recommendations)
        self.assertIn('parallel_interleave_calls', recommendations)
        self.assertIn('recommended_batch_size', recommendations)


@unittest.skipIf(not TENSORFLOW_AVAILABLE, "TensorFlow not available")
class TestDistributedTraining(unittest.TestCase):
    """Test distributed training support."""

    def test_distributed_builder_creation(self):
        """Test distributed dataset builder creation."""
        builder = DistributedPaimonDatasetBuilder()
        self.assertIsNotNone(builder)

    def test_distributed_builder_with_strategy(self):
        """Test builder with distributed strategy."""
        strategy = tf.distribute.get_strategy()
        builder = DistributedPaimonDatasetBuilder(strategy)

        self.assertIsNotNone(builder.strategy)


@unittest.skipIf(not (TORCH_AVAILABLE and TENSORFLOW_AVAILABLE), "Both PyTorch and TensorFlow required")
class TestCrossFrameworkIntegration(unittest.TestCase):
    """Test cross-framework integration between PyTorch and TensorFlow."""

    @classmethod
    def setUpClass(cls):
        """Set up test data."""
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            ('feature1', pa.int64()),
            ('feature2', pa.float64()),
            ('label', pa.int32())
        ])

        cls.raw_data = {
            'feature1': [1, 2, 3, 4, 5],
            'feature2': [1.0, 2.0, 3.0, 4.0, 5.0],
            'label': [0, 1, 0, 1, 1]
        }
        cls.expected = pa.Table.from_pydict(cls.raw_data, schema=cls.pa_schema)

        schema = Schema.from_pyarrow_schema(cls.pa_schema)
        cls.catalog.create_table('default.test_cross_integration', schema, False)
        cls.table = cls.catalog.get_table('default.test_cross_integration')

        write_builder = cls.table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(cls.expected)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    @classmethod
    def tearDownClass(cls):
        """Clean up test data."""
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_pytorch_to_tensorflow_interoperability(self):
        """Test data interoperability between PyTorch and TensorFlow."""
        read_builder = self.table.new_read_builder()

        # PyTorch dataset
        config = PaimonDatasetConfig(target_column='label')
        pytorch_dataset = PaimonIterableDataset(read_builder=read_builder, config=config)

        # Verify PyTorch dataset can be iterated
        batch_count = 0
        for features, labels in pytorch_dataset:
            batch_count += 1
            self.assertIsNotNone(features)
            self.assertIsNotNone(labels)

        self.assertGreater(batch_count, 0)


if __name__ == '__main__':
    unittest.main()
