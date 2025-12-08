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
integration tests - production optimization feature tests.
"""

import unittest
import time

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

from pypaimon.ml.pytorch.zero_copy import (
    ZeroCopyConverter, AdaptiveBatchSizer, MemoryPoolManager
)
from pypaimon.ml.pytorch.online_features import (
    SlidingWindowAggregator, TimeDecayFeatureBuilder, InteractionFeatureBuilder, FeatureCache
)
from pypaimon.ml.pytorch.augmentation import (
    Mixup, NoiseInjection, AugmentationPipeline
)
from pypaimon.ml.monitoring import (
    PerformanceMonitor, DataSampler, RetryPolicy, DataValidator
)


class TestZeroCopyConversion(unittest.TestCase):
    """Test zero-copy conversion functionality."""

    def test_zero_copy_converter_creation(self):
        """Test zero-copy converter creation."""
        try:
            converter = ZeroCopyConverter()
            self.assertIsNotNone(converter)
        except ImportError:
            self.skipTest("NumPy/PyArrow not installed")

    def test_memory_estimation(self):
        """Test memory usage estimation."""
        try:
            import pyarrow as pa
            converter = ZeroCopyConverter()

            arrow_array = pa.array([1.0, 2.0, 3.0])
            memory_usage = converter.estimate_memory_usage(arrow_array)

            self.assertGreater(memory_usage, 0)
        except ImportError:
            self.skipTest("PyArrow not installed")


class TestAdaptiveBatchSizing(unittest.TestCase):
    """Test adaptive batch sizing."""

    def test_batch_sizer_creation(self):
        """Test batch sizer creation."""
        try:
            sizer = AdaptiveBatchSizer(
                initial_batch_size=32,
                max_memory_mb=1024
            )
            self.assertIsNotNone(sizer)
        except ImportError:
            self.skipTest("PyTorch not installed")

    @unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
    def test_batch_size_calculation(self):
        """Test batch size calculation."""
        sizer = AdaptiveBatchSizer(initial_batch_size=32, max_memory_mb=256)

        sample_tensor = torch.randn(1, 10)
        batch_size = sizer.calculate_batch_size(sample_tensor)

        self.assertGreater(batch_size, 0)
        self.assertTrue(batch_size & (batch_size - 1) == 0)  # Power of 2


class TestMemoryPoolManager(unittest.TestCase):
    """Test memory pool management."""

    def test_memory_pool_creation(self):
        """Test memory pool creation."""
        try:
            manager = MemoryPoolManager(pool_size_mb=256)
            self.assertIsNotNone(manager)
        except ImportError:
            self.skipTest("PyTorch not installed")

    @unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
    def test_memory_allocation(self):
        """Test memory allocation."""
        manager = MemoryPoolManager(pool_size_mb=256)

        tensor = manager.allocate((10, 10), torch.float32)
        self.assertIsNotNone(tensor)
        self.assertEqual(tensor.shape, (10, 10))

    @unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
    def test_memory_stats(self):
        """Test memory pool statistics."""
        manager = MemoryPoolManager(pool_size_mb=256)

        manager.allocate((10, 10), torch.float32)
        stats = manager.get_stats()

        self.assertIn('used_blocks', stats)
        self.assertIn('free_blocks', stats)
        self.assertGreaterEqual(stats['used_blocks'], 1)


class TestSlidingWindowAggregation(unittest.TestCase):
    """Test sliding window aggregation."""

    def test_sliding_window_aggregator(self):
        """Test sliding window aggregator."""
        import numpy as np

        aggregator = SlidingWindowAggregator(
            window_size=3,
            agg_functions={'sum': np.sum, 'mean': np.mean}
        )

        values = [1, 2, 3, 4, 5]
        results = aggregator.aggregate(values)

        self.assertEqual(len(results), 3)
        self.assertIn('sum', results[0])
        self.assertEqual(results[0]['sum'], 6)


class TestTimeDecayFeatures(unittest.TestCase):
    """Test time decay features."""

    def test_time_decay_feature_builder(self):
        """Test time decay feature builder."""
        builder = TimeDecayFeatureBuilder(decay_factor=0.9)

        values = [1.0, 2.0, 3.0]
        timestamps = [0, 1, 2]

        features = builder.build(values, timestamps)

        self.assertIn('time_decay_feature', features)
        self.assertGreater(features['time_decay_feature'], 0)


class TestInteractionFeatures(unittest.TestCase):
    """Test interaction features."""

    def test_interaction_feature_builder(self):
        """Test interaction feature builder."""
        builder = InteractionFeatureBuilder()

        features = {'age': 30, 'income': 50000}
        interactions = [('age', 'income', 'mul')]

        result = builder.build_interactions(features, interactions)

        self.assertIn('age_x_income', result)
        self.assertEqual(result['age_x_income'], 1500000)


class TestFeatureCache(unittest.TestCase):
    """Test feature cache."""

    def test_feature_cache(self):
        """Test feature cache functionality."""
        cache = FeatureCache(max_size=100)

        cache.put('user_1', {'age': 30, 'income': 50000})
        features = cache.get('user_1')

        self.assertIsNotNone(features)
        self.assertEqual(features['age'], 30)

    def test_cache_stats(self):
        """Test cache statistics."""
        cache = FeatureCache(max_size=100)

        cache.put('user_1', {'data': 'test'})
        cache.get('user_1')
        cache.get('user_1')

        stats = cache.get_stats()

        self.assertEqual(stats['cache_size'], 1)
        self.assertGreater(stats['total_accesses'], 0)


class TestMixupAugmentation(unittest.TestCase):
    """Test Mixup augmentation."""

    def test_mixup_creation(self):
        """Test Mixup creation."""
        try:
            augment = Mixup(alpha=0.2)
            self.assertIsNotNone(augment)
        except ImportError:
            self.skipTest("PyTorch not installed")

    @unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
    def test_mixup_augmentation(self):
        """Test Mixup augmentation."""
        augment = Mixup(alpha=0.2)

        features = torch.randn(4, 10)
        labels = torch.tensor([0, 1, 0, 1])

        mixed_features, mixed_labels = augment.augment((features, labels))

        self.assertEqual(mixed_features.shape, features.shape)
        self.assertEqual(mixed_labels.shape[0], labels.shape[0])


class TestNoiseInjection(unittest.TestCase):
    """Test noise injection."""

    def test_noise_injection(self):
        """Test noise injection augmentation."""
        try:
            augment = NoiseInjection(noise_type='gaussian', std=0.1)
            self.assertIsNotNone(augment)
        except ImportError:
            self.skipTest("PyTorch not installed")

    @unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
    def test_gaussian_noise(self):
        """Test Gaussian noise injection."""
        augment = NoiseInjection(noise_type='gaussian', std=0.1)

        data = torch.randn(10, 5)
        noisy_data = augment.augment(data)

        self.assertEqual(noisy_data.shape, data.shape)
        self.assertFalse(torch.allclose(data, noisy_data))


class TestAugmentationPipeline(unittest.TestCase):
    """Test augmentation pipeline."""

    def test_pipeline_creation(self):
        """Test augmentation pipeline creation."""
        pipeline = AugmentationPipeline()
        self.assertIsNotNone(pipeline)

    @unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
    def test_pipeline_application(self):
        """Test augmentation pipeline application."""
        pipeline = AugmentationPipeline()
        pipeline.add_augmentation(NoiseInjection(std=0.1), probability=1.0)

        data = torch.randn(10, 5)
        augmented_data = pipeline.apply(data)

        self.assertEqual(augmented_data.shape, data.shape)


class TestPerformanceMonitor(unittest.TestCase):
    """Test performance monitoring."""

    def test_performance_monitor(self):
        """Test performance monitoring."""
        monitor = PerformanceMonitor()

        monitor.start_timer('test_op')
        time.sleep(0.01)
        elapsed = monitor.end_timer('test_op')

        self.assertGreater(elapsed, 0)

    def test_metrics_retrieval(self):
        """Test performance metrics retrieval."""
        monitor = PerformanceMonitor()

        monitor.start_timer('op1')
        time.sleep(0.01)
        monitor.end_timer('op1')

        metrics = monitor.get_metrics()

        self.assertIn('op1', metrics)
        self.assertGreater(metrics['op1']['avg_time'], 0)


class TestDataSampler(unittest.TestCase):
    """Test data sampling."""

    def test_data_sampler(self):
        """Test data sampling."""
        sampler = DataSampler(sample_size=10)

        data = range(100)
        samples = sampler.sample(iter(data))

        self.assertLessEqual(len(samples), 10)

    def test_sampler_statistics(self):
        """Test sampling statistics."""
        sampler = DataSampler(sample_size=10)

        data = range(100)
        samples = sampler.sample(iter(data))
        stats = sampler.get_statistics()

        self.assertIn('sample_count', stats)
        self.assertEqual(stats['sample_count'], len(samples))


class TestRetryPolicy(unittest.TestCase):
    """Test retry strategy."""

    def test_retry_success(self):
        """Test successful execution (no retry needed)."""
        policy = RetryPolicy(max_retries=3)

        def success_func():
            return 'success'

        result = policy.execute(success_func)
        self.assertEqual(result, 'success')

    def test_retry_with_failure(self):
        """Test retry mechanism."""
        policy = RetryPolicy(max_retries=3, initial_delay=0.01)
        attempt_count = {'count': 0}

        def failing_func():
            attempt_count['count'] += 1
            if attempt_count['count'] < 2:
                raise ValueError('Test error')
            return 'success'

        result = policy.execute(failing_func)
        self.assertEqual(result, 'success')
        self.assertEqual(attempt_count['count'], 2)


class TestDataValidator(unittest.TestCase):
    """Test data validation."""

    def test_data_validation(self):
        """Test data validation."""
        validator = DataValidator()
        validator.add_rule('age', lambda x: 0 <= x <= 150, 'Age invalid')
        validator.add_rule('name', lambda x: len(x) > 0, 'Name cannot be empty')

        data = {'age': 30, 'name': 'John'}
        is_valid, errors = validator.validate(data)

        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)

    def test_validation_failure(self):
        """Test validation failure."""
        validator = DataValidator()
        validator.add_rule('age', lambda x: 0 <= x <= 150)

        data = {'age': 200}
        is_valid, errors = validator.validate(data)

        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)


if __name__ == '__main__':
    unittest.main()
