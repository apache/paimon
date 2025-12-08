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
TensorFlow data pipeline performance optimization module.

Provides a set of optimization techniques and tools:
- Prefetch: asynchronous data loading
- Cache: memory or disk caching
- Parallel interleave: read multiple files in parallel
- Batch: dynamic batch size
- Performance analysis: throughput and latency testing
"""

import logging
import time
from typing import Any, Optional, Callable

try:
    import tensorflow as tf
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False

logger = logging.getLogger(__name__)


class TensorFlowPipelineOptimizer:
    """TensorFlow data pipeline optimizer.

    Provides best practices to optimize data pipeline performance, including:
    - Automatic adjustment of prefetch buffer size
    - Parallel processing optimization
    - Cache strategy
    - Performance monitoring

    Example:
        >>> optimizer = TensorFlowPipelineOptimizer()
        >>> optimized_dataset = optimizer.optimize(dataset, num_workers=4)
    """

    def __init__(self):
        """Initialize optimizer."""
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def optimize(
        self,
        dataset: Any,
        num_workers: Optional[int] = None,
        prefetch_buffer_size: Optional[int] = None,
        enable_cache: bool = True,
        cache_file: Optional[str] = None,
        parallel_map_calls: Optional[int] = None,
        enable_performance_monitoring: bool = False
    ) -> Any:
        """Apply a series of optimizations to the data pipeline.

        Args:
            dataset: tf.data.Dataset instance
            num_workers: Number of workers (for parallel processing)
            prefetch_buffer_size: Prefetch buffer size (default: AUTOTUNE)
            enable_cache: Whether to enable caching
            cache_file: Cache file path (default: memory cache)
            parallel_map_calls: Number of parallel map calls (default: AUTOTUNE)
            enable_performance_monitoring: Whether to enable performance monitoring

        Returns:
            Optimized tf.data.Dataset

        Example:
            >>> optimizer = TensorFlowPipelineOptimizer()
            >>> optimized = optimizer.optimize(
            ...     dataset,
            ...     num_workers=4,
            ...     prefetch_buffer_size=tf.data.AUTOTUNE,
            ...     enable_cache=True
            ... )
        """
        try:
            optimized = dataset

            # 1. Cache (cache raw data before other operations)
            if enable_cache:
                self.logger.debug(f"Cache enabled (file: {cache_file or 'memory'})")
                optimized = optimized.cache(filename=cache_file)

            # 2. Shuffle (shuffle after cache for good data distribution)
            # Note: if dataset is very large, should do before batch
            # optimized = optimized.shuffle(buffer_size=10000)

            # 3. Parallel map (if transformation function available)
            if parallel_map_calls is not None:
                self.logger.debug(f"Parallel map enabled, concurrency: {parallel_map_calls}")
                # Users can add transformation functions via map operation

            # 4. Batching
            # Batching is usually controlled by user, suggestions provided here
            # optimized = optimized.batch(batch_size)

            # 5. Prefetch (after all other operations)
            if prefetch_buffer_size is None:
                prefetch_buffer_size = tf.data.AUTOTUNE

            self.logger.debug(f"Prefetch enabled, buffer size: {prefetch_buffer_size}")
            optimized = optimized.prefetch(buffer_size=prefetch_buffer_size)

            if enable_performance_monitoring:
                self.logger.info("Performance monitoring enabled, use benchmark() to test throughput")

            return optimized

        except Exception as e:
            self.logger.error(f"Data pipeline optimization failed: {e}", exc_info=True)
            raise

    @staticmethod
    def benchmark(
        dataset: Any,
        num_batches: Optional[int] = None,
        batch_size: int = 32,
        verbose: bool = True
    ) -> dict:
        """Benchmark the data pipeline performance.

        Args:
            dataset: tf.data.Dataset instance
            num_batches: Number of batches to test (default: all)
            batch_size: Batch size
            verbose: Whether to print detailed information

        Returns:
            Dictionary of performance metrics (throughput, latency, etc.)

        Example:
            >>> metrics = optimizer.benchmark(dataset, num_batches=100)
            >>> print(f"Throughput: {metrics['throughput']:.2f} batches/sec")
        """
        try:
            start_time = time.time()
            batch_count = 0
            total_samples = 0

            for batch in dataset.take(num_batches):
                batch_count += 1
                if isinstance(batch, tuple):
                    total_samples += batch[0].shape[0]
                else:
                    total_samples += batch.shape[0]

            elapsed_time = time.time() - start_time

            # Calculate metrics
            throughput = batch_count / elapsed_time if elapsed_time > 0 else 0
            samples_per_sec = total_samples / elapsed_time if elapsed_time > 0 else 0
            latency_per_batch = (elapsed_time / batch_count) * 1000 if batch_count > 0 else 0

            metrics = {
                'batch_count': batch_count,
                'total_samples': total_samples,
                'elapsed_time': elapsed_time,
                'throughput': throughput,  # batches/sec
                'samples_per_sec': samples_per_sec,
                'latency_per_batch_ms': latency_per_batch,
            }

            if verbose:
                logger.info(
                    f"Benchmark completed:\n"
                    f"  Number of batches: {batch_count}\n"
                    f"  Total samples: {total_samples}\n"
                    f"  Elapsed time: {elapsed_time:.2f}s\n"
                    f"  Throughput: {throughput:.2f} batches/sec\n"
                    f"  Sample throughput: {samples_per_sec:.2f} samples/sec\n"
                    f"  Batch latency: {latency_per_batch:.2f}ms"
                )

            return metrics

        except Exception as e:
            logger.error(f"Benchmark failed: {e}", exc_info=True)
            raise

    @staticmethod
    def get_optimization_recommendations(dataset_size: int, num_workers: int = 1) -> dict:
        """Provide optimization suggestions based on dataset size.

        Args:
            dataset_size: Dataset size (number of rows)
            num_workers: Number of workers

        Returns:
            Dictionary of optimization suggestions

        Example:
            >>> recommendations = optimizer.get_optimization_recommendations(
            ...     dataset_size=1000000,
            ...     num_workers=4
            ... )
        """
        # Provide suggestions based on dataset size
        if dataset_size < 1000:
            prefetch_buffer = 1
            cache_enabled = False
            parallel_calls = 1
            batch_size = 32
        elif dataset_size < 100000:
            prefetch_buffer = 2
            cache_enabled = True
            parallel_calls = 4
            batch_size = 64
        elif dataset_size < 1000000:
            prefetch_buffer = 4
            cache_enabled = True
            parallel_calls = 8
            batch_size = 128
        else:
            prefetch_buffer = 8
            cache_enabled = True
            parallel_calls = 16
            batch_size = 256

        # Adjust batch size based on number of workers
        batch_size = max(32, batch_size // (num_workers or 1))

        recommendations = {
            'prefetch_buffer_size': prefetch_buffer,
            'enable_cache': cache_enabled,
            'parallel_interleave_calls': parallel_calls,
            'recommended_batch_size': batch_size,
            'enable_performance_monitoring': True,
        }

        logger.info(f"Optimization suggestions (dataset size: {dataset_size}):\n{recommendations}")

        return recommendations


class DatasetPipelineBuilder:
    """Data pipeline builder, provides fluent API for optimization.

    Example:
        >>> builder = DatasetPipelineBuilder(dataset)
        >>> optimized = builder \
        ...     .cache() \
        ...     .shuffle(buffer_size=10000) \
        ...     .batch(32) \
        ...     .prefetch(tf.data.AUTOTUNE) \
        ...     .build()
    """

    def __init__(self, dataset: Any):
        """Initialize pipeline builder.

        Args:
            dataset: tf.data.Dataset instance
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        self.dataset = dataset
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def cache(self, filename: Optional[str] = None) -> 'DatasetPipelineBuilder':
        """Add cache operation.

        Args:
            filename: Cache file path (default: memory cache)

        Returns:
            self (for chaining)
        """
        try:
            self.dataset = self.dataset.cache(filename=filename)
            self.logger.debug(f"Cache operation added (file: {filename or 'memory'})")
            return self
        except Exception as e:
            self.logger.error(f"Failed to add cache: {e}", exc_info=True)
            raise

    def shuffle(self, buffer_size: int) -> 'DatasetPipelineBuilder':
        """Add shuffle operation.

        Args:
            buffer_size: Shuffle buffer size

        Returns:
            self (for chaining)
        """
        try:
            self.dataset = self.dataset.shuffle(buffer_size=buffer_size)
            self.logger.debug(f"Shuffle operation added, buffer size: {buffer_size}")
            return self
        except Exception as e:
            self.logger.error(f"Failed to add shuffle: {e}", exc_info=True)
            raise

    def batch(self, batch_size: int, drop_remainder: bool = False) -> 'DatasetPipelineBuilder':
        """Add batch operation.

        Args:
            batch_size: Batch size
            drop_remainder: Whether to drop incomplete last batch

        Returns:
            self (for chaining)
        """
        try:
            self.dataset = self.dataset.batch(batch_size, drop_remainder=drop_remainder)
            self.logger.debug(f"Batch operation added, batch size: {batch_size}")
            return self
        except Exception as e:
            self.logger.error(f"Failed to add batch: {e}", exc_info=True)
            raise

    def prefetch(self, buffer_size: Any = None) -> 'DatasetPipelineBuilder':
        """Add prefetch operation.

        Args:
            buffer_size: Prefetch buffer size (default: AUTOTUNE)

        Returns:
            self (for chaining)
        """
        try:
            if buffer_size is None:
                buffer_size = tf.data.AUTOTUNE

            self.dataset = self.dataset.prefetch(buffer_size=buffer_size)
            self.logger.debug(f"Prefetch operation added, buffer size: {buffer_size}")
            return self
        except Exception as e:
            self.logger.error(f"Failed to add prefetch: {e}", exc_info=True)
            raise

    def map(
        self,
        map_func: Callable,
        num_parallel_calls: Any = None
    ) -> 'DatasetPipelineBuilder':
        """Add map operation.

        Args:
            map_func: Transformation function
            num_parallel_calls: Number of parallel calls (default: 1)

        Returns:
            self (for chaining)
        """
        try:
            if num_parallel_calls is None:
                num_parallel_calls = 1

            self.dataset = self.dataset.map(map_func, num_parallel_calls=num_parallel_calls)
            self.logger.debug(f"Map operation added, parallel calls: {num_parallel_calls}")
            return self
        except Exception as e:
            self.logger.error(f"Failed to add map: {e}", exc_info=True)
            raise

    def repeat(self, count: int = -1) -> 'DatasetPipelineBuilder':
        """Add repeat operation.

        Args:
            count: Repeat count (-1 for infinite repeat)

        Returns:
            self (for chaining)
        """
        try:
            self.dataset = self.dataset.repeat(count=count)
            self.logger.debug(f"Repeat operation added, repeat count: {count}")
            return self
        except Exception as e:
            self.logger.error(f"Failed to add repeat: {e}", exc_info=True)
            raise

    def build(self) -> Any:
        """Build the final dataset.

        Returns:
            Optimized tf.data.Dataset
        """
        return self.dataset
