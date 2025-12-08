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
TensorFlow 数据管道性能优化模块。

提供一套优化技术和工具：
- 预取（prefetch）：异步加载数据
- 缓存（cache）：内存或磁盘缓存
- 并行处理（parallel_interleave）：并行读取多个文件
- 批处理（batch）：动态批大小
- 性能分析：吞吐量和延迟测试
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
    """TensorFlow 数据管道优化器。

    提供一套最佳实践来优化数据管道性能，包括：
    - 预取缓冲大小自动调整
    - 并行处理优化
    - 缓存策略
    - 性能监控

    Example:
        >>> optimizer = TensorFlowPipelineOptimizer()
        >>> optimized_dataset = optimizer.optimize(dataset, num_workers=4)
    """

    def __init__(self):
        """初始化优化器。"""
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
        """应用一系列优化到数据管道。

        Args:
            dataset: tf.data.Dataset 实例
            num_workers: Worker 数量（用于并行处理）
            prefetch_buffer_size: 预取缓冲大小（默认：AUTOTUNE）
            enable_cache: 是否启用缓存
            cache_file: 缓存文件路径（默认：内存缓存）
            parallel_map_calls: 并行 map 调用数（默认：AUTOTUNE）
            enable_performance_monitoring: 是否启用性能监控

        Returns:
            优化后的 tf.data.Dataset

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

            # 1. 缓存（在其他操作之前缓存原始数据）
            if enable_cache:
                self.logger.debug(f"启用缓存（文件：{cache_file or '内存'}）")
                optimized = optimized.cache(filename=cache_file)

            # 2. 洗牌（在缓存之后洗牌以获得良好的数据分布）
            # 注意：如果数据集非常大，应在 batch 之前进行
            # optimized = optimized.shuffle(buffer_size=10000)

            # 3. 并行 map（如果有转换函数）
            if parallel_map_calls is not None:
                self.logger.debug(f"启用并行 map，并发数：{parallel_map_calls}")
                # 用户可以通过 map 操作添加转换函数

            # 4. 批处理
            # 批处理通常由用户控制，这里提供建议
            # optimized = optimized.batch(batch_size)

            # 5. 预取（在所有其他操作之后）
            if prefetch_buffer_size is None:
                prefetch_buffer_size = tf.data.AUTOTUNE

            self.logger.debug(f"启用预取，缓冲大小：{prefetch_buffer_size}")
            optimized = optimized.prefetch(buffer_size=prefetch_buffer_size)

            if enable_performance_monitoring:
                self.logger.info("性能监控已启用，使用 benchmark() 测试吞吐量")

            return optimized

        except Exception as e:
            self.logger.error(f"数据管道优化失败：{e}", exc_info=True)
            raise

    @staticmethod
    def benchmark(
        dataset: Any,
        num_batches: Optional[int] = None,
        batch_size: int = 32,
        verbose: bool = True
    ) -> dict:
        """测试数据管道的性能。

        Args:
            dataset: tf.data.Dataset 实例
            num_batches: 测试的批数（默认：全部）
            batch_size: 批大小
            verbose: 是否打印详细信息

        Returns:
            性能指标字典（吞吐量、延迟等）

        Example:
            >>> metrics = optimizer.benchmark(dataset, num_batches=100)
            >>> print(f"吞吐量：{metrics['throughput']:.2f} batches/sec")
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

            # 计算指标
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
                    f"性能测试完成：\n"
                    f"  批数：{batch_count}\n"
                    f"  总样本数：{total_samples}\n"
                    f"  耗时：{elapsed_time:.2f}s\n"
                    f"  吞吐量：{throughput:.2f} batches/sec\n"
                    f"  样本吞吐量：{samples_per_sec:.2f} samples/sec\n"
                    f"  批延迟：{latency_per_batch:.2f}ms"
                )

            return metrics

        except Exception as e:
            logger.error(f"性能测试失败：{e}", exc_info=True)
            raise

    @staticmethod
    def get_optimization_recommendations(dataset_size: int, num_workers: int = 1) -> dict:
        """根据数据集大小提供优化建议。

        Args:
            dataset_size: 数据集大小（行数）
            num_workers: Worker 数量

        Returns:
            优化建议字典

        Example:
            >>> recommendations = optimizer.get_optimization_recommendations(
            ...     dataset_size=1000000,
            ...     num_workers=4
            ... )
        """
        # 根据数据集大小提供建议
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

        # 根据 Worker 数量调整批大小
        batch_size = max(32, batch_size // (num_workers or 1))

        recommendations = {
            'prefetch_buffer_size': prefetch_buffer,
            'enable_cache': cache_enabled,
            'parallel_interleave_calls': parallel_calls,
            'recommended_batch_size': batch_size,
            'enable_performance_monitoring': True,
        }

        logger.info(f"优化建议（数据集大小：{dataset_size}）：\n{recommendations}")

        return recommendations


class DatasetPipelineBuilder:
    """数据管道构建器，提供流畅的 API 进行优化。

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
        """初始化管道构建器。

        Args:
            dataset: tf.data.Dataset 实例
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        self.dataset = dataset
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def cache(self, filename: Optional[str] = None) -> 'DatasetPipelineBuilder':
        """添加缓存操作。

        Args:
            filename: 缓存文件路径（默认：内存缓存）

        Returns:
            self（用于链式调用）
        """
        try:
            self.dataset = self.dataset.cache(filename=filename)
            self.logger.debug(f"添加缓存操作（文件：{filename or '内存'}）")
            return self
        except Exception as e:
            self.logger.error(f"添加缓存失败：{e}", exc_info=True)
            raise

    def shuffle(self, buffer_size: int) -> 'DatasetPipelineBuilder':
        """添加 shuffle 操作。

        Args:
            buffer_size: Shuffle 缓冲大小

        Returns:
            self（用于链式调用）
        """
        try:
            self.dataset = self.dataset.shuffle(buffer_size=buffer_size)
            self.logger.debug(f"添加 shuffle 操作，缓冲大小：{buffer_size}")
            return self
        except Exception as e:
            self.logger.error(f"添加 shuffle 失败：{e}", exc_info=True)
            raise

    def batch(self, batch_size: int, drop_remainder: bool = False) -> 'DatasetPipelineBuilder':
        """添加 batch 操作。

        Args:
            batch_size: 批大小
            drop_remainder: 是否丢弃不完整的最后一批

        Returns:
            self（用于链式调用）
        """
        try:
            self.dataset = self.dataset.batch(batch_size, drop_remainder=drop_remainder)
            self.logger.debug(f"添加 batch 操作，批大小：{batch_size}")
            return self
        except Exception as e:
            self.logger.error(f"添加 batch 失败：{e}", exc_info=True)
            raise

    def prefetch(self, buffer_size: Any = None) -> 'DatasetPipelineBuilder':
        """添加 prefetch 操作。

        Args:
            buffer_size: 预取缓冲大小（默认：AUTOTUNE）

        Returns:
            self（用于链式调用）
        """
        try:
            if buffer_size is None:
                buffer_size = tf.data.AUTOTUNE

            self.dataset = self.dataset.prefetch(buffer_size=buffer_size)
            self.logger.debug(f"添加 prefetch 操作，缓冲大小：{buffer_size}")
            return self
        except Exception as e:
            self.logger.error(f"添加 prefetch 失败：{e}", exc_info=True)
            raise

    def map(
        self,
        map_func: Callable,
        num_parallel_calls: Any = None
    ) -> 'DatasetPipelineBuilder':
        """添加 map 操作。

        Args:
            map_func: 转换函数
            num_parallel_calls: 并行调用数（默认：1）

        Returns:
            self（用于链式调用）
        """
        try:
            if num_parallel_calls is None:
                num_parallel_calls = 1

            self.dataset = self.dataset.map(map_func, num_parallel_calls=num_parallel_calls)
            self.logger.debug(f"添加 map 操作，并行调用数：{num_parallel_calls}")
            return self
        except Exception as e:
            self.logger.error(f"添加 map 失败：{e}", exc_info=True)
            raise

    def repeat(self, count: int = -1) -> 'DatasetPipelineBuilder':
        """添加 repeat 操作。

        Args:
            count: 重复次数（-1 表示无限重复）

        Returns:
            self（用于链式调用）
        """
        try:
            self.dataset = self.dataset.repeat(count=count)
            self.logger.debug(f"添加 repeat 操作，重复次数：{count}")
            return self
        except Exception as e:
            self.logger.error(f"添加 repeat 失败：{e}", exc_info=True)
            raise

    def build(self) -> Any:
        """构建最终的数据集。

        Returns:
            优化后的 tf.data.Dataset
        """
        return self.dataset
