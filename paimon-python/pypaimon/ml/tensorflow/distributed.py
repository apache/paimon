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
分布式训练支持模块。

提供与 TensorFlow 分布式策略的集成，包括：
- MirroredStrategy（单机多 GPU）
- MultiWorkerMirroredStrategy（多机多 GPU）
- TPUStrategy（TPU 集群）
- ParameterServerStrategy（参数服务器）
"""

import logging
from typing import Any, Optional

try:
    import tensorflow as tf
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False

logger = logging.getLogger(__name__)


class DistributedPaimonDatasetBuilder:
    """为分布式训练构建 Paimon Dataset。

    自动处理数据分片和批处理，确保每个 Worker 获得不同的数据片段。

    Example:
        >>> strategy = tf.distribute.MirroredStrategy()
        >>> with strategy.scope():
        ...     builder = DistributedPaimonDatasetBuilder(strategy)
        ...     dataset = builder.build(table, read_builder)
        ...     dataset = dataset.batch(32).prefetch(tf.data.AUTOTUNE)
    """

    def __init__(self, strategy: Optional[Any] = None):
        """初始化分布式数据集构建器。

        Args:
            strategy: TensorFlow 分布式策略（默认：None，使用单机）

        Raises:
            ImportError: 如果 TensorFlow 未安装
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        self.strategy = strategy
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def build(
        self,
        table: Any,
        read_builder: Any,
        feature_columns: Optional[list] = None,
        label_column: Optional[str] = None,
        **kwargs
    ) -> Any:
        """构建分布式数据集。

        Args:
            table: Paimon 表实例
            read_builder: 配置的 ReadBuilder
            feature_columns: 特征列名
            label_column: 标签列名
            **kwargs: 其他参数

        Returns:
            tf.data.Dataset 实例
        """
        from .dataset import PaimonTensorFlowDataset

        try:
            # 创建基础数据集
            paimon_dataset = PaimonTensorFlowDataset.from_paimon(
                table=table,
                read_builder=read_builder,
                feature_columns=feature_columns,
                label_column=label_column,
                **kwargs
            )

            # 如果提供了策略，自动调整批大小
            if self.strategy:
                batch_size = kwargs.get('batch_size', 32)
                global_batch_size = batch_size * self.strategy.num_replicas_in_sync

                self.logger.info(
                    f"分布式训练：全局批大小 = {global_batch_size} "
                    f"（本地批大小 {batch_size} × {self.strategy.num_replicas_in_sync} replicas）"
                )

                # 返回可用于分布式训练的数据集
                dataset = paimon_dataset.dataset
                return dataset.batch(
                    batch_size=batch_size,
                    drop_remainder=kwargs.get('drop_remainder', False)
                )
            else:
                return paimon_dataset

        except Exception as e:
            self.logger.error(f"构建分布式数据集失败：{e}", exc_info=True)
            raise


class DistributedStrategy:
    """分布式策略工厂类。

    提供简便的方式创建和配置常见的分布式策略。
    """

    @staticmethod
    def mirrored_strategy(devices: Optional[list] = None) -> Any:
        """创建 MirroredStrategy（单机多 GPU）。

        Args:
            devices: GPU 设备列表（默认：所有可用 GPU）

        Returns:
            MirroredStrategy 实例
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        try:
            if devices:
                strategy = tf.distribute.MirroredStrategy(devices=devices)
            else:
                strategy = tf.distribute.MirroredStrategy()

            logger.info(
                f"MirroredStrategy 创建成功，使用 {strategy.num_replicas_in_sync} 个副本"
            )
            return strategy

        except Exception as e:
            logger.error(f"创建 MirroredStrategy 失败：{e}", exc_info=True)
            raise

    @staticmethod
    def multi_worker_strategy() -> Any:
        """创建 MultiWorkerMirroredStrategy（多机多 GPU）。

        Returns:
            MultiWorkerMirroredStrategy 实例
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        try:
            strategy = tf.distribute.MultiWorkerMirroredStrategy()
            logger.info(
                f"MultiWorkerMirroredStrategy 创建成功，使用 {strategy.num_replicas_in_sync} 个副本"
            )
            return strategy

        except Exception as e:
            logger.error(f"创建 MultiWorkerMirroredStrategy 失败：{e}", exc_info=True)
            raise

    @staticmethod
    def tpu_strategy(tpu_address: str) -> Any:
        """创建 TPUStrategy（TPU 集群）。

        Args:
            tpu_address: TPU 地址（例如：'grpc://10.0.0.2:8431'）

        Returns:
            TPUStrategy 实例
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        try:
            resolver = tf.distribute.cluster_resolver.TPUClusterResolver(tpu=tpu_address)
            tf.config.experimental_connect_to_cluster(resolver)
            tf.tpu.experimental.initialize_tpu_system(resolver)

            strategy = tf.distribute.TPUStrategy(resolver)
            logger.info(
                f"TPUStrategy 创建成功，使用 {strategy.num_replicas_in_sync} 个 TPU cores"
            )
            return strategy

        except Exception as e:
            logger.error(f"创建 TPUStrategy 失败：{e}", exc_info=True)
            raise

    @staticmethod
    def parameter_server_strategy(
        worker_hosts: list,
        ps_hosts: list,
        worker_index: int
    ) -> Any:
        """创建 ParameterServerStrategy（参数服务器）。

        Args:
            worker_hosts: Worker 主机列表（例如：['localhost:12355', 'localhost:12356']）
            ps_hosts: 参数服务器主机列表（例如：['localhost:12357']）
            worker_index: 当前 Worker 的索引

        Returns:
            ParameterServerStrategy 实例
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        try:
            cluster_def = tf.train.ClusterDef()

            # 添加 Worker
            job_def = cluster_def.job.add()
            job_def.name = 'worker'
            for i, host in enumerate(worker_hosts):
                job_def.tasks[i] = host

            # 添加参数服务器
            job_def = cluster_def.job.add()
            job_def.name = 'ps'
            for i, host in enumerate(ps_hosts):
                job_def.tasks[i] = host

            resolver = tf.distribute.cluster_resolver.SimpleClusterResolver(
                cluster_def=cluster_def,
                task_type='worker',
                task_id=worker_index,
                rpc_layer='grpc'
            )

            strategy = tf.distribute.ParameterServerStrategy(resolver)
            logger.info("ParameterServerStrategy 创建成功")
            return strategy

        except Exception as e:
            logger.error(f"创建 ParameterServerStrategy 失败：{e}", exc_info=True)
            raise
