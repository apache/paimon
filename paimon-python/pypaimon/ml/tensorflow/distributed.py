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
Distributed training support module.

Provides integration with TensorFlow distributed strategies, including:
- MirroredStrategy (single machine, multi-GPU)
- MultiWorkerMirroredStrategy (multi-machine, multi-GPU)
- TPUStrategy (TPU cluster)
- ParameterServerStrategy (parameter server)
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
    """Build Paimon Dataset for distributed training.

    Automatically handles data sharding and batching, ensuring each worker gets different data shards.

    Example:
        >>> strategy = tf.distribute.MirroredStrategy()
        >>> with strategy.scope():
        ...     builder = DistributedPaimonDatasetBuilder(strategy)
        ...     dataset = builder.build(table, read_builder)
        ...     dataset = dataset.batch(32).prefetch(tf.data.AUTOTUNE)
    """

    def __init__(self, strategy: Optional[Any] = None):
        """Initialize distributed dataset builder.

        Args:
            strategy: TensorFlow distributed strategy (default: None, single machine)

        Raises:
            ImportError: if TensorFlow is not installed
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
        """Build distributed dataset.

        Args:
            table: Paimon table instance
            read_builder: configured ReadBuilder
            feature_columns: feature column names
            label_column: label column name
            **kwargs: other parameters

        Returns:
            tf.data.Dataset instance
        """
        from .dataset import PaimonTensorFlowDataset

        try:
            # Create base dataset
            paimon_dataset = PaimonTensorFlowDataset.from_paimon(
                table=table,
                read_builder=read_builder,
                feature_columns=feature_columns,
                label_column=label_column,
                **kwargs
            )

            # If strategy is provided, automatically adjust batch size
            if self.strategy:
                batch_size = kwargs.get('batch_size', 32)
                global_batch_size = batch_size * self.strategy.num_replicas_in_sync

                self.logger.info(
                    f"Distributed training: global batch size = {global_batch_size} "
                    f"(local batch size {batch_size} × {self.strategy.num_replicas_in_sync} replicas)"
                )

                # Return dataset for distributed training
                dataset = paimon_dataset.dataset
                return dataset.batch(
                    batch_size=batch_size,
                    drop_remainder=kwargs.get('drop_remainder', False)
                )
            else:
                return paimon_dataset

        except Exception as e:
            self.logger.error(f"Failed to build distributed dataset: {e}", exc_info=True)
            raise


class DistributedStrategy:
    """Distributed strategy factory class.

    Provides simple ways to create and configure common distributed strategies.
    """

    @staticmethod
    def mirrored_strategy(devices: Optional[list] = None) -> Any:
        """Create MirroredStrategy (single machine, multi-GPU).

        Args:
            devices: GPU device list (default: all available GPUs)

        Returns:
            MirroredStrategy instance
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        try:
            if devices:
                strategy = tf.distribute.MirroredStrategy(devices=devices)
            else:
                strategy = tf.distribute.MirroredStrategy()

            logger.info(
                f"MirroredStrategy created successfully, using {strategy.num_replicas_in_sync} replicas"
            )
            return strategy

        except Exception as e:
            logger.error(f"Failed to create MirroredStrategy: {e}", exc_info=True)
            raise

    @staticmethod
    def multi_worker_strategy() -> Any:
        """Create MultiWorkerMirroredStrategy (multi-machine, multi-GPU).

        Returns:
            MultiWorkerMirroredStrategy instance
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        try:
            strategy = tf.distribute.MultiWorkerMirroredStrategy()
            logger.info(
                f"MultiWorkerMirroredStrategy created successfully, using {strategy.num_replicas_in_sync} replicas"
            )
            return strategy

        except Exception as e:
            logger.error(f"Failed to create MultiWorkerMirroredStrategy: {e}", exc_info=True)
            raise

    @staticmethod
    def tpu_strategy(tpu_address: str) -> Any:
        """Create TPUStrategy (TPU cluster).

        Args:
            tpu_address: TPU address (e.g., 'grpc://10.0.0.2:8431')

        Returns:
            TPUStrategy instance
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        try:
            resolver = tf.distribute.cluster_resolver.TPUClusterResolver(tpu=tpu_address)
            tf.config.experimental_connect_to_cluster(resolver)
            tf.tpu.experimental.initialize_tpu_system(resolver)

            strategy = tf.distribute.TPUStrategy(resolver)
            logger.info(
                f"TPUStrategy created successfully, using {strategy.num_replicas_in_sync} TPU cores"
            )
            return strategy

        except Exception as e:
            logger.error(f"Failed to create TPUStrategy: {e}", exc_info=True)
            raise

    @staticmethod
    def parameter_server_strategy(
        worker_hosts: list,
        ps_hosts: list,
        worker_index: int
    ) -> Any:
        """Create ParameterServerStrategy (parameter server).

        Args:
            worker_hosts: Worker host list (e.g., ['localhost:12355', 'localhost:12356'])
            ps_hosts: Parameter server host list (e.g., ['localhost:12357'])
            worker_index: Current worker index

        Returns:
            ParameterServerStrategy instance
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required")

        try:
            cluster_def = tf.train.ClusterDef()

            # Add workers
            job_def = cluster_def.job.add()
            job_def.name = 'worker'
            for i, host in enumerate(worker_hosts):
                job_def.tasks[i] = host

            # Add parameter servers
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
            logger.info("ParameterServerStrategy created successfully")
            return strategy

        except Exception as e:
            logger.error(f"Failed to create ParameterServerStrategy: {e}", exc_info=True)
            raise
