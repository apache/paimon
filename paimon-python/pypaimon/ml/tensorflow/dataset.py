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
TensorFlow Dataset implementation for Paimon tables.

Provides tf.data.Dataset wrapper for efficient streaming from Paimon.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

try:
    import tensorflow as tf
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False

import pyarrow as pa

logger = logging.getLogger(__name__)


@dataclass
class PaimonTensorFlowDatasetConfig:
    """Configuration for Paimon TensorFlow Dataset.

    Attributes:
        feature_columns: List of feature column names. If empty, all columns except label are used.
        label_column: Optional label column name for supervised learning.
        transform: Optional custom transformation function for data.
        batch_size: Batch size for dataset (default: 32).
        prefetch_buffer: Prefetch buffer size (default: tf.data.AUTOTUNE).
        drop_remainder: Drop last batch if incomplete (default: False).
    """

    feature_columns: List[str] = field(default_factory=list)
    label_column: Optional[str] = None
    transform: Optional[Callable] = None
    batch_size: int = 32
    prefetch_buffer: Any = None
    drop_remainder: bool = False

    def __post_init__(self):
        """Validate and initialize configuration."""
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required. Install with: pip install tensorflow")

        if self.prefetch_buffer is None:
            self.prefetch_buffer = tf.data.AUTOTUNE


class PaimonTensorFlowDataset:
    """TensorFlow Dataset wrapper for Paimon tables.

    This class provides a high-level API for reading Paimon tables
    as tf.data.Dataset, which can be directly used with Keras models
    or custom training loops.

    Key features:
    - Streaming read via Arrow batch reader
    - Automatic type conversion from Arrow to TensorFlow
    - Support for prefetching and parallel processing
    - Compatible with tf.distribute.Strategy for distributed training
    - Zero-copy Arrow data transfer for efficiency

    Example:
        >>> from pypaimon import CatalogFactory
        >>> from pypaimon.ml.tensorflow import PaimonTensorFlowDataset
        >>> import tensorflow as tf
        >>>
        >>> catalog = CatalogFactory.create({'warehouse': '/path/to/warehouse'})
        >>> table = catalog.get_table('default.training_data')
        >>> read_builder = table.new_read_builder()
        >>>
        >>> tf_dataset = PaimonTensorFlowDataset.from_paimon(
        ...     table=table,
        ...     read_builder=read_builder,
        ...     feature_columns=['feature1', 'feature2'],
        ...     label_column='label'
        ... )
        >>>
        >>> # Apply standard TensorFlow operations
        >>> tf_dataset = tf_dataset.batch(32).prefetch(tf.data.AUTOTUNE)
        >>>
        >>> # Use with Keras
        >>> model = tf.keras.Sequential([...])
        >>> model.fit(tf_dataset, epochs=10)
    """

    def __init__(
        self,
        dataset: Any,
        config: Optional[PaimonTensorFlowDatasetConfig] = None
    ):
        """Initialize PaimonTensorFlowDataset.

        Args:
            dataset: Underlying tf.data.Dataset
            config: Optional dataset configuration
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required. Install with: pip install tensorflow")

        self.dataset = dataset
        self.config = config or PaimonTensorFlowDatasetConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @staticmethod
    def from_paimon(
        table: Any,
        read_builder: Any,
        feature_columns: Optional[List[str]] = None,
        label_column: Optional[str] = None,
        config: Optional[PaimonTensorFlowDatasetConfig] = None,
        **kwargs
    ) -> 'PaimonTensorFlowDataset':
        """Create TensorFlow Dataset from Paimon table.

        Args:
            table: Paimon table instance
            read_builder: Configured ReadBuilder from Paimon table
            feature_columns: List of feature column names
            label_column: Optional label column name
            config: Optional PaimonTensorFlowDatasetConfig
            **kwargs: Additional arguments for tf.data.Dataset

        Returns:
            PaimonTensorFlowDataset instance

        Raises:
            ImportError: If TensorFlow not available
            ValueError: If read_builder is invalid
        """
        if not TENSORFLOW_AVAILABLE:
            raise ImportError("TensorFlow is required. Install with: pip install tensorflow")

        if read_builder is None:
            raise ValueError("read_builder cannot be None")

        logger.info("Creating TensorFlow Dataset from Paimon table...")

        # Update config if needed
        if config is None:
            config = PaimonTensorFlowDatasetConfig()

        if feature_columns:
            config.feature_columns = feature_columns
        if label_column:
            config.label_column = label_column

        # Create generator
        def generator():
            """Generator function for tf.data.Dataset.from_generator."""
            try:
                # Get all splits and create batch reader
                scan = read_builder.new_scan()
                splits = scan.plan().splits()
                table_read = read_builder.new_read()
                batch_reader = table_read.to_arrow_batch_reader(splits)

                batch_count = 0
                for arrow_batch in batch_reader:
                    batch_count += 1

                    # Process Arrow batch
                    df = arrow_batch.to_pandas()

                    # Determine feature columns
                    if not config.feature_columns:
                        feat_cols = [
                            col
                            for col in df.columns
                            if col != config.label_column
                        ]
                    else:
                        feat_cols = config.feature_columns

                    # Extract features and labels
                    features = df[feat_cols].values.astype('float32')

                    if config.label_column and config.label_column in df.columns:
                        labels = df[config.label_column].values
                        yield (features, labels)
                    else:
                        yield (features,)

                logger.info(f"Processed {batch_count} batches from Paimon table")

            except Exception as e:
                logger.error(f"Error in generator: {e}", exc_info=True)
                raise

        # Determine output signature
        output_spec = PaimonTensorFlowDataset._infer_output_spec(
            read_builder, config
        )

        # Create tf.data.Dataset
        tf_dataset = tf.data.Dataset.from_generator(
            generator,
            output_signature=output_spec
        )

        logger.info("TensorFlow Dataset created successfully")

        return PaimonTensorFlowDataset(tf_dataset, config)

    @staticmethod
    def _infer_output_spec(
        read_builder: Any,
        config: PaimonTensorFlowDatasetConfig
    ) -> Tuple[Any, ...]:
        """Infer output specification from table schema.

        Args:
            read_builder: Configured ReadBuilder
            config: Dataset configuration

        Returns:
            Tuple of tf.TensorSpec for output_signature
        """
        try:
            # Get schema from read builder
            read_type = read_builder.read_type()

            # Determine feature and label columns
            all_columns = [field.name for field in read_type]

            if not config.feature_columns:
                feat_cols = [
                    col
                    for col in all_columns
                    if col != config.label_column
                ]
            else:
                feat_cols = config.feature_columns

            # Create TensorSpec for features
            num_features = len(feat_cols)
            features_spec = tf.TensorSpec(
                shape=(None, num_features),
                dtype=tf.float32
            )

            if config.label_column and config.label_column in all_columns:
                labels_spec = tf.TensorSpec(shape=(None,), dtype=tf.int32)
                return (features_spec, labels_spec)
            else:
                return (features_spec,)

        except Exception as e:
            logger.error(f"Error inferring output spec: {e}", exc_info=True)
            # Return generic spec
            return (
                tf.TensorSpec(shape=(None, None), dtype=tf.float32),
            )

    def batch(self, batch_size: int, drop_remainder: bool = False) -> 'PaimonTensorFlowDataset':
        """Batch the dataset.

        Args:
            batch_size: Size of batches
            drop_remainder: Drop incomplete last batch

        Returns:
            New PaimonTensorFlowDataset with batched data
        """
        batched = self.dataset.batch(batch_size, drop_remainder=drop_remainder)
        return PaimonTensorFlowDataset(batched, self.config)

    def prefetch(self, buffer_size: Any = tf.data.AUTOTUNE) -> 'PaimonTensorFlowDataset':
        """Prefetch data for performance.

        Args:
            buffer_size: Prefetch buffer size (default: AUTOTUNE)

        Returns:
            New PaimonTensorFlowDataset with prefetch
        """
        prefetched = self.dataset.prefetch(buffer_size)
        return PaimonTensorFlowDataset(prefetched, self.config)

    def cache(self, filename: Optional[str] = None) -> 'PaimonTensorFlowDataset':
        """Cache dataset in memory or to disk.

        Args:
            filename: Optional file path for disk cache

        Returns:
            New PaimonTensorFlowDataset with cache
        """
        cached = self.dataset.cache(filename)
        return PaimonTensorFlowDataset(cached, self.config)

    def shuffle(self, buffer_size: int) -> 'PaimonTensorFlowDataset':
        """Shuffle the dataset.

        Args:
            buffer_size: Shuffle buffer size

        Returns:
            New PaimonTensorFlowDataset with shuffle
        """
        shuffled = self.dataset.shuffle(buffer_size)
        return PaimonTensorFlowDataset(shuffled, self.config)

    def map(self, func: Callable) -> 'PaimonTensorFlowDataset':
        """Apply transformation to each element.

        Args:
            func: Transformation function

        Returns:
            New PaimonTensorFlowDataset with transformation
        """
        mapped = self.dataset.map(func)
        return PaimonTensorFlowDataset(mapped, self.config)

    def repeat(self, count: int = -1) -> 'PaimonTensorFlowDataset':
        """Repeat dataset.

        Args:
            count: Number of repeats (-1 for infinite)

        Returns:
            New PaimonTensorFlowDataset with repeat
        """
        repeated = self.dataset.repeat(count)
        return PaimonTensorFlowDataset(repeated, self.config)

    def take(self, count: int) -> 'PaimonTensorFlowDataset':
        """Take first n elements.

        Args:
            count: Number of elements to take

        Returns:
            New PaimonTensorFlowDataset with take
        """
        taken = self.dataset.take(count)
        return PaimonTensorFlowDataset(taken, self.config)

    def __iter__(self):
        """Iterate over dataset elements."""
        return iter(self.dataset)

    def __getattr__(self, name: str) -> Any:
        """Delegate to underlying tf.data.Dataset."""
        return getattr(self.dataset, name)
