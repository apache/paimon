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
PyTorch Dataset implementations for Paimon tables.

Provides PaimonIterableDataset and PaimonMapDataset for efficient
streaming and random access to Paimon data.
"""

import logging
from typing import Any, Callable, List, Optional, Tuple, Iterator
from dataclasses import dataclass, field

try:
    import torch
    from torch.utils.data import IterableDataset, Dataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    IterableDataset = object  # type: ignore
    Dataset = object  # type: ignore
    torch = None  # type: ignore

import pyarrow as pa

logger = logging.getLogger(__name__)


@dataclass
class PaimonDatasetConfig:
    """Configuration for Paimon PyTorch Dataset.

    Attributes:
        transform: Optional custom transformation function for data.
        target_column: Optional label column name for supervised learning.
        feature_columns: List of feature column names. If empty, all columns except target are used.
        tensor_type: PyTorch tensor type for output (default: torch.float32).
        batch_size: Default batch size hint (used for map dataset pre-allocation).
        prefetch_batches: Number of batches to prefetch in streaming.
        shuffle_buffer_size: Size of shuffle buffer for map dataset (0 = no shuffle).
    """

    transform: Optional[Callable] = None
    target_column: Optional[str] = None
    feature_columns: List[str] = field(default_factory=list)
    tensor_type: Any = None
    batch_size: int = 32
    prefetch_batches: int = 2
    shuffle_buffer_size: int = 0

    def __post_init__(self):
        """Validate and initialize configuration."""
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required. Install with: pip install torch")

        if self.tensor_type is None:
            self.tensor_type = torch.float32


class PaimonIterableDataset(IterableDataset):
    """Iterable PyTorch Dataset for streaming Paimon data.

    This dataset is designed for large-scale data training where the entire
    dataset doesn't fit in memory. It supports multi-worker data loading with
    automatic sharding across workers.

    Key features:
    - Streaming read via Arrow batch reader
    - Automatic worker sharding using Paimon's split mechanism
    - Memory-efficient batch processing
    - Custom data transformation pipeline
    - Type conversion from Arrow to PyTorch tensors

    Args:
        read_builder: Paimon ReadBuilder for configuring table read
        config: Optional PaimonDatasetConfig for customization

    Example:
        >>> from pypaimon import CatalogFactory
        >>> from pypaimon.ml.pytorch import PaimonIterableDataset
        >>> from torch.utils.data import DataLoader
        >>>
        >>> catalog = CatalogFactory.create({'warehouse': '/path/to/warehouse'})
        >>> table = catalog.get_table('default.training_data')
        >>> read_builder = table.new_read_builder()
        >>> dataset = PaimonIterableDataset(read_builder=read_builder)
        >>> dataloader = DataLoader(dataset, batch_size=32, num_workers=4)
        >>> for batch in dataloader:
        ...     features, labels = batch
    """

    def __init__(self, read_builder: Any, config: Optional[PaimonDatasetConfig] = None):
        """Initialize PaimonIterableDataset.

        Args:
            read_builder: Configured ReadBuilder from Paimon table
            config: Optional dataset configuration

        Raises:
            ImportError: If PyTorch is not installed
            ValueError: If read_builder is invalid
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required. Install with: pip install torch")

        if read_builder is None:
            raise ValueError("read_builder cannot be None")

        self.read_builder = read_builder
        self.config = config or PaimonDatasetConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __iter__(self) -> 'Iterator[Tuple[Any, Optional[Any]]]':
        """Iterate over batches of data.

        This method implements worker sharding by using get_worker_info() to
        determine the worker ID and total number of workers. Each worker
        automatically receives its portion of the data using Paimon's
        with_shard() mechanism.

        Yields:
            Tuple of (features, labels) as PyTorch tensors
        """
        try:
            worker_info = torch.utils.data.get_worker_info()

            # Create scan and determine splits
            scan = self.read_builder.new_scan()

            if worker_info is None:
                # Single process: read all splits
                self.logger.debug("Single process mode - reading all splits")
                splits = scan.plan().splits()
            else:
                # Multi-process: auto-shard across workers
                shard_index = worker_info.id
                total_shards = worker_info.num_workers
                self.logger.debug(
                    f"Multi-process mode - worker {shard_index} of {total_shards}"
                )
                splits = scan.with_shard(shard_index, total_shards).plan().splits()

            # Create table read and batch reader
            table_read = self.read_builder.new_read()
            batch_reader = table_read.to_arrow_batch_reader(splits)

            # Process batches
            batch_count = 0
            for arrow_batch in batch_reader:
                batch_count += 1
                features, labels = self._process_arrow_batch(arrow_batch)

                if features is not None:
                    yield (features, labels)

            self.logger.info(f"Processed {batch_count} batches")

        except Exception as e:
            self.logger.error(f"Error during iteration: {e}", exc_info=True)
            raise

    def _process_arrow_batch(
        self, arrow_batch: pa.RecordBatch
    ) -> 'Tuple[Optional[Any], Optional[Any]]':
        """Process Arrow RecordBatch into PyTorch tensors.

        Args:
            arrow_batch: PyArrow RecordBatch from Paimon

        Returns:
            Tuple of (features, labels) as PyTorch tensors

        Raises:
            ValueError: If target column not found in batch
        """
        try:
            # Convert Arrow batch to pandas for easier manipulation
            df = arrow_batch.to_pandas()

            # Determine feature columns
            if not self.config.feature_columns:
                feature_cols = [
                    col
                    for col in df.columns
                    if col != self.config.target_column
                ]
            else:
                feature_cols = self.config.feature_columns

            # Extract features
            features_data = df[feature_cols].values
            features = torch.tensor(
                features_data,
                dtype=self.config.tensor_type
            )

            # Extract labels if configured
            labels = None
            if self.config.target_column:
                if self.config.target_column not in df.columns:
                    raise ValueError(
                        f"Target column '{self.config.target_column}' not found "
                        f"in batch. Available columns: {list(df.columns)}"
                    )
                labels_data = df[self.config.target_column].values
                labels = torch.tensor(labels_data)

            # Apply custom transformation if provided
            if self.config.transform:
                features = self.config.transform(features)
                if labels is not None:
                    labels = self.config.transform(labels)

            return features, labels

        except Exception as e:
            self.logger.error(f"Error processing batch: {e}", exc_info=True)
            raise


class PaimonMapDataset(Dataset):
    """Map-style PyTorch Dataset for random access to Paimon data.

    This dataset is suitable for scenarios requiring random access and
    sampling (e.g., with PyTorch Sampler). It loads the entire table
    into memory (or Arrow table).

    Key features:
    - Random access via __getitem__
    - Optional shuffling support
    - Supports PyTorch Sampler for advanced sampling strategies
    - Efficient Arrow table caching

    Args:
        read_builder: Paimon ReadBuilder for configuring table read
        config: Optional PaimonDatasetConfig for customization

    Example:
        >>> from pypaimon import CatalogFactory
        >>> from pypaimon.ml.pytorch import PaimonMapDataset
        >>> from torch.utils.data import DataLoader, RandomSampler
        >>>
        >>> catalog = CatalogFactory.create({'warehouse': '/path/to/warehouse'})
        >>> table = catalog.get_table('default.training_data')
        >>> read_builder = table.new_read_builder()
        >>> dataset = PaimonMapDataset(read_builder=read_builder)
        >>> sampler = RandomSampler(dataset)
        >>> dataloader = DataLoader(dataset, sampler=sampler, batch_size=32)
    """

    def __init__(self, read_builder: Any, config: Optional[PaimonDatasetConfig] = None):
        """Initialize PaimonMapDataset.

        Args:
            read_builder: Configured ReadBuilder from Paimon table
            config: Optional dataset configuration

        Raises:
            ImportError: If PyTorch is not installed
            ValueError: If read_builder is invalid
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required. Install with: pip install torch")

        if read_builder is None:
            raise ValueError("read_builder cannot be None")

        self.read_builder = read_builder
        self.config = config or PaimonDatasetConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Load data
        self._load_data()

    def _load_data(self) -> None:
        """Load entire table as Arrow table and prepare indices."""
        try:
            self.logger.info("Loading data from Paimon table...")

            # Get all splits and read into Arrow table
            scan = self.read_builder.new_scan()
            splits = scan.plan().splits()
            table_read = self.read_builder.new_read()
            self.arrow_table = table_read.to_arrow(splits)

            if self.arrow_table is None:
                raise RuntimeError("Failed to load data from Paimon table")

            self.num_rows = self.arrow_table.num_rows
            self.logger.info(f"Loaded {self.num_rows} rows from Paimon table")

            # Create indices for optional shuffling
            self.indices = list(range(self.num_rows))

            # Apply shuffle if configured
            if self.config.shuffle_buffer_size > 0:
                import random
                random.shuffle(self.indices)
                self.logger.info(
                    f"Applied shuffling with buffer size {self.config.shuffle_buffer_size}"
                )

        except Exception as e:
            self.logger.error(f"Error loading data: {e}", exc_info=True)
            raise

    def __len__(self) -> int:
        """Return the number of samples in the dataset."""
        return self.num_rows

    def __getitem__(self, idx: int) -> 'Tuple[Any, Optional[Any]]':
        """Get a single sample by index.

        Args:
            idx: Index of the sample

        Returns:
            Tuple of (features, labels) as PyTorch tensors

        Raises:
            IndexError: If index is out of bounds
        """
        if idx < 0 or idx >= self.num_rows:
            raise IndexError(f"Index {idx} out of range [0, {self.num_rows})")

        try:
            # Use actual index if shuffled
            actual_idx = self.indices[idx]

            # Get row as Python dict
            row_data = {}
            for col_name in self.arrow_table.column_names:
                col = self.arrow_table.column(col_name)
                row_data[col_name] = col[actual_idx].as_py()

            # Extract features
            if not self.config.feature_columns:
                feature_cols = [
                    col
                    for col in row_data.keys()
                    if col != self.config.target_column
                ]
            else:
                feature_cols = self.config.feature_columns

            features = torch.tensor(
                [row_data[col] for col in feature_cols],
                dtype=self.config.tensor_type
            )

            # Extract labels
            labels = None
            if self.config.target_column:
                if self.config.target_column not in row_data:
                    raise ValueError(
                        f"Target column '{self.config.target_column}' not found "
                        f"in table. Available columns: {list(row_data.keys())}"
                    )
                labels = torch.tensor(row_data[self.config.target_column])

            # Apply transformation
            if self.config.transform:
                features = self.config.transform(features)
                if labels is not None:
                    labels = self.config.transform(labels)

            return features, labels

        except Exception as e:
            self.logger.error(f"Error getting item at index {idx}: {e}", exc_info=True)
            raise
