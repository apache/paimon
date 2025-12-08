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
Data transformation utilities for PyTorch integration.

Provides utilities for transforming Paimon data (Arrow format) to
PyTorch-compatible tensors and applying custom transformations.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, List, Optional

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

import pyarrow as pa

logger = logging.getLogger(__name__)


class DataTransformer(ABC):
    """Abstract base class for data transformers.

    Transformers are responsible for converting raw data (e.g., Arrow batches)
    into formats suitable for PyTorch training.
    """

    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Transform input data.

        Args:
            data: Input data to transform

        Returns:
            Transformed data
        """
        pass


class ArrowToTorchTransformer(DataTransformer):
    """Transformer for converting Arrow data to PyTorch tensors.

    This transformer handles type conversions from PyArrow types to
    PyTorch tensor types, with support for batching and normalization.

    Args:
        dtype: Target PyTorch tensor dtype (default: torch.float32)
        normalize: Whether to normalize numeric features (default: False)
        handle_missing: How to handle missing values: 'drop', 'mean', 'zero' (default: 'zero')
    """

    def __init__(
        self,
        dtype: Optional[Any] = None,
        normalize: bool = False,
        handle_missing: str = 'zero'
    ):
        """Initialize ArrowToTorchTransformer.

        Args:
            dtype: Target PyTorch dtype
            normalize: Enable feature normalization
            handle_missing: Strategy for missing values

        Raises:
            ImportError: If PyTorch not available
            ValueError: If invalid handle_missing strategy
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        if handle_missing not in ('drop', 'mean', 'zero'):
            raise ValueError(
                f"Invalid handle_missing strategy: {handle_missing}. "
                "Must be one of: 'drop', 'mean', 'zero'"
            )

        self.dtype = dtype or torch.float32
        self.normalize = normalize
        self.handle_missing = handle_missing
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def transform(self, arrow_batch: pa.RecordBatch) -> torch.Tensor:
        """Transform Arrow RecordBatch to PyTorch tensor.

        Args:
            arrow_batch: PyArrow RecordBatch

        Returns:
            PyTorch tensor

        Raises:
            ValueError: If batch is empty
            TypeError: If data types are incompatible
        """
        try:
            if arrow_batch.num_rows == 0:
                raise ValueError("Cannot transform empty batch")

            # Convert to pandas for easier manipulation
            df = arrow_batch.to_pandas()

            # Handle missing values
            if self.handle_missing == 'drop':
                df = df.dropna()
            elif self.handle_missing == 'mean':
                df = df.fillna(df.mean())
            elif self.handle_missing == 'zero':
                df = df.fillna(0)

            # Convert to numpy array then tensor
            data = df.values
            tensor = torch.tensor(data, dtype=self.dtype)

            # Normalize if requested
            if self.normalize:
                mean = tensor.mean(dim=0)
                std = tensor.std(dim=0)
                std[std == 0] = 1  # Avoid division by zero
                tensor = (tensor - mean) / std

            return tensor

        except Exception as e:
            self.logger.error(f"Error transforming Arrow batch: {e}", exc_info=True)
            raise


class ColumnSelector:
    """Utility for selecting and reordering columns from Arrow data.

    Args:
        columns: List of column names to select
        required: Whether all columns must be present (default: True)
    """

    def __init__(self, columns: List[str], required: bool = True):
        """Initialize ColumnSelector.

        Args:
            columns: Column names to select
            required: Raise error if columns not found

        Raises:
            ValueError: If columns is empty
        """
        if not columns:
            raise ValueError("At least one column must be specified")

        self.columns = columns
        self.required = required
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def select(self, arrow_batch: pa.RecordBatch) -> pa.RecordBatch:
        """Select columns from Arrow batch.

        Args:
            arrow_batch: Input Arrow RecordBatch

        Returns:
            RecordBatch with selected columns

        Raises:
            ValueError: If required columns not found
        """
        try:
            available = set(arrow_batch.column_names)
            requested = set(self.columns)

            missing = requested - available
            if missing and self.required:
                raise ValueError(
                    f"Missing required columns: {missing}. "
                    f"Available columns: {available}"
                )

            # Select columns that exist
            cols_to_select = [c for c in self.columns if c in available]
            selected = arrow_batch.select(cols_to_select)

            self.logger.debug(f"Selected {len(cols_to_select)} columns")
            return selected

        except Exception as e:
            self.logger.error(f"Error selecting columns: {e}", exc_info=True)
            raise


class ComposedTransformer(DataTransformer):
    """Composes multiple transformers sequentially.

    Applies transformers in order, passing output of one as input to the next.

    Args:
        transformers: List of transformers to apply in order
    """

    def __init__(self, transformers: List[DataTransformer]):
        """Initialize ComposedTransformer.

        Args:
            transformers: List of transformers

        Raises:
            ValueError: If transformers list is empty
        """
        if not transformers:
            raise ValueError("At least one transformer must be provided")

        self.transformers = transformers
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def transform(self, data: Any) -> Any:
        """Apply all transformers sequentially.

        Args:
            data: Input data

        Returns:
            Transformed data

        Raises:
            RuntimeError: If any transformer fails
        """
        try:
            result = data
            for i, transformer in enumerate(self.transformers):
                result = transformer.transform(result)
                self.logger.debug(f"Applied transformer {i + 1}/{len(self.transformers)}")
            return result
        except Exception as e:
            self.logger.error(f"Error in composed transformation: {e}", exc_info=True)
            raise


def create_default_transformer(
    normalize: bool = False,
    dtype: Optional[Any] = None
) -> ArrowToTorchTransformer:
    """Factory function to create default Arrow-to-Torch transformer.

    Args:
        normalize: Enable feature normalization
        dtype: Target tensor dtype

    Returns:
        Configured ArrowToTorchTransformer instance
    """
    if dtype is None and TORCH_AVAILABLE:
        dtype = torch.float32

    return ArrowToTorchTransformer(dtype=dtype, normalize=normalize)
