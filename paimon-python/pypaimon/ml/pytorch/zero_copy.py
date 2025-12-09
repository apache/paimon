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
Zero-copy conversion and memory optimization module.

Provides efficient Arrow to PyTorch tensor conversion, avoiding unnecessary data copies.
"""

import logging
from typing import Any, Dict, Optional, List

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

logger = logging.getLogger(__name__)


class ZeroCopyConverter:
    """Zero-copy data converter.

    Minimizes data copies during Arrow → NumPy → PyTorch conversion,
    leveraging memory continuity and pointer sharing.

    Example:
        >>> converter = ZeroCopyConverter()
        >>> arrow_batch = pa.RecordBatch.from_pydict({'a': [1, 2, 3]})
        >>> tensor = converter.convert_to_tensor(arrow_batch, 'a')
    """

    def __init__(self):
        """Initialize zero-copy converter."""
        if not all([NUMPY_AVAILABLE, TORCH_AVAILABLE, PYARROW_AVAILABLE]):
            raise ImportError("NumPy, PyTorch and PyArrow are required")

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def convert_to_tensor(
        self,
        arrow_array: Any,
        dtype: Optional[torch.dtype] = None,
        copy: bool = False
    ) -> torch.Tensor:
        """Convert Arrow array to PyTorch tensor.

        Args:
            arrow_array: PyArrow Array or ChunkedArray
            dtype: Target tensor data type
            copy: Whether to force copy (default tries to avoid)

        Returns:
            PyTorch tensor

        Example:
            >>> arrow_array = pa.array([1.0, 2.0, 3.0])
            >>> tensor = converter.convert_to_tensor(arrow_array, torch.float32)
        """
        try:
            # Handle ChunkedArray
            if isinstance(arrow_array, pa.ChunkedArray):
                if arrow_array.num_chunks == 1:
                    arrow_array = arrow_array.chunk(0)
                else:
                    # Multiple chunks, combine into one
                    arrow_array = arrow_array.combine_chunks()

            # Convert to NumPy (try to zero-copy)
            numpy_array = arrow_array.to_numpy(zero_copy_only=not copy)

            # Convert to PyTorch tensor
            if numpy_array.flags['C_CONTIGUOUS']:
                # C-contiguous memory, can create tensor directly
                tensor = torch.from_numpy(numpy_array)
                if dtype and tensor.dtype != dtype:
                    tensor = tensor.to(dtype)
            else:
                # Non-contiguous memory, needs copy
                tensor = torch.from_numpy(np.ascontiguousarray(numpy_array))
                if dtype and tensor.dtype != dtype:
                    tensor = tensor.to(dtype)

            self.logger.debug(
                f"Conversion completed: Arrow {arrow_array.type} → "
                f"Tensor {tensor.shape} {tensor.dtype}"
            )

            return tensor

        except Exception as e:
            self.logger.error(f"Zero-copy conversion failed: {e}", exc_info=True)
            raise

    def convert_batch_to_tensors(
        self,
        arrow_batch: Any,
        column_names: List[str],
        dtypes: Optional[Dict[str, torch.dtype]] = None
    ) -> Dict[str, torch.Tensor]:
        """Convert Arrow RecordBatch to tensor dictionary.

        Minimizes memory copying, directly utilizing Arrow memory blocks.

        Args:
            arrow_batch: PyArrow RecordBatch
            column_names: List of column names
            dtypes: Mapping of column names to data types

        Returns:
            Dictionary of column names to tensors

        Example:
            >>> batch = pa.RecordBatch.from_pydict({
            ...     'x': [1, 2, 3], 'y': [4, 5, 6]
            ... })
            >>> tensors = converter.convert_batch_to_tensors(
            ...     batch, ['x', 'y']
            ... )
        """
        try:
            tensors = {}
            dtypes = dtypes or {}

            for col_name in column_names:
                if col_name not in arrow_batch.column_names:
                    raise ValueError(f"Column {col_name} does not exist")

                arrow_col = arrow_batch[col_name]
                target_dtype = dtypes.get(col_name)

                tensor = self.convert_to_tensor(arrow_col, target_dtype)
                tensors[col_name] = tensor

            self.logger.debug(f"Batch conversion completed: {len(tensors)} columns")
            return tensors

        except Exception as e:
            self.logger.error(f"Batch conversion failed: {e}", exc_info=True)
            raise

    @staticmethod
    def estimate_memory_usage(arrow_array: Any) -> int:
        """Estimate memory usage of Arrow array.

        Args:
            arrow_array: PyArrow Array

        Returns:
            Memory usage (bytes)
        """
        if isinstance(arrow_array, pa.ChunkedArray):
            total = 0
            for chunk in arrow_array.chunks:
                total += chunk.nbytes
            return total
        else:
            return arrow_array.nbytes


class AdaptiveBatchSizer:
    """Adaptive batch size adjuster.

    Dynamically adjusts batch size based on memory usage.

    Example:
        >>> sizer = AdaptiveBatchSizer(max_memory_mb=1024)
        >>> batch_size = sizer.calculate_batch_size(sample_tensor)
    """

    def __init__(
        self,
        initial_batch_size: int = 32,
        max_memory_mb: int = 1024,
        growth_factor: float = 1.5,
        shrink_factor: float = 0.8
    ):
        """Initialize adaptive batch size adjuster.

        Args:
            initial_batch_size: Initial batch size
            max_memory_mb: Maximum memory limit (MB)
            growth_factor: Growth factor (when memory is sufficient)
            shrink_factor: Shrink factor (when memory is insufficient)
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.initial_batch_size = initial_batch_size
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.growth_factor = growth_factor
        self.shrink_factor = shrink_factor
        self.current_batch_size = initial_batch_size

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def calculate_batch_size(
        self,
        sample_tensor: torch.Tensor,
        target_memory_ratio: float = 0.8
    ) -> int:
        """Calculate optimal batch size based on sample tensor.

        Args:
            sample_tensor: Single sample tensor
            target_memory_ratio: Target memory usage ratio (0-1)

        Returns:
            Suggested batch size
        """
        try:
            sample_memory = sample_tensor.element_size() * sample_tensor.nelement()
            max_batch_size = int(
                (self.max_memory_bytes * target_memory_ratio) / sample_memory
            )

            # Adjust to power of 2
            batch_size = 2 ** int(np.log2(max(max_batch_size, 1)))
            batch_size = max(self.initial_batch_size, batch_size)

            self.logger.info(
                f"Calculate batch size: sample memory={sample_memory} bytes, "
                f"recommended batch size={batch_size}"
            )

            return batch_size

        except Exception as e:
            self.logger.error(f"Batch size calculation failed: {e}", exc_info=True)
            return self.initial_batch_size

    def adjust_batch_size(self, current_memory_usage: float) -> int:
        """Dynamically adjust batch size based on actual memory usage.

        Args:
            current_memory_usage: Current memory usage (bytes)

        Returns:
            Adjusted batch size
        """
        try:
            memory_ratio = current_memory_usage / self.max_memory_bytes

            if memory_ratio > 0.95:
                # Memory tight, shrink batch size
                self.current_batch_size = max(
                    1,
                    int(self.current_batch_size * self.shrink_factor)
                )
                action = "shrink"
            elif memory_ratio < 0.5:
                # Memory abundant, increase batch size
                self.current_batch_size = int(
                    self.current_batch_size * self.growth_factor
                )
                action = "grow"
            else:
                action = "keep"

            self.logger.info(
                f"Batch size adjusted ({action}): memory usage={memory_ratio*100:.1f}%, "
                f"new batch size={self.current_batch_size}"
            )

            return self.current_batch_size

        except Exception as e:
            self.logger.error(f"Batch size adjustment failed: {e}", exc_info=True)
            return self.current_batch_size


class MemoryPoolManager:
    """Memory pool manager.

    Manages PyTorch memory allocation, reducing overhead of frequent allocation/deallocation.

    Example:
        >>> manager = MemoryPoolManager(pool_size_mb=256)
        >>> tensor = manager.allocate((100, 100), torch.float32)
    """

    def __init__(self, pool_size_mb: int = 256):
        """Initialize memory pool manager.

        Args:
            pool_size_mb: Memory pool size (MB)
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.pool_size_bytes = pool_size_mb * 1024 * 1024
        self.free_blocks: List[torch.Tensor] = []
        self.used_blocks: Dict[int, torch.Tensor] = {}

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info(f"Initialized memory pool, size={pool_size_mb} MB")

    def allocate(
        self,
        shape: tuple,
        dtype: torch.dtype = torch.float32,
        device: Optional[str] = None
    ) -> torch.Tensor:
        """Allocate tensor from memory pool.

        Args:
            shape: Tensor shape
            dtype: Data type
            device: Device ('cpu' or 'cuda')

        Returns:
            Allocated tensor
        """
        try:
            device = device or ('cuda' if torch.cuda.is_available() else 'cpu')
            needed_size = int(np.prod(shape))

            # Try to find a suitable block from free blocks
            for i, block in enumerate(self.free_blocks):
                if block.numel() >= needed_size and block.device.type == device:
                    # Use the found block
                    tensor = block[:needed_size].reshape(shape).clone().detach()
                    self.free_blocks.pop(i)
                    self.used_blocks[id(tensor)] = tensor

                    self.logger.debug(f"Allocated memory from free block: size={needed_size}")
                    return tensor

            # No suitable block, allocate new one
            tensor = torch.zeros(shape, dtype=dtype, device=device)
            self.used_blocks[id(tensor)] = tensor

            self.logger.debug(f"Allocated new memory block: size={needed_size}")
            return tensor

        except Exception as e:
            self.logger.error(f"Memory allocation failed: {e}", exc_info=True)
            raise

    def release(self, tensor: torch.Tensor) -> None:
        """Return tensor to memory pool.

        Args:
            tensor: Tensor to return
        """
        try:
            tensor_id = id(tensor)
            if tensor_id in self.used_blocks:
                del self.used_blocks[tensor_id]

                # If memory pool is not full, add to free blocks
                if len(self.free_blocks) * tensor.numel() < self.pool_size_bytes:
                    self.free_blocks.append(tensor.detach())
                    self.logger.debug("Tensor returned to memory pool")
                else:
                    self.logger.debug("Memory pool full, releasing tensor")

        except Exception as e:
            self.logger.error(f"Memory release failed: {e}", exc_info=True)

    def get_stats(self) -> Dict[str, Any]:
        """Get memory pool statistics.

        Returns:
            Statistics dictionary
        """
        total_used = sum(t.numel() for t in self.used_blocks.values())
        total_free = sum(t.numel() for t in self.free_blocks)

        return {
            'used_blocks': len(self.used_blocks),
            'free_blocks': len(self.free_blocks),
            'total_used_bytes': total_used * 4,
            'total_free_bytes': total_free * 4,
            'pool_size_bytes': self.pool_size_bytes,
            'utilization_percent': (
                (total_used / (total_used + total_free) * 100)
                if (total_used + total_free) > 0 else 0
            ),
        }
