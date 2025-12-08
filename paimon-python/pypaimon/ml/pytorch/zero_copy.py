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
零拷贝转换和内存优化模块。

提供高效的 Arrow 到 PyTorch 张量转换，避免不必要的数据复制。
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
    """零拷贝数据转换器。

    最小化 Arrow → NumPy → PyTorch 转换过程中的数据复制，
    利用内存的连续性和指针共享。

    Example:
        >>> converter = ZeroCopyConverter()
        >>> arrow_batch = pa.RecordBatch.from_pydict({'a': [1, 2, 3]})
        >>> tensor = converter.convert_to_tensor(arrow_batch, 'a')
    """

    def __init__(self):
        """初始化零拷贝转换器。"""
        if not all([NUMPY_AVAILABLE, TORCH_AVAILABLE, PYARROW_AVAILABLE]):
            raise ImportError("NumPy, PyTorch 和 PyArrow are required")

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def convert_to_tensor(
        self,
        arrow_array: Any,
        dtype: Optional[torch.dtype] = None,
        copy: bool = False
    ) -> torch.Tensor:
        """将 Arrow 数组转换为 PyTorch 张量。

        Args:
            arrow_array: PyArrow Array 或 ChunkedArray
            dtype: 目标张量数据类型
            copy: 是否强制复制（默认尽量避免）

        Returns:
            PyTorch 张量

        Example:
            >>> arrow_array = pa.array([1.0, 2.0, 3.0])
            >>> tensor = converter.convert_to_tensor(arrow_array, torch.float32)
        """
        try:
            # 处理 ChunkedArray
            if isinstance(arrow_array, pa.ChunkedArray):
                if arrow_array.num_chunks == 1:
                    arrow_array = arrow_array.chunk(0)
                else:
                    # 多个 chunk，合并为一个
                    arrow_array = arrow_array.combine_chunks()

            # 转换为 NumPy（尽量零拷贝）
            numpy_array = arrow_array.to_numpy(zero_copy_only=not copy)

            # 转换为 PyTorch 张量
            if numpy_array.flags['C_CONTIGUOUS']:
                # C 连续内存，可以直接创建张量
                tensor = torch.from_numpy(numpy_array)
                if dtype and tensor.dtype != dtype:
                    tensor = tensor.to(dtype)
            else:
                # 非连续内存，需要复制
                tensor = torch.from_numpy(np.ascontiguousarray(numpy_array))
                if dtype and tensor.dtype != dtype:
                    tensor = tensor.to(dtype)

            self.logger.debug(
                f"转换完成：Arrow {arrow_array.type} → "
                f"Tensor {tensor.shape} {tensor.dtype}"
            )

            return tensor

        except Exception as e:
            self.logger.error(f"零拷贝转换失败：{e}", exc_info=True)
            raise

    def convert_batch_to_tensors(
        self,
        arrow_batch: Any,
        column_names: List[str],
        dtypes: Optional[Dict[str, torch.dtype]] = None
    ) -> Dict[str, torch.Tensor]:
        """将 Arrow RecordBatch 转换为张量字典。

        最小化内存复制，直接利用 Arrow 内存块。

        Args:
            arrow_batch: PyArrow RecordBatch
            column_names: 列名列表
            dtypes: 列名到数据类型的映射

        Returns:
            列名到张量的字典

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
                    raise ValueError(f"列 {col_name} 不存在")

                arrow_col = arrow_batch[col_name]
                target_dtype = dtypes.get(col_name)

                tensor = self.convert_to_tensor(arrow_col, target_dtype)
                tensors[col_name] = tensor

            self.logger.debug(f"批转换完成：{len(tensors)} 列")
            return tensors

        except Exception as e:
            self.logger.error(f"批转换失败：{e}", exc_info=True)
            raise

    @staticmethod
    def estimate_memory_usage(arrow_array: Any) -> int:
        """估计 Arrow 数组的内存使用量。

        Args:
            arrow_array: PyArrow Array

        Returns:
            内存使用量（字节）
        """
        if isinstance(arrow_array, pa.ChunkedArray):
            total = 0
            for chunk in arrow_array.chunks:
                total += chunk.nbytes
            return total
        else:
            return arrow_array.nbytes


class AdaptiveBatchSizer:
    """自适应批大小调整器。

    根据内存使用情况动态调整批处理大小。

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
        """初始化自适应批大小调整器。

        Args:
            initial_batch_size: 初始批大小
            max_memory_mb: 最大内存限制（MB）
            growth_factor: 增长因子（内存充足时）
            shrink_factor: 收缩因子（内存不足时）
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
        """根据样本张量计算最优批大小。

        Args:
            sample_tensor: 单个样本张量
            target_memory_ratio: 目标内存使用比例（0-1）

        Returns:
            建议的批大小
        """
        try:
            sample_memory = sample_tensor.element_size() * sample_tensor.nelement()
            max_batch_size = int(
                (self.max_memory_bytes * target_memory_ratio) / sample_memory
            )

            # 调整到 2 的幂
            batch_size = 2 ** int(np.log2(max(max_batch_size, 1)))
            batch_size = max(self.initial_batch_size, batch_size)

            self.logger.info(
                f"计算批大小：样本内存={sample_memory} bytes，"
                f"推荐批大小={batch_size}"
            )

            return batch_size

        except Exception as e:
            self.logger.error(f"批大小计算失败：{e}", exc_info=True)
            return self.initial_batch_size

    def adjust_batch_size(self, current_memory_usage: float) -> int:
        """根据实际内存使用动态调整批大小。

        Args:
            current_memory_usage: 当前内存使用（字节）

        Returns:
            调整后的批大小
        """
        try:
            memory_ratio = current_memory_usage / self.max_memory_bytes

            if memory_ratio > 0.95:
                # 内存紧张，缩小批大小
                self.current_batch_size = max(
                    1,
                    int(self.current_batch_size * self.shrink_factor)
                )
                action = "缩小"
            elif memory_ratio < 0.5:
                # 内存充裕，增大批大小
                self.current_batch_size = int(
                    self.current_batch_size * self.growth_factor
                )
                action = "增大"
            else:
                action = "保持"

            self.logger.info(
                f"批大小调整({action})：内存使用={memory_ratio*100:.1f}%，"
                f"新批大小={self.current_batch_size}"
            )

            return self.current_batch_size

        except Exception as e:
            self.logger.error(f"批大小调整失败：{e}", exc_info=True)
            return self.current_batch_size


class MemoryPoolManager:
    """内存池管理器。

    管理 PyTorch 内存分配，减少频繁分配/释放的开销。

    Example:
        >>> manager = MemoryPoolManager(pool_size_mb=256)
        >>> tensor = manager.allocate((100, 100), torch.float32)
    """

    def __init__(self, pool_size_mb: int = 256):
        """初始化内存池管理器。

        Args:
            pool_size_mb: 内存池大小（MB）
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.pool_size_bytes = pool_size_mb * 1024 * 1024
        self.free_blocks: List[torch.Tensor] = []
        self.used_blocks: Dict[int, torch.Tensor] = {}

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info(f"初始化内存池，大小={pool_size_mb} MB")

    def allocate(
        self,
        shape: tuple,
        dtype: torch.dtype = torch.float32,
        device: Optional[str] = None
    ) -> torch.Tensor:
        """从内存池分配张量。

        Args:
            shape: 张量形状
            dtype: 数据类型
            device: 设备（'cpu' 或 'cuda'）

        Returns:
            分配的张量
        """
        try:
            device = device or ('cuda' if torch.cuda.is_available() else 'cpu')
            needed_size = int(np.prod(shape))

            # 尝试从空闲块中找到合适的块
            for i, block in enumerate(self.free_blocks):
                if block.numel() >= needed_size and block.device.type == device:
                    # 使用找到的块
                    tensor = block[:needed_size].reshape(shape).clone().detach()
                    self.free_blocks.pop(i)
                    self.used_blocks[id(tensor)] = tensor

                    self.logger.debug(f"从空闲块分配内存：大小={needed_size}")
                    return tensor

            # 没有合适的块，分配新块
            tensor = torch.zeros(shape, dtype=dtype, device=device)
            self.used_blocks[id(tensor)] = tensor

            self.logger.debug(f"分配新内存块：大小={needed_size}")
            return tensor

        except Exception as e:
            self.logger.error(f"内存分配失败：{e}", exc_info=True)
            raise

    def release(self, tensor: torch.Tensor) -> None:
        """将张量归还到内存池。

        Args:
            tensor: 要归还的张量
        """
        try:
            tensor_id = id(tensor)
            if tensor_id in self.used_blocks:
                del self.used_blocks[tensor_id]

                # 如果内存池未满，添加到空闲块
                if len(self.free_blocks) * tensor.numel() < self.pool_size_bytes:
                    self.free_blocks.append(tensor.detach())
                    self.logger.debug("张量归还到内存池")
                else:
                    self.logger.debug("内存池已满，释放张量")

        except Exception as e:
            self.logger.error(f"内存释放失败：{e}", exc_info=True)

    def get_stats(self) -> Dict[str, Any]:
        """获取内存池统计信息。

        Returns:
            统计信息字典
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
