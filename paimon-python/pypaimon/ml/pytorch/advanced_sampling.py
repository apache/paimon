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
高级采样策略模块。

提供高级采样方式，用于：
- 类别不平衡处理
- 分层采样
- 优先级采样
- 困难样本挖掘
"""

import logging
import random
from typing import Dict, List, Optional, Callable
from abc import ABC, abstractmethod

try:
    import torch
    from torch.utils.data import Sampler
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

logger = logging.getLogger(__name__)


class AdvancedSampler(Sampler, ABC):
    """高级采样器基类。"""

    @abstractmethod
    def __iter__(self):
        """返回样本索引迭代器。"""
        pass

    @abstractmethod
    def __len__(self):
        """返回数据集长度。"""
        pass


class WeightedRandomSampler(AdvancedSampler):
    """加权随机采样器，用于处理类别不平衡。

    Example:
        >>> weights = [1, 1, 1, 10]  # 最后一个样本权重更高
        >>> sampler = WeightedRandomSampler(weights, num_samples=100)
    """

    def __init__(self, weights: List[float], num_samples: int, replacement: bool = True):
        """初始化加权采样器。

        Args:
            weights: 每个样本的权重
            num_samples: 采样数量
            replacement: 是否有放回采样
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        if len(weights) == 0:
            raise ValueError("weights 不能为空")

        self.weights = torch.as_tensor(weights, dtype=torch.double)
        self.num_samples = num_samples
        self.replacement = replacement

        logger.debug(
            f"WeightedRandomSampler 初始化：采样数={num_samples}，"
            f"权重范围=[{self.weights.min():.2f}, {self.weights.max():.2f}]"
        )

    def __iter__(self):
        """返回加权随机采样的索引。"""
        rand_tensor = torch.multinomial(
            self.weights,
            self.num_samples,
            self.replacement
        )
        return iter(rand_tensor.tolist())

    def __len__(self):
        """返回采样数量。"""
        return self.num_samples


class StratifiedSampler(AdvancedSampler):
    """分层采样器，用于保持各类别比例。

    Example:
        >>> labels = [0, 0, 1, 1, 1, 2]
        >>> sampler = StratifiedSampler(labels, num_samples_per_class=2)
    """

    def __init__(
        self,
        labels: List[int],
        num_samples_per_class: Optional[int] = None,
        proportional: bool = True
    ):
        """初始化分层采样器。

        Args:
            labels: 样本标签列表
            num_samples_per_class: 每个类别的采样数量
            proportional: 是否按原始比例采样
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.labels = labels
        self.num_samples_per_class = num_samples_per_class
        self.proportional = proportional

        # 统计类别分布
        self.class_indices: Dict[int, List[int]] = {}
        for idx, label in enumerate(labels):
            if label not in self.class_indices:
                self.class_indices[label] = []
            self.class_indices[label].append(idx)

        logger.debug(f"StratifiedSampler 初始化：{len(self.class_indices)} 个类别")

    def __iter__(self):
        """返回分层采样的索引。"""
        sampled_indices = []

        for class_id, indices in self.class_indices.items():
            if self.num_samples_per_class:
                num_samples = min(self.num_samples_per_class, len(indices))
            elif self.proportional:
                # 按原始比例采样
                num_samples = max(1, int(len(indices) * 0.8))  # 采样 80%
            else:
                num_samples = len(indices)

            sampled = random.sample(indices, num_samples)
            sampled_indices.extend(sampled)

        random.shuffle(sampled_indices)
        return iter(sampled_indices)

    def __len__(self):
        """返回采样总数。"""
        if self.num_samples_per_class:
            return self.num_samples_per_class * len(self.class_indices)
        else:
            return len(self.labels)


class HardExampleMiningSampler(AdvancedSampler):
    """困难样本挖掘采样器，优先采样困难样本。

    用于训练过程中自动增加困难样本的采样比例。

    Example:
        >>> def compute_difficulty(batch):
        ...     # 返回每个样本的困难度分数（0-1）
        ...     return torch.rand(batch.shape[0])
        >>> sampler = HardExampleMiningSampler(
        ...     num_samples=1000,
        ...     difficulty_fn=compute_difficulty
        ... )
    """

    def __init__(
        self,
        num_samples: int,
        difficulty_scores: Optional[List[float]] = None,
        difficulty_fn: Optional[Callable] = None,
        hard_ratio: float = 0.3
    ):
        """初始化困难样本挖掘采样器。

        Args:
            num_samples: 总采样数
            difficulty_scores: 预计算的困难度分数
            difficulty_fn: 困难度计算函数
            hard_ratio: 困难样本比例
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.num_samples = num_samples
        self.difficulty_scores = difficulty_scores
        self.difficulty_fn = difficulty_fn
        self.hard_ratio = hard_ratio

        if difficulty_scores is None and difficulty_fn is None:
            raise ValueError("必须提供 difficulty_scores 或 difficulty_fn")

        logger.debug(f"HardExampleMiningSampler 初始化：困难样本比例={hard_ratio}")

    def __iter__(self):
        """返回包含困难样本的采样索引。"""
        if self.difficulty_scores is None:
            raise RuntimeError("未设置困难度分数")

        scores = torch.tensor(self.difficulty_scores)
        num_hard = int(self.num_samples * self.hard_ratio)
        num_easy = self.num_samples - num_hard

        # 选择最困难的样本
        _, hard_indices = torch.topk(scores, k=num_hard)

        # 随机选择简单样本
        easy_indices = torch.where(scores < scores.median())[0]
        if len(easy_indices) > num_easy:
            easy_indices = easy_indices[torch.randperm(len(easy_indices))[:num_easy]]
        else:
            # 如果简单样本不足，补充困难样本
            remaining = num_easy - len(easy_indices)
            additional = torch.where(scores >= scores.median())[0]
            if len(additional) > remaining:
                additional = additional[torch.randperm(len(additional))[:remaining]]
            easy_indices = torch.cat([easy_indices, additional])

        sampled_indices = torch.cat([hard_indices, easy_indices])
        return iter(sampled_indices[torch.randperm(len(sampled_indices))].tolist())

    def __len__(self):
        """返回采样数量。"""
        return self.num_samples


class BalancedBatchSampler(AdvancedSampler):
    """平衡批采样器，确保每个批次中各类别比例均衡。

    Example:
        >>> labels = [0, 0, 1, 1, 1, 2, 2]
        >>> sampler = BalancedBatchSampler(labels, batch_size=4)
    """

    def __init__(self, labels: List[int], batch_size: int):
        """初始化平衡批采样器。

        Args:
            labels: 样本标签列表
            batch_size: 批大小
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.labels = labels
        self.batch_size = batch_size

        # 构建类别索引
        self.class_indices: Dict[int, List[int]] = {}
        for idx, label in enumerate(labels):
            if label not in self.class_indices:
                self.class_indices[label] = []
            self.class_indices[label].append(idx)

        logger.debug(
            f"BalancedBatchSampler 初始化：批大小={batch_size}，"
            f"{len(self.class_indices)} 个类别"
        )

    def __iter__(self):
        """返回平衡的批索引。"""
        batches = []
        class_iterators = {
            class_id: iter(random.sample(indices, len(indices)))
            for class_id, indices in self.class_indices.items()
        }

        # 创建平衡批次
        num_classes = len(self.class_indices)
        samples_per_class = self.batch_size // num_classes

        for _ in range(len(self) // self.batch_size):
            batch = []
            for class_id, iterator in class_iterators.items():
                try:
                    # 从每个类别中采样
                    for _ in range(samples_per_class):
                        batch.append(next(iterator))
                except StopIteration:
                    # 重新开始该类别的迭代
                    class_iterators[class_id] = iter(
                        random.sample(self.class_indices[class_id], len(self.class_indices[class_id]))
                    )
                    batch.append(next(class_iterators[class_id]))

            batches.extend(batch)

        return iter(batches[:len(self)])

    def __len__(self):
        """返回总采样数。"""
        return len(self.labels)
