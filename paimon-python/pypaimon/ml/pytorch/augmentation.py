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
数据增强模块。

提供多种数据增强技术：
- Mixup：混合两个样本
- Cutmix：随机剪切混合
- 噪声注入
- 随机变换组合
"""

import logging
import random
from typing import Any, List, Tuple
from abc import ABC, abstractmethod

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

logger = logging.getLogger(__name__)


class Augmentation(ABC):
    """数据增强基类。"""

    @abstractmethod
    def augment(self, data: Any) -> Any:
        """执行增强。"""
        pass


class Mixup(Augmentation):
    """Mixup 数据增强。

    在两个样本和标签之间进行线性插值。

    Example:
        >>> augment = Mixup(alpha=0.2)
        >>> x_mixed, y_mixed = augment.augment((x_batch, y_batch))
    """

    def __init__(self, alpha: float = 0.2):
        """初始化 Mixup。

        Args:
            alpha: Beta 分布参数（越小越接近原样本）
        """
        if not all([NUMPY_AVAILABLE, TORCH_AVAILABLE]):
            raise ImportError("NumPy 和 PyTorch are required")

        self.alpha = alpha
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def augment(
        self,
        data: Tuple[torch.Tensor, torch.Tensor]
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """执行 Mixup 增强。

        Args:
            data: (features, labels) 元组

        Returns:
            (混合特征, 混合标签)

        Example:
            >>> features = torch.randn(32, 10)
            >>> labels = torch.randint(0, 2, (32,))
            >>> mixed_features, mixed_labels = augment.augment((features, labels))
        """
        try:
            features, labels = data
            batch_size = features.shape[0]

            # 采样 lambda（混合权重）
            lam = np.random.beta(self.alpha, self.alpha) if self.alpha > 0 else 1

            # 随机选择另一个样本索引
            index = torch.randperm(batch_size)

            # 混合特征
            mixed_features = lam * features + (1 - lam) * features[index, :]

            # 混合标签（软标签）
            if labels.dtype == torch.long:
                # 硬标签，转换为软标签
                num_classes = int(labels.max().item()) + 1
                y_a_one_hot = torch.nn.functional.one_hot(labels, num_classes)
                y_b_one_hot = torch.nn.functional.one_hot(labels[index], num_classes)
                mixed_labels = lam * y_a_one_hot + (1 - lam) * y_b_one_hot
            else:
                # 已经是软标签
                mixed_labels = lam * labels + (1 - lam) * labels[index, :]

            self.logger.debug(f"Mixup 增强完成：lambda={lam:.3f}")

            return mixed_features, mixed_labels

        except Exception as e:
            self.logger.error(f"Mixup 增强失败：{e}", exc_info=True)
            raise


class Cutmix(Augmentation):
    """Cutmix 数据增强。

    在特征空间中随机剪切混合。

    Example:
        >>> augment = Cutmix(alpha=1.0)
        >>> x_mixed, y_mixed = augment.augment((x_batch, y_batch))
    """

    def __init__(self, alpha: float = 1.0):
        """初始化 Cutmix。

        Args:
            alpha: Beta 分布参数
        """
        if not all([NUMPY_AVAILABLE, TORCH_AVAILABLE]):
            raise ImportError("NumPy 和 PyTorch are required")

        self.alpha = alpha
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def augment(
        self,
        data: Tuple[torch.Tensor, torch.Tensor]
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """执行 Cutmix 增强。

        Args:
            data: (features, labels) 元组，特征必须是 4D (batch, channels, height, width)

        Returns:
            (剪切混合特征, 剪切混合标签)
        """
        try:
            features, labels = data

            if features.dim() != 4:
                self.logger.warning(
                    "Cutmix 要求 4D 输入 (batch, channels, height, width)，"
                    "降级为 Mixup"
                )
                return Mixup(self.alpha).augment(data)

            batch_size, _, h, w = features.shape
            lam = np.random.beta(self.alpha, self.alpha) if self.alpha > 0 else 1

            # 随机选择另一个样本
            index = torch.randperm(batch_size)

            # 随机选择剪切区域
            cut_ratio = np.sqrt(1 - lam)
            cut_h = int(h * cut_ratio)
            cut_w = int(w * cut_ratio)

            # 随机剪切位置
            cx = np.random.randint(0, w)
            cy = np.random.randint(0, h)

            bbx1 = np.clip(cx - cut_w // 2, 0, w)
            bby1 = np.clip(cy - cut_h // 2, 0, h)
            bbx2 = np.clip(cx + cut_w // 2, 0, w)
            bby2 = np.clip(cy + cut_h // 2, 0, h)

            # 执行剪切混合
            mixed_features = features.clone()
            mixed_features[:, :, bby1:bby2, bbx1:bbx2] = features[
                index, :, bby1:bby2, bbx1:bbx2
            ]

            # 混合标签（调整 lambda 考虑实际剪切面积）
            lam = 1 - ((bbx2 - bbx1) * (bby2 - bby1) / (h * w))

            if labels.dtype == torch.long:
                num_classes = int(labels.max().item()) + 1
                y_a_one_hot = torch.nn.functional.one_hot(labels, num_classes)
                y_b_one_hot = torch.nn.functional.one_hot(labels[index], num_classes)
                mixed_labels = lam * y_a_one_hot + (1 - lam) * y_b_one_hot
            else:
                mixed_labels = lam * labels + (1 - lam) * labels[index, :]

            self.logger.debug(f"Cutmix 增强完成：lambda={lam:.3f}")

            return mixed_features, mixed_labels

        except Exception as e:
            self.logger.error(f"Cutmix 增强失败：{e}", exc_info=True)
            raise


class NoiseInjection(Augmentation):
    """噪声注入增强。

    向特征添加高斯噪声。

    Example:
        >>> augment = NoiseInjection(std=0.1)
        >>> x_noisy = augment.augment(x_batch)
    """

    def __init__(self, noise_type: str = 'gaussian', std: float = 0.1):
        """初始化噪声注入。

        Args:
            noise_type: 噪声类型 ('gaussian', 'uniform')
            std: 噪声标准差
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.noise_type = noise_type
        self.std = std
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def augment(self, data: torch.Tensor) -> torch.Tensor:
        """执行噪声注入。

        Args:
            data: 输入张量

        Returns:
            添加噪声后的张量

        Example:
            >>> data = torch.randn(32, 10)
            >>> noisy_data = augment.augment(data)
        """
        try:
            if self.noise_type == 'gaussian':
                noise = torch.normal(
                    mean=0,
                    std=self.std,
                    size=data.shape,
                    device=data.device,
                    dtype=data.dtype
                )
            elif self.noise_type == 'uniform':
                noise = torch.uniform(
                    -self.std,
                    self.std,
                    size=data.shape,
                    device=data.device,
                    dtype=data.dtype
                )
            else:
                raise ValueError(f"未知的噪声类型：{self.noise_type}")

            noisy_data = data + noise

            self.logger.debug(
                f"噪声注入完成：类型={self.noise_type}，std={self.std}"
            )

            return noisy_data

        except Exception as e:
            self.logger.error(f"噪声注入失败：{e}", exc_info=True)
            raise


class AugmentationPipeline:
    """增强管道。

    组合多个增强操作。

    Example:
        >>> pipeline = AugmentationPipeline()
        >>> pipeline.add_augmentation(Mixup(alpha=0.2), probability=0.5)
        >>> pipeline.add_augmentation(NoiseInjection(std=0.1), probability=0.3)
        >>> features, labels = pipeline.apply((features, labels))
    """

    def __init__(self):
        """初始化增强管道。"""
        self.augmentations: List[Tuple[Augmentation, float]] = []
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add_augmentation(
        self,
        augmentation: Augmentation,
        probability: float = 1.0
    ) -> 'AugmentationPipeline':
        """添加增强到管道。

        Args:
            augmentation: 增强对象
            probability: 应用概率（0-1）

        Returns:
            self（用于链式调用）
        """
        if not 0 <= probability <= 1:
            raise ValueError("probability 必须在 [0, 1] 范围内")

        self.augmentations.append((augmentation, probability))
        self.logger.debug(
            f"添加增强：{augmentation.__class__.__name__}，概率={probability}"
        )

        return self

    def apply(self, data: Any) -> Any:
        """应用所有增强。

        Args:
            data: 输入数据

        Returns:
            增强后的数据

        Example:
            >>> pipeline = AugmentationPipeline()
            >>> pipeline.add_augmentation(Mixup(), 0.5)
            >>> enhanced_data = pipeline.apply(data)
        """
        try:
            result = data

            for augmentation, probability in self.augmentations:
                if random.random() < probability:
                    result = augmentation.augment(result)

            self.logger.debug(
                f"管道应用完成：{len(self.augmentations)} 个增强"
            )

            return result

        except Exception as e:
            self.logger.error(f"增强管道执行失败：{e}", exc_info=True)
            raise

    def __repr__(self) -> str:
        """返回管道描述。"""
        aug_str = ", ".join(
            f"{aug.__class__.__name__}(p={prob:.2f})"
            for aug, prob in self.augmentations
        )
        return f"AugmentationPipeline([{aug_str}])"
