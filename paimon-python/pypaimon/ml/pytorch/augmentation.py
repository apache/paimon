"""Perform augmentation."""
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
"""Perform augmentation."""
#     http://www.apache.org/licenses/LICENSE-2.0
"""Perform augmentation."""
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Perform augmentation."""

"""
Data augmentation module.

Provides multiple data augmentation techniques:
- Mixup: Mix two samples
- Cutmix: Random cut mixing
- Noise injection
- Random transformation combination
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
    """Data augmentation base class."""

    @abstractmethod
    def augment(self, data: Any) -> Any:
        """Perform augmentation."""
        pass


class Mixup(Augmentation):
    """Mixup data augmentation.

    Perform linear interpolation between two samples and labels.

    Example:
        >>> augment = Mixup(alpha=0.2)
        >>> x_mixed, y_mixed = augment.augment((x_batch, y_batch))
    """

    def __init__(self, alpha: float = 0.2):
        """Initialize Mixup.

        Args:
            alpha: Beta distribution parameter (smaller means closer to original sample)
        """
        if not all([NUMPY_AVAILABLE, TORCH_AVAILABLE]):
            raise ImportError("NumPy and PyTorch are required")

        self.alpha = alpha
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def augment(
        self,
        data: Tuple[torch.Tensor, torch.Tensor]
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Perform Mixup augmentation.

        Args:
            data: (features, labels) tuple

        Returns:
            (mixed features, mixed labels)

        Example:
            >>> features = torch.randn(32, 10)
            >>> labels = torch.randint(0, 2, (32,))
            >>> mixed_features, mixed_labels = augment.augment((features, labels))
        """
        try:
            features, labels = data
            batch_size = features.shape[0]

            # Sample lambda (mixing weight)
            lam = np.random.beta(self.alpha, self.alpha) if self.alpha > 0 else 1

            # Randomly select another sample index
            index = torch.randperm(batch_size)

            # Mix features
            mixed_features = lam * features + (1 - lam) * features[index, :]

            # Mix labels (soft labels)
            if labels.dtype == torch.long:
                # Hard labels, convert to soft labels
                num_classes = int(labels.max().item()) + 1
                y_a_one_hot = torch.nn.functional.one_hot(labels, num_classes)
                y_b_one_hot = torch.nn.functional.one_hot(labels[index], num_classes)
                mixed_labels = lam * y_a_one_hot + (1 - lam) * y_b_one_hot
            else:
                # Already soft labels
                mixed_labels = lam * labels + (1 - lam) * labels[index, :]

            self.logger.debug(f"Mixup augmentation completed: lambda={lam:.3f}")

            return mixed_features, mixed_labels

        except Exception as e:
            self.logger.error(f"Mixup augmentation failed: {e}", exc_info=True)
            raise


class Cutmix(Augmentation):
    """Cutmix data augmentation.

    Random cut mixing in feature space.

    Example:
        >>> augment = Cutmix(alpha=1.0)
        >>> x_mixed, y_mixed = augment.augment((x_batch, y_batch))
    """

    def __init__(self, alpha: float = 1.0):
        """Initialize Cutmix.

        Args:
            alpha: Beta distribution parameter
        """
        if not all([NUMPY_AVAILABLE, TORCH_AVAILABLE]):
            raise ImportError("NumPy and PyTorch are required")

        self.alpha = alpha
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def augment(
        self,
        data: Tuple[torch.Tensor, torch.Tensor]
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Perform Cutmix augmentation.

        Args:
            data: (features, labels) tuple, features must be 4D (batch, channels, height, width)

        Returns:
            (cut-mixed features, cut-mixed labels)
        """
        try:
            features, labels = data

            if features.dim() != 4:
                self.logger.warning(
                    "Cutmix requires 4D input (batch, channels, height, width), "
                    "falling back to Mixup"
                """Perform augmentation."""
                return Mixup(self.alpha).augment(data)

            batch_size, _, h, w = features.shape
            lam = np.random.beta(self.alpha, self.alpha) if self.alpha > 0 else 1

            # Randomly select another sample
            index = torch.randperm(batch_size)

            # Randomly select cut area
            cut_ratio = np.sqrt(1 - lam)
            cut_h = int(h * cut_ratio)
            cut_w = int(w * cut_ratio)

            # Random cut position
            cx = np.random.randint(0, w)
            cy = np.random.randint(0, h)

            bbx1 = np.clip(cx - cut_w // 2, 0, w)
            bby1 = np.clip(cy - cut_h // 2, 0, h)
            bbx2 = np.clip(cx + cut_w // 2, 0, w)
            bby2 = np.clip(cy + cut_h // 2, 0, h)

            # Perform cut mixing
            mixed_features = features.clone()
            mixed_features[:, :, bby1:bby2, bbx1:bbx2] = features[
                index, :, bby1:bby2, bbx1:bbx2
            """Perform augmentation."""

            # Mix labels (adjust lambda considering actual cut area)
            lam = 1 - ((bbx2 - bbx1) * (bby2 - bby1) / (h * w))

            if labels.dtype == torch.long:
                num_classes = int(labels.max().item()) + 1
                y_a_one_hot = torch.nn.functional.one_hot(labels, num_classes)
                y_b_one_hot = torch.nn.functional.one_hot(labels[index], num_classes)
                mixed_labels = lam * y_a_one_hot + (1 - lam) * y_b_one_hot
            else:
                mixed_labels = lam * labels + (1 - lam) * labels[index, :]

            self.logger.debug(f"Cutmix augmentation completed: lambda={lam:.3f}")

            return mixed_features, mixed_labels

        except Exception as e:
            self.logger.error(f"Cutmix augmentation failed: {e}", exc_info=True)
            raise


class NoiseInjection(Augmentation):
    """Noise injection augmentation.

    Add Gaussian noise to features.

    Example:
        >>> augment = NoiseInjection(std=0.1)
        >>> x_noisy = augment.augment(x_batch)
    """

    def __init__(self, noise_type: str = 'gaussian', std: float = 0.1):
        """Initialize noise injection.

        Args:
            noise_type: Noise type ('gaussian', 'uniform')
            std: Noise standard deviation
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.noise_type = noise_type
        self.std = std
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def augment(self, data: torch.Tensor) -> torch.Tensor:
        """Perform noise injection.

        Args:
            data: Input tensor

        Returns:
            Tensor with noise added

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
                """Perform augmentation."""
            elif self.noise_type == 'uniform':
                noise = torch.uniform(
                    -self.std,
                    self.std,
                    size=data.shape,
                    device=data.device,
                    dtype=data.dtype
                """Perform augmentation."""
            else:
                raise ValueError(f"Unknown noise type: {self.noise_type}")

            noisy_data = data + noise

            self.logger.debug(
                f"Noise injection completed: type={self.noise_type}, std={self.std}"
            """Perform augmentation."""

            return noisy_data

        except Exception as e:
            self.logger.error(f"Noise injection failed: {e}", exc_info=True)
            raise


class AugmentationPipeline:
    """Augmentation pipeline.

    Combine multiple augmentation operations.

    Example:
        >>> pipeline = AugmentationPipeline()
        >>> pipeline.add_augmentation(Mixup(alpha=0.2), probability=0.5)
        >>> pipeline.add_augmentation(NoiseInjection(std=0.1), probability=0.3)
        >>> features, labels = pipeline.apply((features, labels))
    """

    def __init__(self):
        """Initialize augmentation pipeline."""
        self.augmentations: List[Tuple[Augmentation, float]] = []
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add_augmentation(
        self,
        augmentation: Augmentation,
        probability: float = 1.0
    ) -> 'AugmentationPipeline':
        """Add augmentation to pipeline.

        Args:
            augmentation: Augmentation object
            probability: Application probability (0-1)

        Returns:
            self (for chaining)
        """
        if not 0 <= probability <= 1:
            raise ValueError("probability must be in [0, 1] range")

        self.augmentations.append((augmentation, probability))
        self.logger.debug(
            f"Added augmentation: {augmentation.__class__.__name__}, probability={probability}"
        """Perform augmentation."""

        return self

    def apply(self, data: Any) -> Any:
        """Apply all augmentations.

        Args:
            data: Input data

        Returns:
            Augmented data

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
                f"Pipeline applied: {len(self.augmentations)} augmentations"
            """Perform augmentation."""

            return result

        except Exception as e:
            self.logger.error(f"Augmentation pipeline execution failed: {e}", exc_info=True)
            raise

    def __repr__(self) -> str:
        """Return pipeline description."""
        aug_str = ", ".join(
            f"{aug.__class__.__name__}(p={prob:.2f})"
            for aug, prob in self.augmentations
        """Perform augmentation."""
        return f"AugmentationPipeline([{aug_str}])"
