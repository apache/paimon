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
Advanced sampling strategy module.

Provides advanced sampling methods for:
- Class imbalance handling
- Stratified sampling
- Priority sampling
- Hard example mining
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
    """Advanced sampler base class."""

    @abstractmethod
    def __iter__(self):
        """Return iterator of sample indices."""
        pass

    @abstractmethod
    def __len__(self):
        """Return dataset length."""
        pass


class WeightedRandomSampler(AdvancedSampler):
    """Weighted random sampler for handling class imbalance.

    Example:
        >>> weights = [1, 1, 1, 10]  # Last sample has higher weight
        >>> sampler = WeightedRandomSampler(weights, num_samples=100)
    """

    def __init__(self, weights: List[float], num_samples: int, replacement: bool = True):
        """Initialize weighted sampler.

        Args:
            weights: Weight for each sample
            num_samples: Number of samples to draw
            replacement: Whether to sample with replacement
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        if len(weights) == 0:
            raise ValueError("weights cannot be empty")

        self.weights = torch.as_tensor(weights, dtype=torch.double)
        self.num_samples = num_samples
        self.replacement = replacement

        logger.debug(
            f"WeightedRandomSampler initialized: num_samples={num_samples}, "
            f"weight_range=[{self.weights.min():.2f}, {self.weights.max():.2f}]"
        )

    def __iter__(self):
        """Return indices of weighted random sampling."""
        rand_tensor = torch.multinomial(
            self.weights,
            self.num_samples,
            self.replacement
        )
        return iter(rand_tensor.tolist())

    def __len__(self):
        """Return number of samples."""
        return self.num_samples


class StratifiedSampler(AdvancedSampler):
    """Stratified sampler for maintaining class proportions.

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
        """Initialize stratified sampler.

        Args:
            labels: List of sample labels
            num_samples_per_class: Number of samples per class
            proportional: Whether to sample proportionally to original distribution
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.labels = labels
        self.num_samples_per_class = num_samples_per_class
        self.proportional = proportional

        # Count class distribution
        self.class_indices: Dict[int, List[int]] = {}
        for idx, label in enumerate(labels):
            if label not in self.class_indices:
                self.class_indices[label] = []
            self.class_indices[label].append(idx)

        logger.debug(f"StratifiedSampler initialized: {len(self.class_indices)} classes")

    def __iter__(self):
        """Return indices of stratified sampling."""
        sampled_indices = []

        for class_id, indices in self.class_indices.items():
            if self.num_samples_per_class:
                num_samples = min(self.num_samples_per_class, len(indices))
            elif self.proportional:
                # Sample proportionally to original distribution
                num_samples = max(1, int(len(indices) * 0.8))  # Sample 80%
            else:
                num_samples = len(indices)

            sampled = random.sample(indices, num_samples)
            sampled_indices.extend(sampled)

        random.shuffle(sampled_indices)
        return iter(sampled_indices)

    def __len__(self):
        """Return total number of samples."""
        if self.num_samples_per_class:
            return self.num_samples_per_class * len(self.class_indices)
        else:
            return len(self.labels)


class HardExampleMiningSampler(AdvancedSampler):
    """Hard example mining sampler for prioritizing hard samples.

    Automatically increase sampling ratio of hard samples during training.

    Example:
        >>> def compute_difficulty(batch):
        ...     # Return difficulty score (0-1) for each sample
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
        """Initialize hard example mining sampler.

        Args:
            num_samples: Total number of samples
            difficulty_scores: Pre-computed difficulty scores
            difficulty_fn: Function to compute difficulty
            hard_ratio: Ratio of hard samples
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.num_samples = num_samples
        self.difficulty_scores = difficulty_scores
        self.difficulty_fn = difficulty_fn
        self.hard_ratio = hard_ratio

        if difficulty_scores is None and difficulty_fn is None:
            raise ValueError("Must provide difficulty_scores or difficulty_fn")

        logger.debug(f"HardExampleMiningSampler initialized: hard_ratio={hard_ratio}")

    def __iter__(self):
        """Return indices with hard examples."""
        if self.difficulty_scores is None:
            raise RuntimeError("Difficulty scores not set")

        scores = torch.tensor(self.difficulty_scores)
        num_hard = int(self.num_samples * self.hard_ratio)
        num_easy = self.num_samples - num_hard

        # Select hardest samples
        _, hard_indices = torch.topk(scores, k=num_hard)

        # Randomly select easy samples
        easy_indices = torch.where(scores < scores.median())[0]
        if len(easy_indices) > num_easy:
            easy_indices = easy_indices[torch.randperm(len(easy_indices))[:num_easy]]
        else:
            # If not enough easy samples, supplement with hard samples
            remaining = num_easy - len(easy_indices)
            additional = torch.where(scores >= scores.median())[0]
            if len(additional) > remaining:
                additional = additional[torch.randperm(len(additional))[:remaining]]
            easy_indices = torch.cat([easy_indices, additional])

        sampled_indices = torch.cat([hard_indices, easy_indices])
        return iter(sampled_indices[torch.randperm(len(sampled_indices))].tolist())

    def __len__(self):
        """Return number of samples."""
        return self.num_samples


class BalancedBatchSampler(AdvancedSampler):
    """Balanced batch sampler ensuring balanced class proportions in each batch.

    Example:
        >>> labels = [0, 0, 1, 1, 1, 2, 2]
        >>> sampler = BalancedBatchSampler(labels, batch_size=4)
    """

    def __init__(self, labels: List[int], batch_size: int):
        """Initialize balanced batch sampler.

        Args:
            labels: List of sample labels
            batch_size: Batch size
        """
        if not TORCH_AVAILABLE:
            raise ImportError("PyTorch is required")

        self.labels = labels
        self.batch_size = batch_size

        # Build class indices
        self.class_indices: Dict[int, List[int]] = {}
        for idx, label in enumerate(labels):
            if label not in self.class_indices:
                self.class_indices[label] = []
            self.class_indices[label].append(idx)

        logger.debug(
            f"BalancedBatchSampler initialized: batch_size={batch_size}, "
            f"{len(self.class_indices)} classes"
        )

    def __iter__(self):
        """Return balanced batch indices."""
        batches = []
        class_iterators = {
            class_id: iter(random.sample(indices, len(indices)))
            for class_id, indices in self.class_indices.items()
        }

        # Create balanced batches
        num_classes = len(self.class_indices)
        samples_per_class = self.batch_size // num_classes

        for _ in range(len(self) // self.batch_size):
            batch = []
            for class_id, iterator in class_iterators.items():
                try:
                    # Sample from each class
                    for _ in range(samples_per_class):
                        batch.append(next(iterator))
                except StopIteration:
                    # Restart iterator for this class
                    class_iterators[class_id] = iter(
                        random.sample(self.class_indices[class_id], len(self.class_indices[class_id]))
                    )
                    batch.append(next(class_iterators[class_id]))

            batches.extend(batch)

        return iter(batches[:len(self)])

    def __len__(self):
        """Return total number of samples."""
        return len(self.labels)
