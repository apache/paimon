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
Online feature engineering module。

Provides dynamic feature computation during training, including:
- Sliding window aggregation
- Time decay features
- Interaction feature computation
- Real-time feature cache
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import defaultdict
from abc import ABC, abstractmethod

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

logger = logging.getLogger(__name__)


class OnlineFeatureComputer(ABC):
    """Online feature computer base class."""

    @abstractmethod
    def compute(self, data: Any) -> Any:
        """Compute features."""
        pass


class SlidingWindowAggregator(OnlineFeatureComputer):
    """Sliding window aggregator。

    Sliding window aggregation based on time or sequence length。

    Example:
        >>> aggregator = SlidingWindowAggregator(
        ...     window_size=7,
        ...     agg_functions={'sum': np.sum, 'mean': np.mean}
        ... )
        >>> features = aggregator.aggregate(values)
    """

    def __init__(
        self,
        window_size: int,
        agg_functions: Dict[str, Callable],
        step_size: int = 1
    ):
        """Initialize sliding window aggregator。

        Args:
            window_size: Window size
            agg_functions: Aggregation functions dictionary {'name': function}
            step_size: Window step size
        """
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        self.window_size = window_size
        self.agg_functions = agg_functions
        self.step_size = step_size

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def aggregate(self, values: List[float]) -> List[Dict[str, float]]:
        """Apply sliding window aggregation to a sequence of values.

        Args:
            values: List of values

        Returns:
            List of aggregation results for each window position

        Example:
            >>> aggregator = SlidingWindowAggregator(
            ...     window_size=3,
            ...     agg_functions={'sum': np.sum}
            ... )
            >>> results = aggregator.aggregate([1, 2, 3, 4, 5])
            >>> # results = [
            >>> #     {'sum': 6},    # window [1, 2, 3]
            >>> #     {'sum': 9},    # window [2, 3, 4]
            >>> #     {'sum': 12}    # window [3, 4, 5]
            >>> # ]
        """
        try:
            values = np.asarray(values)
            results = []

            for start in range(0, len(values) - self.window_size + 1, self.step_size):
                window = values[start:start + self.window_size]
                agg_result = {}

                for name, func in self.agg_functions.items():
                    agg_result[name] = float(func(window))

                results.append(agg_result)

            self.logger.debug(f"Sliding window aggregation completed: {len(results)} windows")
            return results

        except Exception as e:
            self.logger.error(f"Sliding window aggregation failed: {e}", exc_info=True)
            raise

    def compute(self, data: Any) -> Any:
        """Compute features (implementing ABC interface)."""
        if isinstance(data, list):
            return self.aggregate(data)
        return data


class TimeDecayFeatureBuilder(OnlineFeatureComputer):
    """Time decay feature builder。

    Compute time series features using exponential decay weights。

    Example:
        >>> builder = TimeDecayFeatureBuilder(decay_factor=0.9)
        >>> features = builder.build(values, timestamps)
    """

    def __init__(
        self,
        decay_factor: float = 0.9,
        aggregation: str = 'weighted_mean'
    ):
        """Initialize time decay feature builder.

        Args:
            decay_factor: Decay factor（0-1）
            aggregation: Aggregation method ('weighted_mean', 'weighted_sum')
        """
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        if not 0 < decay_factor <= 1:
            raise ValueError("decay_factor must be in (0, 1] range")

        self.decay_factor = decay_factor
        self.aggregation = aggregation

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def build(
        self,
        values: List[float],
        timestamps: Optional[List[int]] = None
    ) -> Dict[str, float]:
        """Build time decay features。

        Args:
            values: List of values
            timestamps: List of timestamps (default to using index)

        Returns:
            Feature dictionary

        Example:
            >>> builder = TimeDecayFeatureBuilder(decay_factor=0.9)
            >>> features = builder.build(
            ...     values=[1.0, 2.0, 3.0],
            ...     timestamps=[0, 1, 2]
            ... )
        """
        try:
            values = np.asarray(values)

            if timestamps is None:
                timestamps = np.arange(len(values))
            else:
                timestamps = np.asarray(timestamps)

            # Calculate decay weights (latest data has largest weight)
            time_diff = timestamps[-1] - timestamps
            weights = np.power(self.decay_factor, time_diff)

            # Normalize weights
            weights = weights / weights.sum()

            if self.aggregation == 'weighted_mean':
                result = float(np.sum(values * weights))
            elif self.aggregation == 'weighted_sum':
                result = float(np.sum(values * weights))
            else:
                raise ValueError(f"Unknown aggregation method: {self.aggregation}")

            self.logger.debug(f"Time decay feature builder completed: {result:.4f}")

            return {
                'time_decay_feature': result,
                'max_weight': float(weights.max()),
                'weight_std': float(weights.std())
            }

        except Exception as e:
            self.logger.error(f"Time decay feature builder failed: {e}", exc_info=True)
            raise

    def compute(self, data: Tuple[List[float], List[int]]) -> Dict[str, float]:
        """Compute features (implementing ABC interface)."""
        values, timestamps = data
        return self.build(values, timestamps)


class InteractionFeatureBuilder:
    """Interaction feature builder。

    Compute interaction features between features (product, difference, ratio, etc.)。

    Example:
        >>> builder = InteractionFeatureBuilder()
        >>> interactions = builder.build_interactions(
        ...     {'a': 2, 'b': 3},
        ...     interactions=[('a', 'b', 'mul')]
        ... )
    """

    def __init__(self):
        """Initialize interaction feature builder。"""
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def build_interactions(
        self,
        features: Dict[str, float],
        interactions: List[Tuple[str, str, str]]
    ) -> Dict[str, float]:
        """Build interaction features。

        Args:
            features: Feature dictionary
            interactions: Interaction list [(feature1, feature2, operation)]
                         operation can be: 'mul', 'add', 'sub', 'div', 'pow'

        Returns:
            Dictionary containing interaction features

        Example:
            >>> features = {'age': 30, 'income': 50000}
            >>> interactions = [('age', 'income', 'mul')]
            >>> result = builder.build_interactions(features, interactions)
            >>> # result = {'age': 30, 'income': 50000, 'age_x_income': 1500000}
        """
        try:
            result = dict(features)

            for feat1, feat2, op in interactions:
                if feat1 not in features or feat2 not in features:
                    self.logger.warning(
                        f"Missing features: {feat1} or {feat2} does not exist"
                    )
                    continue

                val1 = features[feat1]
                val2 = features[feat2]

                if op == 'mul':
                    interaction_value = val1 * val2
                    interaction_name = f"{feat1}_x_{feat2}"
                elif op == 'add':
                    interaction_value = val1 + val2
                    interaction_name = f"{feat1}_plus_{feat2}"
                elif op == 'sub':
                    interaction_value = val1 - val2
                    interaction_name = f"{feat1}_minus_{feat2}"
                elif op == 'div':
                    interaction_value = val1 / val2 if val2 != 0 else 0
                    interaction_name = f"{feat1}_div_{feat2}"
                elif op == 'pow':
                    interaction_value = val1 ** val2
                    interaction_name = f"{feat1}_pow_{feat2}"
                else:
                    self.logger.warning(f"Unknown operation: {op}")
                    continue

                result[interaction_name] = interaction_value

            self.logger.debug(
                f"Interaction features construction completed: {len(interactions)} interactions"
            )
            return result

        except Exception as e:
            self.logger.error(f"Interaction feature construction failed: {e}", exc_info=True)
            raise


class FeatureCache:
    """Feature cache。

    Cache computed features to avoid recomputation。

    Example:
        >>> cache = FeatureCache(max_size=10000)
        >>> cache.put('user_123', computed_features)
        >>> features = cache.get('user_123')
    """

    def __init__(self, max_size: int = 10000):
        """Initialize feature cache.

        Args:
            max_size: Maximum number of cache entries
        """
        self.max_size = max_size
        self.cache: Dict[str, Any] = {}
        self.access_count = defaultdict(int)

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def put(self, key: str, value: Any) -> None:
        """Store features to cache.

        Args:
            key: Cache key (e.g., user ID)
            value: Feature values
        """
        try:
            # If cache is full, remove least recently used item
            if len(self.cache) >= self.max_size:
                # LRU eviction strategy
                lru_key = min(self.access_count, key=self.access_count.get)
                del self.cache[lru_key]
                del self.access_count[lru_key]
                self.logger.debug(f"Cache full, evicting: {lru_key}")

            self.cache[key] = value
            self.access_count[key] = 0

            self.logger.debug(f"Feature cache：{key}")

        except Exception as e:
            self.logger.error(f"Cache storage failed: {e}", exc_info=True)

    def get(self, key: str) -> Optional[Any]:
        """Get features from cache.

        Args:
            key: Cache key

        Returns:
            Cached feature values, return None if not exists
        """
        try:
            if key in self.cache:
                self.access_count[key] += 1
                self.logger.debug(f"Cache hit: {key}")
                return self.cache[key]

            self.logger.debug(f"Cache miss: {key}")
            return None

        except Exception as e:
            self.logger.error(f"Cache read failed: {e}", exc_info=True)
            return None

    def clear(self) -> None:
        """Clear the cache."""
        self.cache.clear()
        self.access_count.clear()
        self.logger.info("Feature cache cleared")

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics.

        Returns:
            Statistics dictionary
        """
        total_accesses = sum(self.access_count.values())
        return {
            'cache_size': len(self.cache),
            'max_size': self.max_size,
            'total_accesses': total_accesses,
            'hit_rate': total_accesses / len(self.access_count) if self.access_count else 0,
        }
