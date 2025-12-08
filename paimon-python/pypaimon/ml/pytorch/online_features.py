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
在线特征工程模块。

提供在训练过程中动态计算特征，包括：
- 滑动窗口聚合
- 时间衰减特征
- 交互特征计算
- 实时特征缓存
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
    """在线特征计算器基类。"""

    @abstractmethod
    def compute(self, data: Any) -> Any:
        """计算特征。"""
        pass


class SlidingWindowAggregator(OnlineFeatureComputer):
    """滑动窗口聚合器。

    基于时间或序列长度的滑动窗口聚合。

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
        """初始化滑动窗口聚合器。

        Args:
            window_size: 窗口大小
            agg_functions: 聚合函数字典 {'name': function}
            step_size: 窗口步长
        """
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        self.window_size = window_size
        self.agg_functions = agg_functions
        self.step_size = step_size

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def aggregate(self, values: List[float]) -> List[Dict[str, float]]:
        """对值序列进行滑动窗口聚合。

        Args:
            values: 值列表

        Returns:
            每个窗口位置的聚合结果列表

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

            self.logger.debug(f"滑动窗口聚合完成：{len(results)} 个窗口")
            return results

        except Exception as e:
            self.logger.error(f"滑动窗口聚合失败：{e}", exc_info=True)
            raise

    def compute(self, data: Any) -> Any:
        """计算特征（实现 ABC 接口）。"""
        if isinstance(data, list):
            return self.aggregate(data)
        return data


class TimeDecayFeatureBuilder(OnlineFeatureComputer):
    """时间衰减特征构建器。

    使用指数衰减权重计算时间序列特征。

    Example:
        >>> builder = TimeDecayFeatureBuilder(decay_factor=0.9)
        >>> features = builder.build(values, timestamps)
    """

    def __init__(
        self,
        decay_factor: float = 0.9,
        aggregation: str = 'weighted_mean'
    ):
        """初始化时间衰减特征构建器。

        Args:
            decay_factor: 衰减因子（0-1）
            aggregation: 聚合方式 ('weighted_mean', 'weighted_sum')
        """
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        if not 0 < decay_factor <= 1:
            raise ValueError("decay_factor 必须在 (0, 1] 范围内")

        self.decay_factor = decay_factor
        self.aggregation = aggregation

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def build(
        self,
        values: List[float],
        timestamps: Optional[List[int]] = None
    ) -> Dict[str, float]:
        """构建时间衰减特征。

        Args:
            values: 值列表
            timestamps: 时间戳列表（默认使用索引）

        Returns:
            特征字典

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

            # 计算衰减权重（最新数据权重最大）
            time_diff = timestamps[-1] - timestamps
            weights = np.power(self.decay_factor, time_diff)

            # 归一化权重
            weights = weights / weights.sum()

            if self.aggregation == 'weighted_mean':
                result = float(np.sum(values * weights))
            elif self.aggregation == 'weighted_sum':
                result = float(np.sum(values * weights))
            else:
                raise ValueError(f"未知的聚合方式：{self.aggregation}")

            self.logger.debug(f"时间衰减特征构建完成：{result:.4f}")

            return {
                'time_decay_feature': result,
                'max_weight': float(weights.max()),
                'weight_std': float(weights.std())
            }

        except Exception as e:
            self.logger.error(f"时间衰减特征构建失败：{e}", exc_info=True)
            raise

    def compute(self, data: Tuple[List[float], List[int]]) -> Dict[str, float]:
        """计算特征（实现 ABC 接口）。"""
        values, timestamps = data
        return self.build(values, timestamps)


class InteractionFeatureBuilder:
    """交互特征构建器。

    计算特征之间的交互特征（乘积、差、比等）。

    Example:
        >>> builder = InteractionFeatureBuilder()
        >>> interactions = builder.build_interactions(
        ...     {'a': 2, 'b': 3},
        ...     interactions=[('a', 'b', 'mul')]
        ... )
    """

    def __init__(self):
        """初始化交互特征构建器。"""
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def build_interactions(
        self,
        features: Dict[str, float],
        interactions: List[Tuple[str, str, str]]
    ) -> Dict[str, float]:
        """构建交互特征。

        Args:
            features: 特征字典
            interactions: 交互列表 [(feature1, feature2, operation)]
                         operation 可以是: 'mul', 'add', 'sub', 'div', 'pow'

        Returns:
            包含交互特征的字典

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
                        f"特征缺失：{feat1} 或 {feat2} 不存在"
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
                    self.logger.warning(f"未知的操作：{op}")
                    continue

                result[interaction_name] = interaction_value

            self.logger.debug(
                f"交互特征构建完成：{len(interactions)} 个交互"
            )
            return result

        except Exception as e:
            self.logger.error(f"交互特征构建失败：{e}", exc_info=True)
            raise


class FeatureCache:
    """特征缓存。

    缓存已计算的特征，避免重复计算。

    Example:
        >>> cache = FeatureCache(max_size=10000)
        >>> cache.put('user_123', computed_features)
        >>> features = cache.get('user_123')
    """

    def __init__(self, max_size: int = 10000):
        """初始化特征缓存。

        Args:
            max_size: 最大缓存条目数
        """
        self.max_size = max_size
        self.cache: Dict[str, Any] = {}
        self.access_count = defaultdict(int)

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def put(self, key: str, value: Any) -> None:
        """存储特征到缓存。

        Args:
            key: 缓存键（例如：用户 ID）
            value: 特征值
        """
        try:
            # 如果缓存满了，删除最少访问的项
            if len(self.cache) >= self.max_size:
                # LRU 淘汰策略
                lru_key = min(self.access_count, key=self.access_count.get)
                del self.cache[lru_key]
                del self.access_count[lru_key]
                self.logger.debug(f"缓存满，淘汰项：{lru_key}")

            self.cache[key] = value
            self.access_count[key] = 0

            self.logger.debug(f"特征缓存：{key}")

        except Exception as e:
            self.logger.error(f"缓存存储失败：{e}", exc_info=True)

    def get(self, key: str) -> Optional[Any]:
        """从缓存获取特征。

        Args:
            key: 缓存键

        Returns:
            缓存的特征值，如果不存在返回 None
        """
        try:
            if key in self.cache:
                self.access_count[key] += 1
                self.logger.debug(f"缓存命中：{key}")
                return self.cache[key]

            self.logger.debug(f"缓存未命中：{key}")
            return None

        except Exception as e:
            self.logger.error(f"缓存读取失败：{e}", exc_info=True)
            return None

    def clear(self) -> None:
        """清空缓存。"""
        self.cache.clear()
        self.access_count.clear()
        self.logger.info("特征缓存已清空")

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息。

        Returns:
            统计字典
        """
        total_accesses = sum(self.access_count.values())
        return {
            'cache_size': len(self.cache),
            'max_size': self.max_size,
            'total_accesses': total_accesses,
            'hit_rate': total_accesses / len(self.access_count) if self.access_count else 0,
        }
