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
特征工程模块。

提供常见的特征处理技术：
- 特征标准化（标准化、最小最大化）
- 特征编码（独热编码、标签编码）
- 特征交叉
- 特征选择
"""

import logging
from typing import Any, List, Optional, Tuple
from abc import ABC, abstractmethod

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

logger = logging.getLogger(__name__)


class FeatureTransformer(ABC):
    """特征转换器基类。"""

    @abstractmethod
    def fit(self, X: Any) -> 'FeatureTransformer':
        """学习转换参数。"""
        pass

    @abstractmethod
    def transform(self, X: Any) -> Any:
        """转换特征。"""
        pass

    def fit_transform(self, X: Any) -> Any:
        """拟合并转换特征。"""
        return self.fit(X).transform(X)


class StandardScaler(FeatureTransformer):
    """标准化转换器（Z-score 标准化）。

    将特征转换为零均值、单位方差。

    Example:
        >>> scaler = StandardScaler()
        >>> X_train = [[1, 2], [3, 4], [5, 6]]
        >>> scaler.fit(X_train)
        >>> X_test = [[2, 3]]
        >>> X_test_scaled = scaler.transform(X_test)
    """

    def __init__(self):
        """初始化标准化转换器。"""
        self.mean = None
        self.std = None

    def fit(self, X: Any) -> 'StandardScaler':
        """学习均值和标准差。"""
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        X = np.asarray(X)
        self.mean = np.mean(X, axis=0)
        self.std = np.std(X, axis=0)
        self.std[self.std == 0] = 1  # 避免除以零

        logger.debug(f"StandardScaler 拟合完成：均值形状={self.mean.shape}")
        return self

    def transform(self, X: Any) -> Any:
        """应用标准化。"""
        if self.mean is None:
            raise RuntimeError("需要先调用 fit()")

        X = np.asarray(X)
        return (X - self.mean) / self.std


class MinMaxScaler(FeatureTransformer):
    """最小最大化转换器。

    将特征缩放到 [0, 1] 范围。

    Example:
        >>> scaler = MinMaxScaler()
        >>> scaler.fit([[1], [2], [3]])
        >>> scaler.transform([[2]])  # 返回 [[0.5]]
    """

    def __init__(self, feature_range: Tuple[float, float] = (0, 1)):
        """初始化最小最大化转换器。

        Args:
            feature_range: 目标范围（min, max）
        """
        self.feature_range = feature_range
        self.min_val = None
        self.max_val = None

    def fit(self, X: Any) -> 'MinMaxScaler':
        """学习最小值和最大值。"""
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        X = np.asarray(X)
        self.min_val = np.min(X, axis=0)
        self.max_val = np.max(X, axis=0)

        logger.debug(f"MinMaxScaler 拟合完成：范围=({self.min_val.min()}, {self.max_val.max()})")
        return self

    def transform(self, X: Any) -> Any:
        """应用最小最大化缩放。"""
        if self.min_val is None:
            raise RuntimeError("需要先调用 fit()")

        X = np.asarray(X)
        X_scaled = (X - self.min_val) / (self.max_val - self.min_val)
        feature_min, feature_max = self.feature_range
        return X_scaled * (feature_max - feature_min) + feature_min


class OneHotEncoder(FeatureTransformer):
    """独热编码转换器。

    将类别特征转换为独热向量。

    Example:
        >>> encoder = OneHotEncoder()
        >>> encoder.fit([['red'], ['green'], ['blue']])
        >>> encoder.transform([['red']])  # 返回 [[1, 0, 0]]
    """

    def __init__(self, handle_unknown: str = 'error'):
        """初始化独热编码转换器。

        Args:
            handle_unknown: 处理未见过的值（'error' 或 'ignore'）
        """
        self.categories = None
        self.handle_unknown = handle_unknown

    def fit(self, X: List[Any]) -> 'OneHotEncoder':
        """学习类别。"""
        self.categories = {}
        for feature_idx, feature_values in enumerate(zip(*X)):
            self.categories[feature_idx] = list(set(feature_values))
            self.categories[feature_idx].sort()

        logger.debug(f"OneHotEncoder 拟合完成：{len(self.categories)} 个特征")
        return self

    def transform(self, X: List[Any]) -> List[List[float]]:
        """应用独热编码。"""
        if self.categories is None:
            raise RuntimeError("需要先调用 fit()")

        result = []
        for sample in X:
            encoded = []
            for feature_idx, value in enumerate(sample):
                if feature_idx in self.categories:
                    categories = self.categories[feature_idx]
                    if value in categories:
                        one_hot = [1.0 if cat == value else 0.0 for cat in categories]
                        encoded.extend(one_hot)
                    elif self.handle_unknown == 'error':
                        raise ValueError(f"未知的类别值：{value}")
                    else:  # ignore
                        encoded.extend([0.0] * len(categories))

            result.append(encoded)

        return result


class FeatureNormalizer:
    """特征归一化工具。

    处理特征缺失值、异常值等。

    Example:
        >>> normalizer = FeatureNormalizer()
        >>> X_clean = normalizer.handle_missing_values(X, strategy='mean')
    """

    @staticmethod
    def handle_missing_values(
        X: Any,
        strategy: str = 'mean',
        fill_value: Optional[float] = None
    ) -> Any:
        """处理缺失值。

        Args:
            X: 输入数据
            strategy: 处理策略（'mean', 'median', 'forward_fill', 'value'）
            fill_value: 当使用 'value' 策略时的填充值

        Returns:
            处理后的数据
        """
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        X = np.asarray(X, dtype=float)

        if strategy == 'mean':
            fill_values = np.nanmean(X, axis=0)
        elif strategy == 'median':
            fill_values = np.nanmedian(X, axis=0)
        elif strategy == 'value':
            fill_values = fill_value
        elif strategy == 'forward_fill':
            X = np.apply_along_axis(
                lambda x: np.where(np.isnan(x))[0],
                axis=0,
                arr=X
            )
            return X
        else:
            raise ValueError(f"未知的策略：{strategy}")

        # 填充 NaN
        for i in range(X.shape[1]):
            mask = np.isnan(X[:, i])
            if strategy == 'value':
                X[mask, i] = fill_value
            else:
                X[mask, i] = fill_values[i]

        logger.debug(f"处理缺失值完成，策略={strategy}")
        return X

    @staticmethod
    def handle_outliers(
        X: Any,
        method: str = 'iqr',
        threshold: float = 3.0
    ) -> Any:
        """处理异常值。

        Args:
            X: 输入数据
            method: 检测方法（'iqr' 或 'zscore'）
            threshold: 阈值

        Returns:
            处理后的数据
        """
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        X = np.asarray(X, dtype=float)

        if method == 'iqr':
            Q1 = np.percentile(X, 25, axis=0)
            Q3 = np.percentile(X, 75, axis=0)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR

            X = np.clip(X, lower_bound, upper_bound)

        elif method == 'zscore':
            mean = np.mean(X, axis=0)
            std = np.std(X, axis=0)
            z_scores = np.abs((X - mean) / std)
            X[z_scores > threshold] = np.nan

        else:
            raise ValueError(f"未知的方法：{method}")

        logger.debug(f"处理异常值完成，方法={method}")
        return X


class FeatureSelector:
    """特征选择工具。

    基于方差、相关性等选择重要特征。

    Example:
        >>> selector = FeatureSelector()
        >>> selected_indices = selector.select_by_variance(X, k=10)
    """

    @staticmethod
    def select_by_variance(X: Any, k: int) -> List[int]:
        """基于方差选择特征。

        方差大的特征被认为更有信息量。

        Args:
            X: 输入数据
            k: 选择的特征数

        Returns:
            选中的特征索引列表
        """
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        X = np.asarray(X)
        variances = np.var(X, axis=0)
        selected = np.argsort(variances)[-k:]

        logger.debug(f"基于方差选择了 {k} 个特征")
        return selected.tolist()

    @staticmethod
    def select_by_correlation(
        X: Any,
        y: Any,
        k: int
    ) -> List[int]:
        """基于与标签的相关性选择特征。

        Args:
            X: 输入特征
            y: 标签
            k: 选择的特征数

        Returns:
            选中的特征索引列表
        """
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        X = np.asarray(X)
        y = np.asarray(y)

        correlations = []
        for i in range(X.shape[1]):
            corr = np.corrcoef(X[:, i], y)[0, 1]
            correlations.append(abs(corr))

        selected = np.argsort(correlations)[-k:]

        logger.debug(f"基于相关性选择了 {k} 个特征")
        return selected.tolist()
