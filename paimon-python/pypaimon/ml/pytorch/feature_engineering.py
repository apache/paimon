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
"""Select features by variance.

Features with large variance are considered more informative.
Feature engineering module.

Provides common feature processing techniques:
- Feature normalization (standardization, min-max scaling)
- Feature encoding (one-hot encoding, label encoding)
- Feature crossing
- Feature selection
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
    """Feature transformer base class."""

    @abstractmethod
    def fit(self, X: Any) -> 'FeatureTransformer':
        """Learn transformation parameters."""
        pass

    @abstractmethod
    def transform(self, X: Any) -> Any:
        """Transform features."""
        pass

    def fit_transform(self, X: Any) -> Any:
        """Fit and transform features."""
        return self.fit(X).transform(X)


class StandardScaler(FeatureTransformer):
    """Standardization transformer (Z-score normalization).

    Convert features to zero mean, unit variance.

    Example:
        >>> scaler = StandardScaler()
        >>> X_train = [[1, 2], [3, 4], [5, 6]]
        >>> scaler.fit(X_train)
        >>> X_test = [[2, 3]]
        >>> X_test_scaled = scaler.transform(X_test)
    """

    def __init__(self):
        """Initialize standardization transformer."""
        self.mean = None
        self.std = None

    def fit(self, X: Any) -> 'StandardScaler':
        """Learn mean and standard deviation."""
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        X = np.asarray(X)
        self.mean = np.mean(X, axis=0)
        self.std = np.std(X, axis=0)
        self.std[self.std == 0] = 1  # Avoiding division by zero

        logger.debug(f"StandardScaler fitting completed: mean shape={self.mean.shape}")
        return self

    def transform(self, X: Any) -> Any:
        """Apply standardization."""
        if self.mean is None:
            raise RuntimeError("Need to call fit() first")

        X = np.asarray(X)
        return (X - self.mean) / self.std


class MinMaxScaler(FeatureTransformer):
    """Min-Max scaler.

    Scale features to [0, 1] range.

    Example:
        >>> scaler = MinMaxScaler()
        >>> scaler.fit([[1], [2], [3]])
        >>> scaler.transform([[2]])  # Return [[0.5]]
    """

    def __init__(self, feature_range: Tuple[float, float] = (0, 1)):
        """Initialize MinMax scaler.

        Args:
            feature_range: target range (min, max)
        """
        self.feature_range = feature_range
        self.min_val = None
        self.max_val = None

    def fit(self, X: Any) -> 'MinMaxScaler':
        """Learn min and max values."""
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        X = np.asarray(X)
        self.min_val = np.min(X, axis=0)
        self.max_val = np.max(X, axis=0)

        logger.debug(f"MinMaxScaler fitting completed: range=({self.min_val.min()}, {self.max_val.max()})")
        return self

    def transform(self, X: Any) -> Any:
        """Apply min-max scaling."""
        if self.min_val is None:
            raise RuntimeError("Need to call fit() first")

        X = np.asarray(X)
        X_scaled = (X - self.min_val) / (self.max_val - self.min_val)
        feature_min, feature_max = self.feature_range
        return X_scaled * (feature_max - feature_min) + feature_min


class OneHotEncoder(FeatureTransformer):
    """One-Hot encoder.

    Convert categorical features to one-hot vectors.

    Example:
        >>> encoder = OneHotEncoder()
        >>> encoder.fit([['red'], ['green'], ['blue']])
        >>> encoder.transform([['red']])  # Return [[1, 0, 0]]
    """

    def __init__(self, handle_unknown: str = 'error'):
        """Initialize one-hot encoder.

        Args:
            handle_unknown: Handle unseen values ('error' or 'ignore')
        """
        self.categories = None
        self.handle_unknown = handle_unknown

    def fit(self, X: List[Any]) -> 'OneHotEncoder':
        """Learn categories."""
        self.categories = {}
        for feature_idx, feature_values in enumerate(zip(*X)):
            self.categories[feature_idx] = list(set(feature_values))
            self.categories[feature_idx].sort()

        logger.debug(f"OneHotEncoder fitting completed: {len(self.categories)} features")
        return self

    def transform(self, X: List[Any]) -> List[List[float]]:
        """Apply one-hot encoding."""
        if self.categories is None:
            raise RuntimeError("Need to call fit() first")

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
                        raise ValueError(f"Unknown category value: {value}")
                    else:  # ignore
                        encoded.extend([0.0] * len(categories))

            result.append(encoded)

        return result


class FeatureNormalizer:
    """Feature normalization utility.

    Handle feature missing values, outliers, etc.

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
        """Handle missing values.

        Args:
            X: Input data
            strategy: Handle strategy ('mean', 'median', 'forward_fill', 'value')
            fill_value: Fill value when using 'value' strategy

        Returns:
            Data after handling
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
            raise ValueError(f"unknown strategy: {strategy}")

        # Fill NaN values
        for i in range(X.shape[1]):
            mask = np.isnan(X[:, i])
            if strategy == 'value':
                X[mask, i] = fill_value
            else:
                X[mask, i] = fill_values[i]

        logger.debug(f"Handle missing values completed, strategy={strategy}")
        return X

    @staticmethod
    def handle_outliers(
        X: Any,
        method: str = 'iqr',
        threshold: float = 3.0
    ) -> Any:
        """Handle outliers.

        Args:
            X: Input data
            method: Detection method ('iqr' or 'zscore')
            threshold: Threshold

        Returns:
            Data after handling
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
            raise ValueError(f"unknown method: {method}")

        logger.debug(f"Handle outliers completed, method={method}")
        return X


class FeatureSelector:
    """Feature selection utility.

    Select important features based on variance and relevance to label.

    Example:
        >>> selector = FeatureSelector()
        >>> selected_indices = selector.select_by_variance(X, k=10)
    """

    @staticmethod
    def select_by_variance(X: Any, k: int) -> List[int]:
        """Select features by variance.

        Features with large variance are considered more informative.

        Args:
            X: Input data
            k: number of selected features

        Returns:
            list of selected feature indices
        """
        if not NUMPY_AVAILABLE:
            raise ImportError("NumPy is required")

        X = np.asarray(X)
        variances = np.var(X, axis=0)
        selected = np.argsort(variances)[-k:]

        logger.debug(f"Selected {k} features by variance")
        return selected.tolist()

    @staticmethod
    def select_by_correlation(
        X: Any,
        y: Any,
        k: int
    ) -> List[int]:
        """Select features by correlation with label.

        Args:
            X: input features
            y: label
            k: number of selected features

        Returns:
            list of selected feature indices
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

        logger.debug(f"Selected {k} features by correlation")
        return selected.tolist()
