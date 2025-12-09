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
Batch collation utilities for PyTorch DataLoader.

Provides custom collate functions for handling heterogeneous batch types
and advanced batching strategies.
"""

import logging
from typing import Any, List, Optional, Tuple

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

logger = logging.getLogger(__name__)


def collate_paimon_batch(
    batch: List[Tuple[Any, Optional[Any]]]
) -> Tuple[torch.Tensor, Optional[torch.Tensor]]:
    """Default collate function for Paimon PyTorch datasets.

    Stacks features and labels from individual samples into batch tensors.

    Args:
        batch: List of (features, labels) tuples from dataset

    Returns:
        Tuple of (batch_features, batch_labels) tensors

    Raises:
        RuntimeError: If batch is empty or tensors cannot be stacked
    """
    if not TORCH_AVAILABLE:
        raise ImportError("PyTorch is required")

    if not batch:
        raise RuntimeError("Empty batch provided to collate function")

    try:
        features_list = []
        labels_list = []

        for features, labels in batch:
            features_list.append(features)
            if labels is not None:
                labels_list.append(labels)

        # Stack features
        batch_features = torch.stack(features_list, dim=0)

        # Stack labels if present
        batch_labels = None
        if labels_list:
            batch_labels = torch.stack(labels_list, dim=0)

        return batch_features, batch_labels

    except Exception as e:
        logger.error(f"Error collating batch: {e}", exc_info=True)
        raise


def collate_paimon_batch_dict(
    batch: List[dict]
) -> dict:
    """Collate function returning batch as dictionary.

    Useful for multi-output models or when you want to preserve field names.

    Args:
        batch: List of sample dictionaries

    Returns:
        Dictionary with batched tensors

    Raises:
        RuntimeError: If batch is empty
    """
    if not TORCH_AVAILABLE:
        raise ImportError("PyTorch is required")

    if not batch:
        raise RuntimeError("Empty batch provided to collate function")

    try:
        result = {}

        # Get all keys from first sample
        keys = batch[0].keys()

        for key in keys:
            values = [sample[key] for sample in batch]

            # Skip non-tensor values
            if not isinstance(values[0], torch.Tensor):
                result[key] = values
            else:
                result[key] = torch.stack(values, dim=0)

        return result

    except Exception as e:
        logger.error(f"Error collating batch dict: {e}", exc_info=True)
        raise


def create_padded_collate_fn(
    pad_value: float = 0.0,
    max_length: Optional[int] = None
):
    """Factory for creating padded collate function.

    Useful for variable-length sequences (e.g., time series, text).

    Args:
        pad_value: Value to use for padding
        max_length: Maximum sequence length (default: longest in batch)

    Returns:
        Collate function for padded batches

    Example:
        >>> collate_fn = create_padded_collate_fn(pad_value=0.0)
        >>> dataloader = DataLoader(dataset, collate_fn=collate_fn)
    """
    if not TORCH_AVAILABLE:
        raise ImportError("PyTorch is required")

    def padded_collate(batch: List[Tuple[Any, Optional[Any]]]):
        """Collate with padding."""
        if not batch:
            raise RuntimeError("Empty batch provided to collate function")

        features_list = []
        labels_list = []
        max_len = max_length

        for features, labels in batch:
            features_list.append(features)
            if labels is not None:
                labels_list.append(labels)

        # Determine padding length
        if max_len is None:
            max_len = max(f.shape[0] for f in features_list)

        # Pad features
        padded_features = []
        for features in features_list:
            if features.shape[0] < max_len:
                padding = torch.full(
                    (max_len - features.shape[0],) + features.shape[1:],
                    pad_value,
                    dtype=features.dtype
                )
                padded = torch.cat([features, padding], dim=0)
            else:
                padded = features[:max_len]
            padded_features.append(padded)

        batch_features = torch.stack(padded_features, dim=0)

        # Pad labels if present
        batch_labels = None
        if labels_list:
            padded_labels = []
            for labels in labels_list:
                if labels.shape[0] < max_len:
                    padding = torch.full(
                        (max_len - labels.shape[0],) + labels.shape[1:],
                        pad_value,
                        dtype=labels.dtype
                    )
                    padded = torch.cat([labels, padding], dim=0)
                else:
                    padded = labels[:max_len]
                padded_labels.append(padded)

            batch_labels = torch.stack(padded_labels, dim=0)

        return batch_features, batch_labels

    return padded_collate
