# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""HDF5 dataset adapter for the RoboMIND ACT benchmark."""

import numpy as np
import torch
from torch.utils.data import Dataset

from pypaimon.benchmark.act.harness import decode_image_tensor


QPOS_FIELDS = (
    "puppet/joint_position_left",
    "puppet/joint_position_right",
)
ACTION_FIELDS = (
    "master/joint_position_left",
    "master/joint_position_right",
)
IMAGE_FIELDS = (
    "observations/rgb_images/camera_front",
    "observations/rgb_images/camera_left_wrist",
    "observations/rgb_images/camera_right_wrist",
)


class Hdf5ACTWindowDataset(Dataset):
    """Read complete ACT windows lazily from one HDF5 episode.

    ``episode`` supplies the file path, logical episode ID, and frame count.
    For a window anchor, state and three camera images come from the anchor
    frame while actions cover ``[anchor, anchor + action_horizon)``. Each
    access opens and closes the HDF5 file and returns the shared ACT sample
    mapping consumed by :mod:`pypaimon.benchmark.act.harness`.
    """

    def __init__(self, episode, normalization, action_horizon):
        self.episode = episode
        self.normalization = normalization
        self.action_horizon = action_horizon
        self.window_count = episode.frame_count - action_horizon + 1
        if self.window_count <= 0:
            raise ValueError(
                "Episode %s is shorter than action horizon %d."
                % (episode.episode_id, action_horizon))

    def __len__(self):
        return self.window_count

    def __getitem__(self, anchor):
        """Return the ACT window whose first frame is ``anchor``.

        Negative anchors follow Python sequence semantics. State and images
        come from the anchor frame, while action contains the complete horizon.
        """
        if anchor < 0:
            anchor += self.window_count
        if anchor < 0 or anchor >= self.window_count:
            raise IndexError(anchor)
        import h5py

        with h5py.File(str(self.episode.path), "r") as h5:
            qpos = _read_vectors(h5, QPOS_FIELDS, anchor)
            action = _read_vectors(
                h5,
                ACTION_FIELDS,
                slice(anchor, anchor + self.action_horizon),
            )
            images = np.stack([
                decode_image_tensor(h5[field][anchor]) for field in IMAGE_FIELDS
            ])
        qpos = (
            (qpos - self.normalization["qpos_mean"])
            / self.normalization["qpos_std"])
        action = (
            (action - self.normalization["action_mean"])
            / self.normalization["action_std"])
        return {
            "sample_id": "%s#%d" % (self.episode.episode_id, anchor),
            "episode_id": self.episode.episode_id,
            "step_idx": anchor,
            "qpos": torch.from_numpy(np.ascontiguousarray(qpos)),
            "action": torch.from_numpy(np.ascontiguousarray(action)),
            "images": torch.from_numpy(np.ascontiguousarray(images)),
            "is_pad": torch.zeros(self.action_horizon, dtype=torch.bool),
        }


def create_datasets(train_episode, validation_episode, normalization, config):
    """Create HDF5 datasets for the experiment's selected episodes.

    Args:
        train_episode: Selected training episode with its HDF5 path and frame
            count.
        validation_episode: Selected validation episode with the same fields.
        normalization: Shared state and action normalization arrays.
        config: Benchmark configuration containing the action horizon.

    Returns:
        ``(train_dataset, validation_dataset)`` in that order.
    """
    return (
        Hdf5ACTWindowDataset(
            train_episode, normalization, config.action_horizon),
        Hdf5ACTWindowDataset(
            validation_episode, normalization, config.action_horizon),
    )


def compute_normalization(episodes):
    """Compute train-only HDF5 state and action normalization.

    Args:
        episodes: Discovered episodes carrying ``path``, ``split``, and
            ``success`` attributes.

    Returns:
        ``(normalization, metadata)`` where normalization contains float32
        arrays used by training. Metadata retains the float64 action moments
        and frame count used to validate Paimon statistics without losing
        precision. Standard deviations use a ``1e-2`` floor.
    """
    train = [
        episode for episode in episodes
        if episode.split == "train" and episode.success
    ]
    if not train:
        raise ValueError("No successful train episodes are available.")
    qpos = _Moments(14)
    action = _Moments(14)
    import h5py

    for episode in sorted(train, key=lambda item: item.episode_id):
        with h5py.File(str(episode.path), "r") as h5:
            qpos.update(_read_vectors(
                h5, QPOS_FIELDS, slice(None), dtype=np.float64))
            action.update(_read_vectors(
                h5, ACTION_FIELDS, slice(None), dtype=np.float64))
    qpos_mean, qpos_std = qpos.finish()
    action_mean, action_std = action.finish()
    return ({
        "qpos_mean": qpos_mean.astype(np.float32),
        "qpos_std": qpos_std.astype(np.float32),
        "action_mean": action_mean.astype(np.float32),
        "action_std": action_std.astype(np.float32),
    }, {
        "action_mean": action_mean,
        "action_std": action_std,
        "frame_count": action.count,
    })


def _read_vectors(h5, fields, selection, dtype=np.float32):
    value = np.concatenate([
        np.asarray(h5[field][selection], dtype=dtype) for field in fields
    ], axis=-1)
    if not np.isfinite(value).all():
        raise ValueError("ACT vector contains NaN or Inf.")
    return value


class _Moments(object):
    """Accumulate float64 population moments with a ``1e-2`` std floor."""

    def __init__(self, width):
        self.count = 0
        self.total = np.zeros(width, dtype=np.float64)
        self.total_square = np.zeros(width, dtype=np.float64)

    def update(self, value):
        value = np.asarray(value, dtype=np.float64)
        if value.ndim != 2 or value.shape[1] != len(self.total):
            raise ValueError(
                "Unexpected normalization shape %s." % (value.shape,))
        if not np.isfinite(value).all():
            raise ValueError("Normalization input contains NaN or Inf.")
        self.count += value.shape[0]
        self.total += value.sum(axis=0)
        self.total_square += np.square(value).sum(axis=0)

    def finish(self):
        if self.count == 0:
            raise ValueError("Cannot compute normalization from no frames.")
        mean = self.total / self.count
        variance = np.maximum(
            self.total_square / self.count - np.square(mean), 0.0)
        return mean, np.maximum(np.sqrt(variance), 1e-2)
