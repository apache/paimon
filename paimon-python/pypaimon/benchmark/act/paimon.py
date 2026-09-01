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

"""Paimon dataset adapter for the RoboMIND ACT benchmark."""

import numpy as np
import torch

from pypaimon.benchmark.act.harness import decode_image_tensor
from pypaimon.sample import robomind_agilex as agilex


QPOS_COLUMNS = (
    "state_joint_position_left",
    "state_joint_position_right",
)
ACTION_COLUMNS = ("action",)
IMAGE_COLUMNS = (
    "rgb_front",
    "rgb_left_wrist",
    "rgb_right_wrist",
)


class PaimonACTAdapter:
    """Convert a contiguous Paimon row window to the shared ACT sample.

    State and camera columns are taken from the anchor row. The action column
    covers the full horizon. The returned mapping has the same IDs, tensors,
    shapes, and normalization as :class:`Hdf5ACTWindowDataset`.
    """

    def __init__(self, normalization):
        self.normalization = normalization

    def __call__(self, sample):
        qpos = np.concatenate([
            np.asarray(sample[name][0], dtype=np.float32)
            for name in QPOS_COLUMNS
        ])
        action = np.concatenate([
            np.asarray(sample[name], dtype=np.float32)
            for name in ACTION_COLUMNS
        ], axis=-1)
        images = np.stack([
            decode_image_tensor(sample[name][0]) for name in IMAGE_COLUMNS
        ])
        qpos = (
            (qpos - self.normalization["qpos_mean"])
            / self.normalization["qpos_std"])
        action = (
            (action - self.normalization["action_mean"])
            / self.normalization["action_std"])
        episode_id = sample["episode_id"]
        step_idx = sample["frame_index"]
        return {
            "sample_id": "%s#%d" % (episode_id, step_idx),
            "episode_id": episode_id,
            "step_idx": step_idx,
            "qpos": torch.from_numpy(np.ascontiguousarray(qpos)),
            "action": torch.from_numpy(np.ascontiguousarray(action)),
            "images": torch.from_numpy(np.ascontiguousarray(images)),
            "is_pad": sample["is_pad"],
        }


def create_datasets(
        frames,
        snapshot_id,
        train_episode_id,
        validation_episode_id,
        normalization,
        config):
    """Create lazy train and validation windows pinned to one snapshot.

    Image columns are anchor-only, so one sample reads three observation
    images rather than one image set per action-horizon row.

    Args:
        frames: Paimon frames table used to create both scans.
        snapshot_id: Snapshot pinned by experiment preparation. Both returned
            datasets reject any different resolved snapshot.
        train_episode_id: Episode selected for training windows.
        validation_episode_id: Episode selected for validation windows.
        normalization: Shared state and action normalization arrays.
        config: Benchmark configuration containing the action horizon.

    Returns:
        ``(train_dataset, validation_dataset)`` in that order, as lazy
        ``ContiguousWindowDataset`` instances pinned to ``snapshot_id``.
    """
    datasets = tuple(
        frames.scan(snapshot_id=snapshot_id).where(
            "episode_id = '%s'" % episode_id.replace("'", "''")
        ).to_contiguous_window_dataset(
            window_size=config.action_horizon,
            columns=QPOS_COLUMNS + ACTION_COLUMNS + IMAGE_COLUMNS,
            anchor_columns=IMAGE_COLUMNS,
            group_key="episode_id",
            order_key="frame_index",
            stride=1,
            tail="drop",
            adapter=PaimonACTAdapter(normalization),
        )
        for episode_id in (train_episode_id, validation_episode_id)
    )
    actual_snapshot_ids = {dataset.snapshot_id for dataset in datasets}
    if actual_snapshot_ids != {snapshot_id}:
        raise RuntimeError(
            "Paimon ACT windows must remain pinned to frames snapshot %s; "
            "got %s." % (snapshot_id, sorted(actual_snapshot_ids)))
    return datasets


def statistics_row(connection, statistics_version):
    """Return the unique versioned action-statistics row."""
    escaped = statistics_version.replace("'", "''")
    rows = (connection.get_table(agilex.FEATURE_STATS_TABLE).scan()
            .where("statistics_version = '%s'" % escaped).to_list())
    if len(rows) != 1:
        raise ValueError(
            "Expected one normalization row for %r, got %d."
            % (statistics_version, len(rows)))
    return rows[0]


def latest_snapshot_id(table):
    """Return the table's latest snapshot ID or fail for an empty table."""
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    if snapshot is None:
        raise ValueError("Paimon frames table has no snapshot.")
    return snapshot.id
