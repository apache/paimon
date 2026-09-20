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

import json

import numpy as np
import torch

from pypaimon.benchmark.act.harness import decode_image_tensor
from pypaimon.benchmark.act import robomind_agilex as agilex


QPOS_COLUMNS = (
    "state_joint_position_left",
    "state_joint_position_right",
)
ACTION_COLUMNS = ("action",)
IMAGE_COLUMNS = (
    "observation_images_rgb_front",
    "observation_images_rgb_wrist_left",
    "observation_images_rgb_wrist_right",
)


class PaimonACTAdapter:
    """Convert a contiguous Paimon row window to the shared ACT sample.

    State and camera columns are taken from the anchor row. The action column
    covers the full horizon. The returned mapping has the same IDs, tensors,
    shapes, and normalization as :class:`Hdf5ACTWindowDataset`.
    """

    def __init__(self, normalization, episode_ids):
        self.normalization = normalization
        self.episode_ids = episode_ids

    def __call__(self, sample):
        """Convert the generic window mapping into ACT tensors and identity.

        The persisted ``frame_index`` is forwarded as the shared ACT sample
        position.
        State and image columns are singleton lists; action retains the full
        horizon and ``action_is_pad`` becomes the ACT ``is_pad`` mask.
        """
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
        episode_id = self.episode_ids[sample["episode_index"]]
        frame_index = sample["frame_index"]
        return {
            "sample_id": "%s#%d" % (episode_id, frame_index),
            "episode_id": episode_id,
            "frame_index": frame_index,
            "qpos": torch.from_numpy(np.ascontiguousarray(qpos)),
            "action": torch.from_numpy(np.ascontiguousarray(action)),
            "images": torch.from_numpy(np.ascontiguousarray(images)),
            "is_pad": sample["action_is_pad"],
        }


def create_datasets(
        frames,
        snapshot_id,
        train_episode_index,
        validation_episode_index,
        normalization,
        config,
        episode_ids):
    """Create lazy train and validation windows pinned to one snapshot.

    State and image columns are anchor-only, so one sample reads the initial
    joint position and three observation images once rather than once per
    action-horizon row.

    Args:
        frames: Paimon frames table used to create both scans.
        snapshot_id: Snapshot pinned by experiment preparation. Both returned
            datasets reject any different resolved snapshot.
        train_episode_index: Episode selected for training windows.
        validation_episode_index: Episode selected for validation windows.
        normalization: Shared state and action normalization arrays.
        config: Benchmark configuration containing the action horizon.
        episode_ids: Mapping from integer episode indices to source IDs.

    Returns:
        ``(train_dataset, validation_dataset)`` in that order, as lazy
        ``ContiguousWindowDataset`` instances pinned to ``snapshot_id``.
    """
    datasets = tuple(
        frames.scan(snapshot_id=snapshot_id).where(
            "episode_index = %d AND quality_status = 0" % episode_index
        ).to_contiguous_window_dataset(
            frame_offsets={
                name: range(config.action_horizon) for name in ACTION_COLUMNS
            },
            columns=QPOS_COLUMNS + ACTION_COLUMNS + IMAGE_COLUMNS,
            group_key="episode_index",
            order_key="frame_index",
            stride=1,
            boundary="drop",
            adapter=PaimonACTAdapter(normalization, episode_ids),
        )
        for episode_index in (train_episode_index, validation_episode_index)
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
    rows = (contract_table(connection, "stat", statistics_version)
            .scan(tag_name=statistics_version)
            .where("feature = 'action' AND split = 'train' AND source_tag = '%s'"
                   % escaped).to_list())
    if len(rows) != 1:
        raise ValueError(
            "Expected one normalization row for %r, got %d."
            % (statistics_version, len(rows)))
    row = rows[0]
    stats = json.loads(row["stats"])
    act = stats["act"]
    if act["episode_filter"] != "success = true AND quality_status = 0":
        raise ValueError("ACT statistics must describe valid successful episodes.")
    return {
        "source_table": row["source_table"],
        "source_snapshot_id": stats["source_snapshot_id"],
        "source_split": row["split"], "feature_name": row["feature"],
        "frame_count": act["count"], "action_mean": act["mean"],
        "action_std": np.maximum(act["std"], act["std_floor"]).tolist(),
        "standard_deviation_floor": act["std_floor"],
    }


def latest_snapshot_id(table):
    """Return the table's latest snapshot ID or fail for an empty table."""
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    if snapshot is None:
        raise ValueError("Paimon frames table has no snapshot.")
    return snapshot.id


def contract_table(connection, role, tag_name):
    """Resolve a table role through the published info row at the same Tag."""
    rows = connection.get_table(agilex.INFO_TABLE).scan(
        tag_name=tag_name).select(["tag", "tables"]).to_list()
    if len(rows) != 1:
        raise ValueError("Expected one published info row for %r." % tag_name)
    if rows[0]["tag"] != tag_name:
        raise ValueError("Published info tag differs from the requested Tag.")
    tables = dict(rows[0]["tables"])
    if role not in tables:
        raise ValueError("Published table group is missing role %r." % role)
    for name in tables.values():
        member = connection.get_table(name)
        if not member.raw_table.tag_manager().tag_exists(tag_name):
            raise ValueError("Group member %s is missing Tag %r." % (name, tag_name))
    return connection.get_table(tables[role])


def episode_indices(connection, tag_name):
    """Map portable source episode IDs to immutable lake episode indices."""
    rows = contract_table(connection, "episode", tag_name).scan(
        tag_name=tag_name).select([
            "source_episode_key", "episode_index"]).to_list()
    result = {row["source_episode_key"]: row["episode_index"] for row in rows}
    if len(result) != len(rows) or None in result:
        raise ValueError("ACT requires unique, non-null source episode IDs.")
    return result


def tagged_snapshot_id(table, tag_name):
    """Resolve a published Tag to its immutable snapshot identity."""
    return table.raw_table.tag_manager().get_or_throw(tag_name).id
