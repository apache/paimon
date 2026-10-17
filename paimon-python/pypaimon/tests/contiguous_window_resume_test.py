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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import random
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pytest
import torch

import pypaimon.multimodal as pmm
from pypaimon.multimodal import window_dataset


@pytest.fixture
def frames(tmp_path):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table("frames", schema=pa.schema([
        pa.field("episode", pa.string()),
        pa.field("step", pa.int64()),
        pa.field("value", pa.float32()),
    ]), options={
        "row-tracking.enabled": "true",
        "data-evolution.enabled": "true",
        "file.format": "parquet",
        "scan.native-plan.enabled": "false",
        "read.native.enabled": "false",
    })
    table.add([
        {"episode": episode, "step": step, "value": float(step)}
        for episode in ("a", "b") for step in range(24)
    ])
    return table


def windows(frames, query=None, **kwargs):
    options = dict(columns=["value"], group_key="episode", order_key="step",
                   frame_offsets={"value": [0, 1, 2]},
                   column_transforms={"value": torch.tensor})
    options.update(kwargs)
    return (query or frames.scan()).to_contiguous_window_dataset(**options)


def test_restore_rejects_same_length_different_anchors(frames):
    original = windows(frames, anchor_row_ids=[0, 1])
    different = windows(frames, anchor_row_ids=[2, 3])
    assert len(original) == len(different)

    with pytest.raises(ValueError, match="different CWD"):
        different.load_state_dict(original.state_dict())


@pytest.mark.parametrize("change", ["context", "offsets", "padding", "snapshot"])
def test_restore_rejects_changed_data_contract(frames, change):
    original = windows(frames, query=frames.scan().where("episode = 'a'"), boundary="pad")
    state = original.state_dict()
    options = {"query": frames.scan().where("episode = 'a'"), "boundary": "pad"}
    if change == "context":
        options["query"] = frames.scan().where("episode = 'b'")
    elif change == "offsets":
        options["frame_offsets"] = {"value": [2, 1, 0]}
    elif change == "padding":
        options["pad_values"] = {"value": -1}
    else:
        frames.add([{"episode": "c", "step": 0, "value": 0.0}])
        options["query"] = frames.scan().where("episode = 'a'")
        # A retained old snapshot remains valid after unrelated new commits.
        pinned = frames.scan(snapshot_id=original.snapshot_id).where("episode = 'a'")
        windows(frames, query=pinned, boundary="pad").load_state_dict(state)
    different = windows(frames, **options)
    assert len(original) == len(different)
    with pytest.raises(ValueError, match="different CWD"):
        different.load_state_dict(state)


class _RandomTransform:

    def __init__(self):
        self.generator = random.Random(37)
        self.calls = [0]

    def __call__(self, values):
        self.calls[0] += 1
        noise = (random.random() + np.random.random() + torch.rand(()).item()
                 + self.generator.random() + self.calls[0])
        return torch.tensor(values) + noise

    def state_dict(self):
        return {"generator": self.generator.getstate(), "calls": self.calls}

    def load_state_dict(self, state):
        self.generator.setstate(state["generator"])
        self.calls[:] = state["calls"]


def test_restore_rng_and_independent_callback_state(frames):
    original = windows(frames)
    original.column_transforms["value"] = _RandomTransform()
    original[0]
    state = original.state_dict()
    expected = original[1]["value"]
    original[2]

    restored = windows(frames)
    restored.column_transforms["value"] = _RandomTransform()
    restored.load_state_dict(state)
    torch.testing.assert_close(restored[1]["value"], expected, rtol=0, atol=0)


def test_restore_rejects_missing_callback_and_unknown_version(frames):
    original = windows(frames, column_transforms={"value": _RandomTransform()})
    state = original.state_dict()
    with pytest.raises(ValueError, match="different stateful callbacks"):
        windows(frames).load_state_dict(state)
    state["version"] = 2
    with pytest.raises(ValueError, match="Unsupported CWD checkpoint version"):
        original.load_state_dict(state)


def test_callback_requires_both_state_methods(frames):
    transform = _RandomTransform()
    transform.load_state_dict = None
    dataset = windows(frames, column_transforms={"value": transform})
    with pytest.raises(TypeError, match="must implement both"):
        dataset.state_dict()


def test_checkpoint_reuses_identity_without_reading_payload(frames):
    dataset = windows(frames)
    with patch.object(dataset, "_read_rows", side_effect=AssertionError("payload read")):
        state = dataset.state_dict()
        with patch.object(window_dataset.hashlib, "sha256", side_effect=AssertionError("rehash")):
            dataset.load_state_dict(state)
            assert dataset.state_dict()["identity"] == state["identity"]


def test_restore_retained_tag_without_snapshot_file(frames):
    frames.raw_table.create_tag("train")
    dataset = windows(frames, query=frames.scan(tag_name="train"))
    manager = frames.raw_table.snapshot_manager()
    frames.raw_table.file_io.delete_quietly(manager.get_snapshot_path(dataset.snapshot_id))
    state = dataset.state_dict()
    restored = windows(frames, query=frames.scan(tag_name="train"))
    restored.load_state_dict(state)
    torch.testing.assert_close(restored[0]["value"], torch.tensor([0.0, 1.0, 2.0]))


@pytest.mark.parametrize("nested", [False, True])
def test_tensor_padding_identity_is_by_value(frames, nested):
    def dataset(value):
        pad = torch.tensor(value)
        if nested:
            pad = {"values": [pad]}
        return windows(frames, boundary="pad", pad_values={"value": pad})

    state = dataset(0.0).state_dict()
    dataset(0.0).load_state_dict(state)
    with pytest.raises(ValueError, match="different CWD"):
        dataset(1.0).load_state_dict(state)


def test_numpy_padding_identity_ignores_memory_layout(frames):
    pad = np.arange(6, dtype=np.float32).reshape(2, 3)
    original = windows(frames, boundary="pad", pad_values={"value": pad})
    restored = windows(frames, boundary="pad", pad_values={"value": np.asfortranarray(pad)})
    restored.load_state_dict(original.state_dict())


def _loader(dataset, workers, persistent=False):
    loader_type = pytest.importorskip("torchdata.stateful_dataloader").StatefulDataLoader
    return loader_type(
        dataset, batch_size=4, shuffle=True,
        generator=torch.Generator().manual_seed(91), num_workers=workers,
        persistent_workers=persistent,
        **({"multiprocessing_context": "spawn", "prefetch_factor": 2} if workers else {}))


def _batch_values(batch):
    return (batch["episode"], batch["step"].tolist(), batch["value"].tolist(),
            batch["value_is_pad"].tolist())


@pytest.mark.parametrize("workers,persistent,completed", [
    (0, False, 3), (2, False, 3), (2, True, 3), (0, False, 11), (2, True, 11),
])
def test_stateful_loader_resumes_prefetch_and_next_epoch(
        frames, tmp_path, workers, persistent, completed):
    dataset = windows(frames)
    dataset.column_transforms["value"] = _RandomTransform()
    loader = _loader(dataset, workers, persistent)
    iterator = iter(loader)
    prefix = [_batch_values(next(iterator)) for _ in range(completed)]
    checkpoint = tmp_path / "loader.pt"
    torch.save({"loader": loader.state_dict(), "generator": loader.generator.get_state()}, checkpoint)
    remaining = [_batch_values(batch) for batch in iterator]
    next_epoch = [_batch_values(batch) for batch in loader]

    restored_dataset = windows(frames)
    restored_dataset.column_transforms["value"] = _RandomTransform()
    restored = _loader(restored_dataset, workers, persistent)
    state = torch.load(checkpoint, weights_only=False)
    restored.load_state_dict(state["loader"])
    restored_iterator = iter(restored)
    restored.generator.set_state(state["generator"])
    assert [_batch_values(batch) for batch in restored_iterator] == remaining
    assert [_batch_values(batch) for batch in restored] == next_epoch
    assert sum(len(batch[0]) for batch in prefix + remaining) == len(dataset)


def test_stateful_distributed_sampler_restores_each_rank(frames):
    stateful = pytest.importorskip("torchdata.stateful_dataloader")
    samplers = pytest.importorskip("torchdata.stateful_dataloader.sampler")
    all_ids = []
    for rank in range(2):
        def make_loader():
            dataset = windows(frames)
            sampler = samplers.StatefulDistributedSampler(
                dataset, num_replicas=2, rank=rank, seed=71)
            sampler.set_epoch(3)
            return stateful.StatefulDataLoader(dataset, sampler=sampler, batch_size=4)

        loader = make_loader()
        iterator = iter(loader)
        prefix = [_batch_values(next(iterator)) for _ in range(2)]
        state = loader.state_dict()
        suffix = [_batch_values(batch) for batch in iterator]
        restored = make_loader()
        restored.load_state_dict(state)
        assert [_batch_values(batch) for batch in restored] == suffix
        for episodes, steps, _, _ in prefix + suffix:
            all_ids.extend(zip(episodes, steps))
    assert len(all_ids) == len(set(all_ids)) == 44
