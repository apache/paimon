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

import pickle
from unittest.mock import Mock, patch

import numpy as np
import pyarrow as pa
import pytest
import torch

import pypaimon.multimodal as pm
from pypaimon import CatalogFactory, Schema
from pypaimon.multimodal.blob_read import fetch_blob_bodies
from pypaimon.multimodal.query import ScanQuery, VectorQuery
from pypaimon.multimodal import window_dataset
from pypaimon.table.row.blob import BlobDescriptor


@pytest.fixture
def frames(tmp_path):
    schema = pa.schema([
        ("episode_index", pa.int64()), ("frame_index", pa.int64()),
        ("state", pa.int64()), ("action", pa.int64()),
        ("eligible", pa.bool_()), ("text", pa.string()),
        ("embedding", pa.list_(pa.float32(), 2)), ("payload", pa.large_binary()),
    ])
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "frames", schema=schema,
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector-index.search-mode": "full", "full-text-index.search-mode": "full",
                 "read.batch-size": "2"})
    for start in (0, 3, 6):
        table.add(pa.Table.from_pylist([
            {"episode_index": 0, "frame_index": step, "state": step,
             "action": 10 + step, "eligible": step in (2, 5),
             "text": "anchor" if step in (2, 5) else "context",
             "embedding": [float(step), 0.], "payload": str(step).encode()}
            for step in range(start, min(start + 3, 8))], schema=schema))
    return table


def search(frames, kind="vector", **options):
    if kind in ("text", "hybrid"):
        pytest.importorskip("paimon_ftindex")
        frames.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index(
            "text", "full-text")
    if kind == "text":
        query = frames.search("anchor", column="text", **options)
    elif kind == "hybrid":
        query = frames.search_hybrid([
            pm.vector_route("embedding", [2., 0.]),
            pm.text_route("anchor", column="text")], **options)
    elif kind == "batch":
        query = frames.search_vectors([[2., 0.], [5., 0.]], **options)
    else:
        query = frames.search([2., 0.], **options)
    return query.pre_filter("eligible = true").select(["state", "action"]).limit(2)


def datasets(result, kind):
    return result if kind == "batch" else [result]


@pytest.mark.parametrize("kind", ["vector", "text", "hybrid", "batch"])
def test_search_selects_anchors_and_retains_history_and_future(frames, kind):
    query = search(frames, kind)
    result = query.to_contiguous_window_dataset(
        frame_offsets={"state": [-1, 0], "action": [0, 1]}, boundary="error")

    for dataset in datasets(result, kind):
        assert isinstance(dataset, torch.utils.data.Dataset)
        assert len(dataset) == 2
        samples = dataset[:]
        assert [sample["frame_index"] for sample in samples] == [2, 5]
        assert [sample["state"] for sample in samples] == [[1, 2], [4, 5]]
        assert [sample["action"] for sample in samples] == [[12, 13], [15, 16]]
        assert all(not sample["state_is_pad"].any() for sample in samples)
    assert query._projection == ["state", "action"]
    assert query._limit == 2


@pytest.mark.parametrize("kind", ["vector", "text", "hybrid", "batch"])
@pytest.mark.parametrize("streaming", [True, False])
def test_search_to_torch_returns_only_post_filtered_hits(frames, kind, streaming):
    query = search(frames, kind).where("frame_index = 5").with_row_id()
    result = query.to_torch(streaming=streaming)
    for dataset in datasets(result, kind):
        assert list(dataset) == [{"state": 5, "action": 15, "_ROW_ID": 5}]


@pytest.mark.parametrize("target", ["to_torch", "to_contiguous_window_dataset"])
def test_search_and_training_share_snapshot_during_concurrent_update(frames, target):
    query = search(frames).where("frame_index = 5")
    snapshot = frames.raw_table.snapshot_manager().get_latest_snapshot().id
    lookup = ScanQuery._read_global_index_result

    def update_after_lookup(execution, result):
        rows = lookup(execution, result)
        frames.update("frame_index >= 0", {"action": 100})
        return rows

    options = ({"streaming": False} if target == "to_torch" else
               {"frame_offsets": {"action": [-1, 0, 1]}, "boundary": "error"})
    with patch.object(ScanQuery, "_read_global_index_result", update_after_lookup):
        dataset = getattr(query, target)(**options)
    if target == "to_torch":
        assert list(dataset) == [{"state": 5, "action": 15}]
        assert list(query.to_torch(streaming=False)) == [{"state": 5, "action": 100}]
    else:
        assert dataset.snapshot_id == snapshot
        assert dataset[0]["action"] == [14, 15, 16]
        assert query.to_contiguous_window_dataset(**options)[0]["action"] == [100, 100, 100]


@pytest.mark.parametrize("selector", ["snapshot_id", "tag_name"])
def test_search_training_honors_time_travel(frames, selector):
    snapshot = frames.raw_table.snapshot_manager().get_latest_snapshot().id
    frames.raw_table.create_tag("training", snapshot_id=snapshot)
    frames.update("frame_index >= 0", {"action": 100})
    if selector == "tag_name":
        frames.raw_table.file_io.delete(
            frames.raw_table.snapshot_manager().get_snapshot_path(snapshot))
    options = {selector: snapshot if selector == "snapshot_id" else "training"}
    query = search(frames, **options).where("frame_index = 5")
    assert list(query.to_torch(streaming=False)) == [{"state": 5, "action": 15}]
    dataset = query.to_contiguous_window_dataset(frame_offsets={"action": [-1, 0, 1]})
    assert dataset.snapshot_id == snapshot
    assert dataset[0]["action"] == [14, 15, 16]
    dataset.load_state_dict(dataset.state_dict())


def test_search_windows_resume_from_retained_tag(frames):
    loader_type = pytest.importorskip("torchdata.stateful_dataloader").StatefulDataLoader
    snapshot = frames.raw_table.snapshot_manager().get_latest_snapshot().id
    frames.raw_table.create_tag("training", snapshot_id=snapshot)
    frames.update("frame_index >= 0", {"action": 100})
    frames.raw_table.file_io.delete(frames.raw_table.snapshot_manager().get_snapshot_path(snapshot))
    query = search(frames, tag_name="training")
    offsets = {"action": [-1, 0, 1]}
    loader = loader_type(query.to_contiguous_window_dataset(frame_offsets=offsets), batch_size=None)
    iterator = iter(loader)
    assert next(iterator)["action"] == [11, 12, 13]
    state = loader.state_dict()
    restored = loader_type(query.to_contiguous_window_dataset(frame_offsets=offsets), batch_size=None)
    restored.load_state_dict(state)
    assert [sample["action"] for sample in restored] == [[14, 15, 16]]


def test_empty_search_does_not_expand_to_all_training_rows(frames):
    query = search(frames).where("frame_index < 0")
    with patch("pypaimon.multimodal.window_dataset.fetch_blob_bodies") as fetch:
        dataset = query.to_contiguous_window_dataset(
            columns=["payload"], frame_offsets={"payload": [-10, 10]}, boundary="error")
        assert len(dataset) == 0
        assert dataset[:] == []
        fetch.assert_not_called()
    assert list(query.to_torch()) == []
    empty = query.to_torch(streaming=False)
    assert len(empty) == 0
    assert list(torch.utils.data.DataLoader(empty)) == []


def test_window_context_filter_does_not_become_an_anchor_filter(frames):
    query = search(frames).where("frame_index = 5")
    dataset = query.to_contiguous_window_dataset(
        context_filter="episode_index = 0", frame_offsets={"state": [-1, 0, 1]})
    assert dataset[0]["state"] == [4, 5, 6]
    with pytest.raises(ValueError, match="outside the context query"):
        query.to_contiguous_window_dataset(context_filter="frame_index < 5")


def test_search_windows_keep_stride_and_boundary_semantics(frames):
    dataset = search(frames).to_contiguous_window_dataset(
        stride=2, frame_offsets={"state": [-2, 0, 5]}, boundary="error")
    assert len(dataset) == 1
    assert dataset[0]["state"] == [0, 2, 7]
    padded = search(frames).where("frame_index = 5").to_contiguous_window_dataset(
        frame_offsets={"action": [-6, 0, 3]}, boundary="pad")
    assert padded[0]["action"] == [10, 15, 17]
    assert padded[0]["action_is_pad"].tolist() == [True, False, True]


@pytest.mark.parametrize("target", ["to_torch", "to_contiguous_window_dataset"])
@pytest.mark.parametrize("option", ["with_score", "order_by_score"])
def test_training_rejects_search_score_output(frames, target, option):
    with pytest.raises(ValueError, match="score"):
        getattr(getattr(search(frames), option)(), target)()


def test_search_windows_reject_explicit_anchor_override(frames):
    with pytest.raises(ValueError, match="anchor_row_ids"):
        search(frames).to_contiguous_window_dataset(anchor_row_ids=[0])


def test_batch_search_keeps_query_specific_anchors(frames):
    query = search(frames, "batch").limit(1)
    windows = query.to_contiguous_window_dataset(frame_offsets={"state": [-1, 0, 1]})
    assert len(windows) == 2
    assert [dataset[0]["state"] for dataset in windows] == [[1, 2, 3], [4, 5, 6]]
    rows = query.to_torch(streaming=False)
    assert [list(dataset) for dataset in rows] == [
        [{"state": 2, "action": 12}], [{"state": 5, "action": 15}]]
    filtered = query.where("frame_index = 5").to_contiguous_window_dataset()
    assert [len(dataset) for dataset in filtered] == [0, 1]


@pytest.mark.parametrize("predicate,expected", [
    (None, [[1, 2, 3], [4, 5, 6], [1, 2, 3]]),
    ("frame_index = 5", [None, [4, 5, 6], None]),
    ("frame_index < 0", [None, None, None]),
])
def test_batch_windows_read_and_retain_context_once(frames, predicate, expected):
    query = (frames.search_vectors([[2., 0.], [5., 0.], [2., 0.]])
             .pre_filter("eligible = true").select(["state"]).limit(1))
    if predicate is not None:
        query.where(predicate)
    with patch.object(window_dataset, "_read_window_index", wraps=window_dataset._read_window_index) as read:
        windows = query.to_contiguous_window_dataset(
            frame_offsets={"state": [-1, 0, 1]}, boundary="error")
    assert read.call_count == 1
    assert [dataset[0]["state"] if len(dataset) else None for dataset in windows] == expected
    for dataset in windows[1:]:
        assert dataset._splits is windows[0]._splits
        for name in ("_row_ids", "_group_starts", "_group_lengths", "_group_first_orders"):
            assert np.shares_memory(getattr(dataset, name), getattr(windows[0], name))
            assert not getattr(dataset, name).flags.writeable
        assert dataset._plans is not windows[0]._plans
        assert dataset.column_transforms is not windows[0].column_transforms


def test_batch_window_views_keep_stride_checkpoints_and_worker_reads(frames):
    query = search(frames, "batch").limit(1)
    kwargs = {"frame_offsets": {"state": [-1, 0, 1]}, "boundary": "error"}
    windows = query.to_contiguous_window_dataset(**kwargs)
    state = windows[0].state_dict()
    standalone = (frames.scan(snapshot_id=windows[0].snapshot_id).select(["state", "action"])
                  .to_contiguous_window_dataset(anchor_row_ids=[2], **kwargs))
    standalone.load_state_dict(state)
    with pytest.raises(ValueError, match="different CWD"):
        windows[1].load_state_dict(state)
    windows[0][0]  # Warm one view's local read plan before serialization.
    assert windows[0]._plans
    assert not windows[1]._plans
    restored = pickle.loads(pickle.dumps(windows))
    assert restored[0]._row_ids is restored[1]._row_ids
    assert not restored[0]._plans and not restored[1]._plans
    restored[0].load_state_dict(state)
    loader = torch.utils.data.DataLoader(
        restored[1], batch_size=None, num_workers=1, multiprocessing_context="spawn")
    assert [sample["state"] for sample in loader] == [[4, 5, 6]]
    strided = query.to_contiguous_window_dataset(stride=2, **kwargs)
    assert [len(dataset) for dataset in strided] == [1, 0]
    assert strided[0][0]["state"] == [1, 2, 3]


def test_batch_window_views_own_video_collators(tmp_path):
    from pypaimon.table.row.blob import Blob

    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "video", schema=pa.schema([
            ("episode_index", pa.int64()), ("frame_index", pa.int64()),
            ("embedding", pa.list_(pa.float32(), 2)), ("video", pa.large_binary()),
        ]), options={"file.format": "parquet", "vector.file.format": "parquet",
                     "vector-index.search-mode": "full", "video-frame-field": "video"})
    video = tmp_path / "episode.mp4"
    video.write_bytes(b"video-content")
    table.add_video(Blob.from_local(str(video)), [
        {"episode_index": 0, "frame_index": step, "embedding": [float(step), 0.]}
        for step in range(3)])
    query = table.search_vectors([[0., 0.], [2., 0.]]).limit(1)
    with patch.object(window_dataset, "VideoFrameCollator", side_effect=lambda *args, **kwargs: Mock()):
        first, second = query.to_contiguous_window_dataset(columns=["video"])
        assert first._video_collators["video"] is not second._video_collators["video"]
        first.close()
        first._video_collators["video"].close.assert_called_once()
        second._video_collators["video"].close.assert_not_called()
        second.close()


@pytest.mark.parametrize("streaming", [True, False])
def test_search_payloads_stay_lazy_and_torch_keeps_blob_descriptors(frames, streaming):
    query = search(frames).select(["payload"]).where("frame_index = 5")
    dataset = query.to_torch(streaming=streaming)
    descriptor = BlobDescriptor.deserialize(list(dataset)[0]["payload"])
    assert descriptor.length == 1
    assert descriptor.uri.endswith(".blob")
    with patch("pypaimon.multimodal.window_dataset.fetch_blob_bodies", wraps=fetch_blob_bodies) as fetch:
        windows = query.to_contiguous_window_dataset(frame_offsets={"payload": [-1, 0, 1]})
        fetch.assert_not_called()
        assert windows[0]["payload"] == [b"4", b"5", b"6"]
        assert fetch.call_count == 1


@pytest.mark.parametrize("target", ["to_torch", "to_contiguous_window_dataset"])
def test_training_dataset_can_be_read_in_spawned_workers(frames, target):
    dataset = getattr(search(frames), target)()
    loader = torch.utils.data.DataLoader(
        dataset, batch_size=None, num_workers=2, multiprocessing_context="spawn")
    assert sorted(sample["state"] if target == "to_torch" else sample["state"][0]
                  for sample in loader) == [2, 5]


@pytest.mark.parametrize("target", ["to_torch", "to_contiguous_window_dataset"])
def test_training_rejects_primary_key_table(tmp_path, target):
    catalog = CatalogFactory.create({"warehouse": str(tmp_path)})
    catalog.create_database("db", False)
    catalog.create_table("db.frames", Schema.from_pyarrow_schema(
        pa.schema([("id", pa.int64()), ("embedding", pa.list_(pa.float32(), 2))]),
        primary_keys=["id"], options={"file.format": "parquet"}), False)
    query = VectorQuery(catalog.get_table("db.frames"), [0., 0.], "embedding")
    with pytest.raises(NotImplementedError, match="data-evolution"):
        getattr(query, target)()


@pytest.mark.parametrize("target", ["to_torch", "to_contiguous_window_dataset"])
def test_training_validates_search_execution_options(frames, target):
    with pytest.raises(ValueError, match="Ray options"):
        getattr(search(frames), target)(concurrency=2)
    with pytest.raises(ValueError, match="execution"):
        getattr(search(frames), target)(execution="invalid")


@pytest.mark.parametrize("target", ["to_torch", "to_contiguous_window_dataset"])
def test_initially_empty_search_keeps_empty_training_view(tmp_path, target):
    from pypaimon.execution.python.read.search.vector_search_scan import DataEvolutionVectorScan

    schema = pa.schema([("episode_index", pa.int64()), ("frame_index", pa.int64()),
                        ("state", pa.int64()), ("embedding", pa.list_(pa.float32(), 2))])
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "empty", schema=schema, options={
            "file.format": "parquet", "vector.file.format": "parquet", "vector-index.search-mode": "full"})
    query = table.search([0., 0.]).select(["state"])
    scan = DataEvolutionVectorScan.scan

    def append_before_search(execution):
        table.add(pa.Table.from_pylist([
            {"episode_index": 0, "frame_index": 0, "state": 10, "embedding": [0., 0.]}], schema=schema))
        return scan(execution)

    options = {"streaming": False} if target == "to_torch" else {}
    with patch.object(DataEvolutionVectorScan, "scan", append_before_search):
        dataset = getattr(query, target)(**options)
    assert len(dataset) == 0
    assert list(torch.utils.data.DataLoader(dataset)) == []
    assert len(getattr(query, target)(**options)) == 1


@pytest.mark.parametrize("kind,backend", [
    ("vector", "vector_search._execute_vector_search"),
    ("text", "full_text_search._execute_full_text_search"),
    ("hybrid", "full_text_search._execute_hybrid_search"),
    ("batch", "batch_vector_search._execute_batch_vector_search"),
])
@pytest.mark.parametrize("target", ["to_torch", "to_contiguous_window_dataset"])
def test_training_delegates_ray_search_and_keeps_its_snapshot(frames, kind, backend, target):
    query = search(frames, kind).where("frame_index = 5")
    calls = []

    def execute(builder, **options):
        calls.append(options)
        result = builder.execute_batch_local() if kind == "batch" else builder.execute_local()
        frames.update("frame_index >= 0", {"action": 100})
        return result

    with patch("pypaimon.ray." + backend, side_effect=execute):
        result = getattr(query, target)(
            execution="ray", concurrency=2, ray_remote_args={"num_cpus": 1})
    assert calls == [{"concurrency": 2, "ray_remote_args": {"num_cpus": 1}}]
    for dataset in datasets(result, kind):
        assert list(dataset)[0]["action"] == (15 if target == "to_torch" else [15])
