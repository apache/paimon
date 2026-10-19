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

import importlib.util
import io
import json
import tempfile
import time
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

import pypaimon.multimodal as pm
from pypaimon.multimodal.lerobot.ray_loader import (
    _episode_groups, _prepare_group, _ray_source_uri, _task_groups,
)


def _counted_prepare_group(trace_path, *args):
    from pypaimon.multimodal.lerobot import source as source_module
    from pypaimon.multimodal.lerobot.ray_loader import _prepare_group

    original = source_module._read_remote_parquet

    def counted(file_io, path, *read_args, **read_kwargs):
        with open(trace_path, "a") as trace:
            trace.write(path + "\n")
        return original(file_io, path, *read_args, **read_kwargs)

    source_module._read_remote_parquet = counted
    try:
        return _prepare_group(*args)
    finally:
        source_module._read_remote_parquet = original


def _bucket_files(table):
    bucket = Path(urlparse(table.table_path).path).resolve() / "bucket-0"
    return sorted(path for path in bucket.rglob("*") if path.is_file())


class LeRobotRayPlanningTest(unittest.TestCase):
    def test_groups_shared_payloads_across_cameras(self):
        info = {
            "data_path": "data/{chunk_index}/{file_index}.parquet",
            "video_path": "{video_key}/{chunk_index}/{file_index}.mp4",
        }
        episodes = []
        for a, b in ((0, 0), (0, 1), (2, 1), (3, 3)):
            episodes.append({
                "data/chunk_index": 0, "data/file_index": len(episodes),
                "videos/a/chunk_index": 0, "videos/a/file_index": a,
                "videos/b/chunk_index": 0, "videos/b/file_index": b,
            })
        self.assertEqual([[0, 1, 2], [3]],
                         _episode_groups(info, episodes, ("a", "b")))
        self.assertEqual([[0], [1], [2], [3]],
                         _episode_groups(info, episodes, ()))
        episodes[1]["data/file_index"] = 0
        self.assertEqual([[0, 1], [2], [3]],
                         _episode_groups(info, episodes, ()))
        self.assertEqual([[0, 1, 3], [2]],
                         _task_groups(info, episodes, (), 2))

    def test_reject_hub_id(self):
        with self.assertRaisesRegex(ValueError, "Hub repo_id"):
            _ray_source_uri("lerobot/example")

    def test_cancel_after_prepare_aborts_worker_files(self):
        table = object()
        messages = [object()]
        dataset = MagicMock()
        dataset.close.side_effect = KeyboardInterrupt
        with patch("pypaimon.multimodal.lerobot.ray_loader._resolved_source") as resolved, \
                patch("pypaimon.multimodal.lerobot.ray_loader._RemoteLeRobotDataset",
                      return_value=dataset), \
                patch("pypaimon.multimodal.lerobot.ray_loader._prepare_dataset",
                      return_value=messages), \
                patch("pypaimon.multimodal.lerobot.ray_loader._abort_messages") as abort:
            resolved.return_value.__enter__.return_value = (object(), {})
            with self.assertRaises(KeyboardInterrupt):
                _prepare_group(table, "ignored", None, {}, None, 1, {}, (), [0])
        abort.assert_called_once_with(table, messages)
        dataset.close.assert_called_once()

    def test_validate_options_before_creating_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            conn = pm.connect(options={"warehouse": Path(directory).as_uri()})
            for options in ({"parallelism": 0}, {"parallelism": -1},
                            {"parallelism": True}, {"parallelism": 1.5},
                            {"parallelism": "2"}):
                with self.subTest(options=options):
                    with self.assertRaises(ValueError):
                        conn.load_from_lerobot("invalid", "missing", **options)
            self.assertEqual([], conn.catalog.list_databases())


@unittest.skipUnless(importlib.util.find_spec("ray"), "Ray is not installed")
class LeRobotRayImportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import ray
        cls.owns_ray = not ray.is_initialized()
        if cls.owns_ray:
            ray.init(num_cpus=2, include_dashboard=False,
                     object_store_memory=100 * 1024 * 1024)

    @classmethod
    def tearDownClass(cls):
        if cls.owns_ray:
            import ray
            ray.shutdown()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.source = self.root / "source"
        self.conn = pm.connect(options={"warehouse": (self.root / "lake").as_uri()})

    def tearDown(self):
        self.directory.cleanup()

    def _source(self, video=False, data_files=1, path_backed=False):
        root = self.source
        (root / "meta/episodes/chunk-000").mkdir(parents=True, exist_ok=True)
        (root / "data/chunk-000").mkdir(parents=True, exist_ok=True)
        features = {key: {"dtype": "int64", "shape": [1]} for key in
                    ("index", "episode_index", "frame_index", "task_index")}
        features.update({
            "timestamp": {"dtype": "float32", "shape": [1]},
            "observation.state": {"dtype": "float32", "shape": [2]},
            "camera": {"dtype": "video" if video else "image", "shape": [16, 16, 3]},
        })
        info = {
            "codebase_version": "v3.0", "fps": 10, "total_frames": 8,
            "total_episodes": 4, "total_tasks": 1, "features": features,
            "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
            "video_path": "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4",
        }
        (root / "meta/info.json").write_text(json.dumps(info))
        pq.write_table(pa.Table.from_pandas(pd.DataFrame(
            {"task_index": [0]}, index=pd.Index(["pick"], name="task"))),
            root / "meta/tasks.parquet")
        episodes = []
        for e in range(4):
            row = {"episode_index": e, "dataset_from_index": e * 2,
                   "dataset_to_index": e * 2 + 2, "length": 2, "tasks": ["pick"],
                   "data/chunk_index": 0,
                   "data/file_index": e // (4 // data_files)}
            if video:
                row.update({"videos/camera/chunk_index": 0,
                            "videos/camera/file_index": e // 2,
                            "videos/camera/from_timestamp": (e % 2) * 0.2,
                            "videos/camera/to_timestamp": (e % 2 + 1) * 0.2})
            episodes.append(row)
        pq.write_table(pa.Table.from_pylist(episodes),
                       root / "meta/episodes/chunk-000/file-000.parquet")
        rows = pa.table({
            "index": pa.array(range(8), type=pa.int64()),
            "episode_index": pa.array([i // 2 for i in range(8)], type=pa.int64()),
            "frame_index": pa.array([i % 2 for i in range(8)], type=pa.int64()),
            "task_index": pa.array([0] * 8, type=pa.int64()),
            "timestamp": pa.array([i % 2 / 10 for i in range(8)], type=pa.float32()),
            "observation.state": pa.array([[i, -i] for i in range(8)], type=pa.list_(pa.float32(), 2)),
        })
        if video:
            import av
            for v in range(2):
                path = root / ("videos/camera/chunk-000/file-%03d.mp4" % v)
                path.parent.mkdir(parents=True, exist_ok=True)
                with av.open(str(path), "w") as container:
                    stream = container.add_stream("mpeg4", rate=10)
                    stream.width = stream.height = 16
                    stream.pix_fmt = "yuv420p"
                    for i in range(4):
                        frame = av.VideoFrame.from_ndarray(
                            np.full((16, 16, 3), (v * 4 + i) * 20, dtype=np.uint8), format="rgb24")
                        for packet in stream.encode(frame):
                            container.mux(packet)
                    for packet in stream.encode():
                        container.mux(packet)
        else:
            images = []
            for i in range(8):
                buffer = io.BytesIO()
                Image.new("RGB", (16, 16), (i * 20, 0, 0)).save(buffer, format="PNG")
                if path_backed:
                    path = root / ("image-%d.png" % i)
                    path.write_bytes(buffer.getvalue())
                    images.append({"bytes": None, "path": str(path.resolve())})
                else:
                    images.append({"bytes": buffer.getvalue(), "path": None})
            rows = rows.append_column("camera", pa.array(images))
        for file_index in range(data_files):
            episode_count = 4 // data_files
            pq.write_table(
                rows.slice(file_index * episode_count * 2, episode_count * 2),
                root / ("data/chunk-000/file-%03d.parquet" % file_index))
        return rows

    def test_source_files_are_read_once_per_worker(self):
        trace_path = self.root / "reads.txt"

        def counted_prepare(*args):
            return _counted_prepare_group(str(trace_path), *args)

        with patch(
                "pypaimon.multimodal.lerobot.ray_loader._prepare_group",
                counted_prepare):
            for data_files in (1, 4):
                with self.subTest(data_files=data_files):
                    self._source(data_files=data_files)
                    if trace_path.exists():
                        trace_path.unlink()
                    self.conn.load_from_lerobot(
                        "counted_%d" % data_files, self.source,
                        parallelism=2, batch_size=1)
                    reads = Counter(
                        str(Path(urlparse(path).path).resolve().relative_to(
                            self.source.resolve()))
                        for path in trace_path.read_text().splitlines())
                    expected_workers = min(data_files, 2)
                    self.assertEqual(expected_workers, reads["meta/tasks.parquet"])
                    self.assertEqual(
                        expected_workers,
                        reads["meta/episodes/chunk-000/file-000.parquet"])
                    for file_index in range(data_files):
                        self.assertEqual(
                            1, reads["data/chunk-000/file-%03d.parquet" % file_index])

    def _import_serial(self):
        # FileIO sources use the native Parquet reader, not the LeRobot class.
        with patch("pypaimon.multimodal.lerobot.api._import_lerobot_dataset", return_value=None):
            self.conn.load_from_lerobot("serial", self.source.as_uri(),
                                        batch_size=1, parallelism=1)

    def test_images_match_serial_import(self):
        self._check_image_import(False)

    def test_absolute_image_paths_match_serial_import(self):
        self._check_image_import(True)

    def _check_image_import(self, path_backed):
        self._source(path_backed=path_backed)
        self._import_serial()
        self.conn.load_from_lerobot("parallel", self.source, parallelism=2,
                                    batch_size=1, tag_name="imported")
        for suffix in ("", "__episodes", "__tasks", "__info"):
            def read(name, tag=None):
                table = self.conn.catalog.get_table("default." + name)
                if tag:
                    table = table.copy({"scan.tag-name": tag})
                builder = table.new_read_builder()
                return builder.new_read().to_arrow(builder.new_scan().plan().splits())

            left = read("serial" + suffix)
            right = read("parallel" + suffix, "imported")
            if not suffix:
                from pypaimon.table.row.blob import Blob
                left, right = left.sort_by("index"), right.sort_by("index")
                file_io = self.conn.get_table("parallel").raw_table.file_io
                for original, imported in zip(left["camera"], right["camera"]):
                    self.assertEqual(
                        Blob.from_descriptor_bytes(original.as_py(), file_io=file_io).to_data(),
                        Blob.from_descriptor_bytes(imported.as_py(), file_io=file_io).to_data())
                left, right = left.drop(["camera"]), right.drop(["camera"])
            else:
                left = left.sort_by(left.column_names[0])
                right = right.sort_by(right.column_names[0])
            self.assertTrue(left.equals(right), suffix)
        # One data snapshot plus the initial BTree snapshot, regardless of task count.
        table = self.conn.get_table("parallel").raw_table
        self.assertEqual(2, table.snapshot_manager().get_latest_snapshot().id)

    @unittest.skipUnless(
        importlib.util.find_spec("av")
        and importlib.util.find_spec("torch")
        and importlib.util.find_spec("datasets"),
        "PyAV, PyTorch, and datasets are required",
    )
    def test_real_videos_are_readable_after_parallel_import(self):
        self._source(video=True)
        self.conn.load_from_lerobot("videos", self.source, parallelism=2, batch_size=1)
        self._import_serial()
        from pypaimon.multimodal.lerobot import PaimonLeRobotDataset
        first = PaimonLeRobotDataset(self.conn.get_table("videos"), video_backend="pyav", return_uint8=True)
        second = PaimonLeRobotDataset(self.conn.get_table("serial"), video_backend="pyav", return_uint8=True)
        try:
            for i in (7, 0, 3, 4, 1, 6, 2, 5):
                self.assertTrue(np.array_equal(first[i]["camera"], second[i]["camera"]))
        finally:
            first.close()
            second.close()

    def test_worker_failure_does_not_publish_frames_or_tags(self):
        rows = self._source(data_files=4)
        # Corruption in the last Episode is detected by its worker, after planning.
        rows = rows.set_column(rows.schema.get_field_index("frame_index"), "frame_index",
                               pa.array([0, 1, 0, 1, 0, 1, 0, 99], type=pa.int64()))
        pq.write_table(rows.slice(6, 2),
                       self.source / "data/chunk-000/file-003.parquet")
        import ray
        from pypaimon.multimodal.lerobot.ray_loader import _abort_messages
        received = self.root / "success_received"
        files_before_failure = []
        connection = self.conn

        class MarkingRay:
            def __getattr__(self, name):
                return getattr(ray, name)

            def get(self, ref, **kwargs):
                result = ray.get(ref, **kwargs)
                if isinstance(result, list) and result and hasattr(result[0], "new_files"):
                    table = connection.get_table("broken").raw_table
                    files_before_failure.extend(_bucket_files(table))
                    received.touch()
                return result

        def prepare(*args):
            if 3 in args[-1]:
                deadline = time.monotonic() + 30
                while not received.exists():
                    if time.monotonic() > deadline:
                        raise TimeoutError("Successful task result was not received.")
                    time.sleep(0.01)
            return _prepare_group(*args)

        with patch("pypaimon.multimodal.lerobot.ray_loader._require_ray",
                   return_value=MarkingRay()), \
                patch("pypaimon.multimodal.lerobot.ray_loader._prepare_group",
                      prepare), \
                patch("pypaimon.multimodal.lerobot.ray_loader._abort_messages",
                      wraps=_abort_messages) as abort:
            with self.assertRaisesRegex(Exception, "frame_index"):
                self.conn.load_from_lerobot("broken", self.source, parallelism=2,
                                            tag_name="imported")
        self.assertTrue(files_before_failure)
        abort.assert_called_once()
        self.assertTrue(abort.call_args[0][1])
        table = self.conn.get_table("broken").raw_table
        self.assertIsNone(table.snapshot_manager().get_latest_snapshot())
        self.assertFalse(table.tag_manager().tag_exists("imported"))
        self.assertEqual([], _bucket_files(table))
        self.assertTrue(all(not path.exists() for path in files_before_failure))

    def test_writer_close_failure_aborts_prepared_files(self):
        self._source()
        for error_type in (RuntimeError, KeyboardInterrupt):
            with self.subTest(error_type=error_type):
                table_name = "close_" + error_type.__name__.lower()
                trace = self.root / (table_name + ".json")

                def prepare(*args):
                    from pypaimon.multimodal.lerobot.ray_loader import _prepare_group
                    from pypaimon.write.table_write import BatchTableWrite

                    original_close = BatchTableWrite.close

                    def fail_close(write):
                        original_close(write)
                        trace.write_text(json.dumps(
                            [path.name for path in _bucket_files(args[0])]))
                        raise error_type("injected writer close")

                    BatchTableWrite.close = fail_close
                    try:
                        return _prepare_group(*args)
                    finally:
                        BatchTableWrite.close = original_close

                with patch("pypaimon.multimodal.lerobot.ray_loader._prepare_group",
                           prepare):
                    with self.assertRaisesRegex(BaseException, "injected writer close"):
                        self.conn.load_from_lerobot(
                            table_name, self.source, parallelism=2)
                before = json.loads(trace.read_text())
                self.assertTrue(any(name.endswith(".parquet") for name in before), before)
                self.assertTrue(any(".vector." in name for name in before), before)
                self.assertTrue(any(name.endswith(".blob") for name in before), before)
                table = self.conn.get_table(table_name).raw_table
                self.assertIsNone(table.snapshot_manager().get_latest_snapshot())
                self.assertEqual([], _bucket_files(table))

    def test_interrupt_during_result_delivery_aborts_ready_task(self):
        import ray
        self._source()
        delivered = []

        class InterruptingRay:
            def __getattr__(self, name):
                return getattr(ray, name)

            def get(self, task, **kwargs):
                result = ray.get(task, **kwargs)
                if not delivered:
                    delivered.extend(result)
                    raise KeyboardInterrupt("interrupted result delivery")
                return result

        with patch("pypaimon.multimodal.lerobot.ray_loader._require_ray",
                   return_value=InterruptingRay()):
            with self.assertRaisesRegex(KeyboardInterrupt, "interrupted result delivery"):
                self.conn.load_from_lerobot("interrupted", self.source, parallelism=2)
        self.assertTrue(delivered)
        table = self.conn.get_table("interrupted").raw_table
        self.assertIsNone(table.snapshot_manager().get_latest_snapshot())
        self.assertEqual([], _bucket_files(table))

    def test_source_close_interrupt_aborts_prepared_files(self):
        self._source()
        trace = self.root / "source-close.json"

        def prepare(*args):
            from contextlib import contextmanager
            from pypaimon.multimodal.lerobot import ray_loader
            original = ray_loader._resolved_source

            @contextmanager
            def interrupted_source(*source_args):
                with original(*source_args) as source:
                    yield source
                trace.write_text(json.dumps(
                    [path.name for path in _bucket_files(args[0])]))
                raise KeyboardInterrupt("interrupted source close")

            with patch.object(ray_loader, "_resolved_source", interrupted_source):
                return ray_loader._prepare_group(*args)

        with patch("pypaimon.multimodal.lerobot.ray_loader._prepare_group", prepare):
            with self.assertRaisesRegex(BaseException, "interrupted source close"):
                self.conn.load_from_lerobot("source_close", self.source, parallelism=2)
        self.assertTrue(json.loads(trace.read_text()))
        table = self.conn.get_table("source_close").raw_table
        self.assertIsNone(table.snapshot_manager().get_latest_snapshot())
        self.assertEqual([], _bucket_files(table))

    def test_invalid_commit_options_fail_before_preparing(self):
        self._source()
        for parallelism in (1, 2):
            with self.subTest(parallelism=parallelism):
                name = "invalid_commit_%d" % parallelism
                with patch("pypaimon.multimodal.lerobot.loader._prepare_dataset") as prepare, \
                        patch("pypaimon.multimodal.lerobot.ray_loader._prepare_group") as group:
                    with self.assertRaises(ValueError):
                        self.conn.load_from_lerobot(
                            name, self.source.as_uri(), parallelism=parallelism,
                            options={"commit.max-retries": "typo"})
                    prepare.assert_not_called()
                    group.assert_not_called()
                table = self.conn.get_table(name).raw_table
                self.assertIsNone(table.snapshot_manager().get_latest_snapshot())
                self.assertEqual([], _bucket_files(table))

    def test_worker_failure_does_not_wait_for_blocked_task(self):
        import ray
        if ray.cluster_resources().get("CPU", 0) < 2:
            self.skipTest("Two Ray CPUs are required to run both tasks")
        self._source(data_files=2)
        started = self.root / "blocked_task_started"
        triggered = self.root / "failure_triggered"
        release = self.root / "release_blocked_task"
        stopped = self.root / "blocked_task_stopped"
        refs = set()

        class TrackingRay:
            def __getattr__(self, name):
                return getattr(ray, name)

            def wait(self, tasks, **kwargs):
                refs.update(tasks)
                return ray.wait(tasks, **kwargs)

        def prepare(*args):
            if args[-1][0] == 0:
                deadline = time.monotonic() + 30
                while not started.exists():
                    if time.monotonic() > deadline:
                        raise TimeoutError("Blocked worker did not start.")
                    time.sleep(0.01)
                triggered.write_text(str(time.monotonic()))
                raise ValueError("planned worker failure")
            started.touch()
            try:
                deadline = time.monotonic() + 15
                while not release.exists():
                    if time.monotonic() > deadline:
                        raise TimeoutError("Blocked task was not released.")
                    time.sleep(0.05)
                return []
            finally:
                stopped.touch()

        from pypaimon.multimodal.lerobot.ray_loader import _abort_messages
        try:
            with patch("pypaimon.multimodal.lerobot.ray_loader._require_ray",
                       return_value=TrackingRay()), \
                    patch("pypaimon.multimodal.lerobot.ray_loader._prepare_group",
                          prepare), \
                    patch("pypaimon.multimodal.lerobot.ray_loader._abort_messages",
                          wraps=_abort_messages) as abort:
                with self.assertRaisesRegex(Exception, "planned worker failure"):
                    self.conn.load_from_lerobot(
                        "blocked", self.source, parallelism=2)
            elapsed = time.monotonic() - float(triggered.read_text())
            abort.assert_called_once()
            self.assertEqual([], abort.call_args[0][1])
            self.assertLess(elapsed, 6)
        finally:
            release.touch()
            deadline = time.monotonic() + 5
            while not stopped.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            if not stopped.exists():
                for ref in refs:
                    ray.cancel(ref, force=True)
            if refs:
                ray.wait(list(refs), num_returns=len(refs), timeout=2)
        self.assertTrue(stopped.exists())
