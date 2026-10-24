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

import io
import multiprocessing
import os
import pickle
import threading
import unittest
from collections import OrderedDict
from types import SimpleNamespace
from unittest.mock import patch

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.multimodal.video import VideoFrameCollator, _VideoDecodePool
from pypaimon.table.row.blob import BlobDescriptor


def identity(value):
    return value


def decode_worker(collator, pool, connection):
    try:
        requests = OrderedDict(
            ((BlobDescriptor("child-%s" % i, 0, 1), None), i)
            for i in range(2))
        with patch("pypaimon.multimodal.video.Blob.from_file", side_effect=lambda *args:
                   SimpleNamespace(new_input_stream=lambda: io.BytesIO(b"video"))):
            with collator._map_decoders(
                    requests, lambda stream, value: value, 2, pool.get(2)) as values:
                result = list(values)
        connection.send((result, collator._owner_pid == os.getpid()))
    finally:
        pool.close()
        collator.close()
        connection.close()


class VideoDecodeParallelismTest(unittest.TestCase):
    def collator(self, capacity=2):
        return VideoFrameCollator(
            SimpleNamespace(file_io="test-file-io"), video_column="camera",
            decoder_factory=identity, decode_fn=identity,
            max_open_videos=capacity)

    def requests(self, count=8):
        return OrderedDict(
            ((BlobDescriptor("video-%s" % i, 0, 1), None), i)
            for i in range(count))

    def streams(self, *args):
        return SimpleNamespace(new_input_stream=lambda: io.BytesIO(b"video"))

    def test_shared_limit_across_cameras_and_eviction(self):
        from concurrent.futures import ThreadPoolExecutor

        pool = _VideoDecodePool()
        executor = pool.get(2)
        collators = [self.collator() for _ in range(3)]
        active = [0, 0]
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def decode(stream, value):
            with lock:
                active[0] += 1
                active[1] = max(active)
            try:
                barrier.wait(timeout=5)
                self.assertFalse(stream.closed)
                return value
            finally:
                with lock:
                    active[0] -= 1

        def camera(collator, count):
            with collator._map_decoders(self.requests(count), decode, 2, executor) as values:
                return list(values)

        try:
            with patch("pypaimon.multimodal.video.Blob.from_file", self.streams), \
                    ThreadPoolExecutor(max_workers=3) as cameras:
                # One camera has a single payload; it must also share the budget.
                futures = [cameras.submit(camera, c, n)
                           for c, n in zip(collators, (1, 3, 8))]
                self.assertEqual([f.result() for f in futures],
                                 [list(range(n)) for n in (1, 3, 8)])
            self.assertEqual(active, [0, 2])
            self.assertIs(executor, pool.get(2))
        finally:
            pool.close()
            for collator in collators:
                streams = list(collator._decoders.values())
                collator.close()
                self.assertTrue(all(stream.closed for _, stream in streams))
        self.assertTrue(executor._shutdown)

    def test_error_drains_running_decodes(self):
        pool = _VideoDecodePool()
        executor = pool.get(2)
        collator = self.collator()
        barrier = threading.Barrier(2)
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        errors = []

        def decode(stream, value):
            barrier.wait(timeout=5)
            if value == 0:
                raise ValueError("decode failed")
            started.set()
            self.assertTrue(release.wait(5))
            self.assertFalse(stream.closed)
            return value

        def consume():
            try:
                with collator._map_decoders(self.requests(2), decode, 2, executor) as values:
                    list(values)
            except Exception as error:
                errors.append(error)
            finally:
                finished.set()

        with patch("pypaimon.multimodal.video.Blob.from_file", self.streams):
            thread = threading.Thread(target=consume)
            thread.start()
            try:
                self.assertTrue(started.wait(5))
                self.assertFalse(finished.is_set())
            finally:
                release.set()
                thread.join(5)
                pool.close()
                collator.close()
        self.assertTrue(finished.is_set())
        self.assertEqual(str(errors[0]), "decode failed")

    def test_serial_and_pickle(self):
        pool = _VideoDecodePool()
        self.assertIsNone(pool.get(1))
        collator = self.collator(1)
        owner = threading.get_ident()
        with patch("pypaimon.multimodal.video.Blob.from_file", self.streams):
            with collator._map_decoders(
                    self.requests(3), lambda stream, value: threading.get_ident(), 1) as values:
                self.assertEqual(list(values), [owner] * 3)
        pool.get(2).submit(identity, 1).result()
        restored_collator, restored_pool = pickle.loads(pickle.dumps((collator, pool)))
        self.assertIsNone(restored_pool._executor)
        self.assertFalse(restored_collator._decoders)
        pool.close()
        collator.close()
        restored_pool.close()
        restored_collator.close()

    @unittest.skipUnless(hasattr(os, "fork"), "requires fork")
    def test_fork_discards_inherited_executor(self):
        pool = _VideoDecodePool()
        executor = pool.get(2)
        try:
            with patch("pypaimon.multimodal.video.os.getpid", return_value=-1), \
                    patch.object(executor, "shutdown", side_effect=AssertionError("inherited pool")):
                child_executor = pool.get(2)
                self.assertIsNot(executor, child_executor)
                pool.close()
                self.assertTrue(child_executor._shutdown)
        finally:
            executor.shutdown(wait=True)

    def test_workers_rebuild_pool(self):
        collator = self.collator()
        pool = _VideoDecodePool()
        pool.get(2).submit(identity, 1).result()
        try:
            for method in ("spawn", "fork"):
                if method not in multiprocessing.get_all_start_methods():
                    continue
                with self.subTest(method=method):
                    ctx = multiprocessing.get_context(method)
                    receive, send = ctx.Pipe(duplex=False)
                    worker = ctx.Process(target=decode_worker, args=(collator, pool, send))
                    worker.start()
                    send.close()
                    try:
                        self.assertTrue(receive.poll(15), "worker did not complete")
                        self.assertEqual(receive.recv(), ([0, 1], True))
                        worker.join(5)
                        self.assertEqual(worker.exitcode, 0)
                    finally:
                        if worker.is_alive():
                            worker.terminate()
                            worker.join(5)
                        receive.close()
        finally:
            pool.close()
            collator.close()

    def test_parallel_windows_match_serial_and_preserve_torch_mode(self):
        try:
            import torch
        except ImportError as error:
            self.skipTest(str(error))
        from pypaimon.multimodal.lerobot.dataset import _decode_video_windows
        from pypaimon.table.row.blob import VideoFrameDescriptor

        pixels = torch.arange(4 * 4 * 5 * 3).reshape(4, 4, 5, 3).to(torch.uint8)
        frames = pixels.permute(0, 3, 1, 2)
        rows = {i: {"camera": VideoFrameDescriptor(
            "video-%s" % (i // 4), 0, 1, i % 4, -1, 0).serialize()}
            for i in range(12)}
        plans = [{"windows": {"camera": w}} for w in (
            [0, 0, 2], [4, 5, 6], [8, 9, 10], [0, 1, 3])]
        features = {"camera": {"dtype": "video", "shape": [4, 5, 3]}}
        modes = []

        def read(indices):
            modes.append((torch.is_grad_enabled(), torch.is_inference_mode_enabled()))
            # Preserve the noncontiguous layout returned by TorchCodec.
            return SimpleNamespace(data=pixels[indices].permute(0, 3, 1, 2))

        collator = self.collator(2)
        decoder = SimpleNamespace(get_frames_at=read)
        try:
            with patch.object(collator, "_decoder", return_value=decoder), \
                    patch("torch.get_num_threads", return_value=1), \
                    torch.inference_mode():
                for uint8 in (True, False):
                    for batch in (True, False):
                        with self.subTest(uint8=uint8, batch=batch):
                            kwargs = dict(return_batch=batch, share_memory=batch)
                            expected = _decode_video_windows(
                                plans, rows, [collator], features, uint8,
                                decode_parallelism=1, **kwargs)["camera"]
                            actual = _decode_video_windows(
                                plans, rows, [collator], features, uint8,
                                decode_parallelism=4, **kwargs)["camera"]
                            if not batch:
                                expected, actual = torch.stack(expected), torch.stack(actual)
                            self.assertTrue(torch.equal(expected, actual))
                            self.assertTrue(actual.is_contiguous())
                            if batch:
                                self.assertTrue(actual.is_shared())
                            saved = actual[3].clone()
                            actual[0].zero_()
                            self.assertTrue(torch.equal(saved, actual[3]))
            self.assertTrue(modes)
            self.assertEqual(set(modes), {(False, True)})
            self.assertTrue(torch.equal(frames, pixels.permute(0, 3, 1, 2)))
        finally:
            collator.close()

    def test_video_windows_share_worker_pool_across_cameras(self):
        try:
            import torch
        except ImportError as error:
            self.skipTest(str(error))
        from contextlib import ExitStack
        from pypaimon.multimodal.lerobot.dataset import _decode_video_windows
        from pypaimon.table.row.blob import VideoFrameDescriptor

        pool = _VideoDecodePool()
        collators = [self.collator(1) for _ in range(3)]
        names = ["camera-%s" % i for i in range(3)]
        for collator, name in zip(collators, names):
            collator.video_column = name
        rows = {0: {name: VideoFrameDescriptor(name, 0, 1, 0, -1, 0).serialize()
                    for name in names}}
        plans = [{"windows": {name: [0] for name in names}}]
        features = {name: {"dtype": "video", "shape": [4, 5, 3]} for name in names}
        threads = set()
        lock = threading.Lock()

        def read(indices):
            with lock:
                threads.add(threading.get_ident())
            return SimpleNamespace(data=torch.ones((len(indices), 3, 4, 5), dtype=torch.uint8))

        try:
            with ExitStack() as stack:
                stack.enter_context(patch("torch.get_num_threads", return_value=1))
                for collator in collators:
                    stack.enter_context(patch.object(
                        collator, "_decoder", return_value=SimpleNamespace(get_frames_at=read)))
                for parallelism in (1, 2):
                    threads.clear()
                    result = _decode_video_windows(
                        plans, rows, collators, features, True, return_batch=True,
                        decode_parallelism=parallelism, decode_pool=pool)
                    self.assertEqual(set(result), set(names))
                    self.assertLessEqual(len(threads), parallelism)
                    for value in result.values():
                        self.assertTrue(torch.equal(value, torch.ones_like(value)))
                    if parallelism == 1:
                        self.assertEqual(threads, {threading.get_ident()})
                    else:
                        self.assertNotIn(threading.get_ident(), threads)
                executor = pool.get(2)
                _decode_video_windows(plans, rows, collators, features, True,
                                      decode_parallelism=2, decode_pool=pool)
                self.assertIs(executor, pool.get(2))
        finally:
            pool.close()
            for collator in collators:
                collator.close()

    def test_dataloader_fallback_honors_decode_limit(self):
        try:
            import torch
        except ImportError as error:
            self.skipTest(str(error))
        import time
        import pyarrow as pa
        import pypaimon.multimodal as pm

        names = ("camera_a", "camera_b")
        features = {name: {"dtype": "int64", "shape": [1]}
                    for name in ("index", "episode_index", "frame_index", "task_index")}
        features["timestamp"] = {"dtype": "float32", "shape": [1]}
        features.update({name: {"dtype": "video", "shape": [2, 2, 3]} for name in names})
        metadata = {
            "repo_id": "test/fallback",
            "info": {"codebase_version": "v3.0", "fps": 10, "total_frames": 1,
                     "total_episodes": 1, "total_tasks": 1, "features": features},
            "episodes": [{"episode_index": 0, "dataset_from_index": 0,
                          "dataset_to_index": 1, "length": 1, "tasks": ["pick"]}],
            "tasks": ["pick"], "stats": {},
        }

        class Reader(pm.PaimonDatasetReader):
            def read_indices(self, indices, columns):
                row = {name: 0 for name in features}
                row.update({name: b"video" for name in names})
                return pa.Table.from_pylist(
                    [{name: row[name] for name in columns}], schema=self.schema)

        active = [0, 0]
        lock = threading.Lock()

        class Collator:
            def __init__(self, name):
                self.video_column = self.output_column = name

            def __call__(self, rows):
                with lock:
                    active[0] += 1
                    active[1] = max(active)
                try:
                    time.sleep(0.02)
                    return [dict(row, **{self.output_column: torch.ones(
                        (3, 2, 2), dtype=torch.uint8)}) for row in rows]
                finally:
                    with lock:
                        active[0] -= 1

            def close(self):
                pass

        for transforms in (None, identity):
            with self.subTest(transforms=transforms):
                reader = Reader(metadata, file_io="test-file-io",
                                delta_timestamps={name: [0.0] for name in names},
                                image_transforms=transforms, return_uint8=True)
                reader._video_collators = [Collator(name) for name in names]
                reader._video_decode_parallelism = 1
                dataset = pm.PaimonLeRobotDataset(reader)
                active[:] = [0, 0]
                try:
                    with patch("torch.get_num_threads", return_value=2):
                        batch = next(iter(dataset.to_dataloader(batch_size=1, num_workers=0)))
                    self.assertEqual(active, [0, 1])
                    for name in names:
                        self.assertTrue(torch.equal(batch[name], torch.ones_like(batch[name])))
                finally:
                    dataset.close()

    def test_single_payload_shared_batch_matches_windows(self):
        try:
            import torch
        except ImportError as error:
            self.skipTest(str(error))
        from pypaimon.multimodal.lerobot.dataset import _decode_video_windows
        from pypaimon.table.row.blob import VideoFrameDescriptor

        pixels = torch.arange(10 * 4 * 5 * 3).reshape(10, 4, 5, 3).to(torch.uint8)
        rows = {i: {"camera": VideoFrameDescriptor("video", 0, 1, i, -1, 0).serialize()}
                for i in range(10)}
        windows = ([0, 0, 2], [2, 3, 4], [9, 7, 8], [0, 0, 2])
        plans = [{"windows": {"camera": indices}} for indices in windows]
        collator = self.collator()
        decoder = SimpleNamespace(get_frames_at=lambda indices: SimpleNamespace(
            data=pixels[indices].permute(0, 3, 1, 2)))
        original_device = torch.get_default_device()
        try:
            with patch.object(collator, "_decoder", return_value=decoder), \
                    patch("torch.get_num_threads", return_value=1), \
                    patch.object(torch.Tensor, "share_memory_",
                                 side_effect=AssertionError("Unexpected storage copy")):
                for default_device in ("cpu", "meta"):
                    torch.set_default_device(default_device)
                    for uint8 in (True, False):
                        result = _decode_video_windows(
                            plans, rows, [collator],
                            {"camera": {"dtype": "video", "shape": [4, 5, 3]}}, uint8,
                            return_batch=True, share_memory=True)["camera"]
                        expected = torch.stack([pixels[w].permute(0, 3, 1, 2) for w in windows])
                        if not uint8:
                            expected = expected.float().div_(255)
                        self.assertEqual(result.device.type, "cpu")
                        self.assertTrue(torch.equal(result, expected))
                        self.assertTrue(result.is_shared())
                        self.assertTrue(result.is_contiguous())
                        saved = result[3].clone()
                        result[0].zero_()
                        self.assertTrue(torch.equal(result[3], saved))
        finally:
            torch.set_default_device(original_device)
            collator.close()

    def test_codec_thread_option(self):
        self.assertEqual(0, CoreOptions(Options({})).read_video_codec_threads())
        for value in (0, 1, 4, "1"):
            self.assertEqual(int(value), CoreOptions(Options({
                "read.video.codec-threads": value})).read_video_codec_threads())
        for value in (-1, True, 1.5, "bad"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                CoreOptions(Options({"read.video.codec-threads": value})) \
                    .read_video_codec_threads()

    def test_codec_threads_preserve_real_indexed_and_scan_frames(self):
        try:
            import av
            import numpy as np
            import torch
        except ImportError as error:
            self.skipTest(str(error))
        from pypaimon.multimodal.lerobot.dataset import _open_video_decoder
        from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex

        target = io.BytesIO()
        with av.open(target, mode="w", format="mp4") as container:
            stream = container.add_stream("mpeg4", rate=30)
            stream.width, stream.height = 16, 16
            stream.pix_fmt = "yuv420p"
            stream.codec_context.gop_size = 4
            for value in range(12):
                image = np.full((16, 16, 3), value * 15, dtype=np.uint8)
                for packet in stream.encode(av.VideoFrame.from_ndarray(image, format="rgb24")):
                    container.mux(packet)
            for packet in stream.encode():
                container.mux(packet)
        payload = target.getvalue()
        index = VideoKeyframeIndex.inspect(io.BytesIO(payload), len(payload))
        outputs = []
        for indexed in (False, True):
            for threads in (0, 1, 2):
                source = io.BytesIO(payload)
                if indexed:
                    source.video_keyframe_index = index
                    source.video_length = len(payload)
                decoder = _open_video_decoder(source, backend="pyav", codec_threads=threads)
                try:
                    if indexed:
                        decode_group = decoder._decode_indexed_group

                        def checked_group(container, stream, *args):
                            self.assertEqual(threads, stream.thread_count)
                            return decode_group(container, stream, *args)

                        with patch.object(decoder, '_decode_indexed_group', checked_group):
                            frames = [decoder[i] for i in (9, 1, 7)]
                    else:
                        self.assertEqual(threads, decoder._stream.thread_count)
                        frames = [decoder[i] for i in (9, 1, 7)]
                    outputs.append(torch.stack(frames))
                finally:
                    decoder.close()
                    source.close()
        for output in outputs[1:]:
            self.assertTrue(torch.equal(outputs[0], output))

    def test_real_pyav_payloads_match_serial(self):
        try:
            import av
            import numpy as np
            import torch
        except ImportError as error:
            self.skipTest(str(error))
        from functools import partial
        from pypaimon.multimodal.lerobot.dataset import (
            _decode_video_windows, _open_video_decoder)
        from pypaimon.table.row.blob import VideoFrameDescriptor

        videos = {}
        for video in range(3):
            target = io.BytesIO()
            with av.open(target, mode="w", format="mp4") as container:
                stream = container.add_stream("mpeg4", rate=30)
                stream.width, stream.height = 16, 16
                stream.pix_fmt = "yuv420p"
                for frame in range(8):
                    image = np.full((16, 16, 3), video * 60 + frame * 5, dtype=np.uint8)
                    for packet in stream.encode(av.VideoFrame.from_ndarray(image, format="rgb24")):
                        container.mux(packet)
                for packet in stream.encode():
                    container.mux(packet)
            videos["video-%s" % video] = target.getvalue()
        rows = {i: {"camera": VideoFrameDescriptor(
            "video-%s" % (i // 8), 0, len(videos["video-%s" % (i // 8)]),
            i % 8, -1, 0).serialize()} for i in range(24)}
        plans = [{"windows": {"camera": indices}} for indices in (
            [0, 0, 3], [8, 9, 11], [16, 18, 19], [1, 2, 4])]
        outputs = []

        def blob(file_io, uri, offset, length):
            return SimpleNamespace(new_input_stream=lambda: io.BytesIO(videos[uri]))

        for parallelism in (1, 4):
            collator = VideoFrameCollator(
                SimpleNamespace(file_io="test"), video_column="camera",
                decoder_factory=partial(_open_video_decoder, backend="pyav"),
                decode_fn=identity, max_open_videos=2)
            try:
                with patch("pypaimon.multimodal.video.Blob.from_file", blob), \
                        patch("torch.get_num_threads", return_value=1):
                    outputs.append(_decode_video_windows(
                        plans, rows, [collator],
                        {"camera": {"dtype": "video", "shape": [16, 16, 3]}}, True,
                        return_batch=True, decode_parallelism=parallelism)["camera"])
            finally:
                collator.close()
        self.assertTrue(torch.equal(*outputs))
        outputs = []
        single_plans = [{"windows": {"camera": indices}} for indices in (
            [6, 1, 6], [0, 1, 2], [2, 3, 4], [4, 5, 7])]
        for replicas in (1, 4):
            collator = VideoFrameCollator(
                SimpleNamespace(file_io="test"), video_column="camera",
                decoder_factory=partial(_open_video_decoder, backend="pyav"),
                decode_fn=identity, max_open_videos=4)
            try:
                with patch("pypaimon.multimodal.video.Blob.from_file", blob), \
                        patch("torch.get_num_threads", return_value=1):
                    outputs.append(_decode_video_windows(
                        single_plans, rows, [collator],
                        {"camera": {"dtype": "video", "shape": [16, 16, 3]}}, True,
                        return_batch=True, decode_parallelism=4,
                        decoders_per_payload=replicas)["camera"])
                    self.assertEqual(len(collator._decoders), replicas)
            finally:
                collator.close()
        self.assertTrue(torch.equal(*outputs))

    def test_single_payload_windows_reuse_independent_decoders(self):
        try:
            import torch
        except ImportError as error:
            self.skipTest(str(error))
        from pypaimon.multimodal.lerobot.dataset import _decode_video_windows
        from pypaimon.table.row.blob import VideoFrameDescriptor

        windows = [[7, 2, 7], [0, 1, 2], [5, 6, 7], [2, 3, 4]]
        rows = {i: {"camera": VideoFrameDescriptor(
            "one-video", 0, 1, i, -1, 0).serialize()} for i in range(8)}
        plans = [{"windows": {"camera": window}} for window in windows]
        features = {"camera": {"dtype": "video", "shape": [4, 5, 3]}}
        for parallelism, capacity, replicas, expected in (
                (1, 4, 4, 1), (4, 1, 4, 1), (2, 4, 4, 2),
                (4, 4, 1, 1), (4, 4, 4, 4), (8, 8, 16, 4)):
            with self.subTest(parallelism=parallelism, capacity=capacity, replicas=replicas):
                collator = self.collator(capacity)
                created = []
                barrier = threading.Barrier(expected)

                class Decoder:
                    def __init__(self, stream):
                        self.stream = stream
                        self.closed = False
                        created.append(self)

                    def get_frames_at(self, indices):
                        barrier.wait(timeout=10)
                        # Non-contiguous decoder output exercises layout conversion.
                        data = torch.tensor(indices, dtype=torch.uint8)[:, None, None, None]
                        return SimpleNamespace(data=data.expand(-1, 4, 5, 3).permute(0, 3, 1, 2))

                    def close(self):
                        self.closed = True

                collator.decoder_factory = Decoder
                try:
                    with patch("pypaimon.multimodal.video.Blob.from_file", self.streams), \
                            patch("torch.get_num_threads", return_value=1):
                        for return_batch in (False, True):
                            for uint8 in (True, False):
                                result = _decode_video_windows(
                                    plans, rows, [collator], features, uint8,
                                    return_batch=return_batch, share_memory=return_batch,
                                    decode_parallelism=parallelism,
                                    decoders_per_payload=replicas)["camera"]
                                target = torch.tensor(windows, dtype=torch.uint8)[:, :, None, None, None]
                                target = target.expand(-1, -1, 3, 4, 5)
                                if not uint8:
                                    target = target.float() / 255
                                actual = result if return_batch else torch.stack(result)
                                self.assertTrue(torch.equal(actual, target))
                                self.assertTrue(actual.is_contiguous())
                                if return_batch:
                                    self.assertTrue(result.is_shared())
                                result[0].zero_()
                                self.assertTrue(torch.equal(result[2], target[2]))
                        self.assertEqual(len(created), expected)
                        self.assertEqual(len({id(d.stream) for d in created}), expected)
                finally:
                    collator.close()
                self.assertTrue(all(d.closed and d.stream.closed for d in created))

    def test_single_payload_split_bounds_duplicate_frame_requests(self):
        try:
            import torch
        except ImportError as error:
            self.skipTest(str(error))
        from pypaimon.multimodal.lerobot.dataset import _decode_video_windows
        from pypaimon.table.row.blob import VideoFrameDescriptor

        features = {"camera": {"dtype": "video", "shape": [4, 5, 3]}}

        def decode(windows):
            rows = {i: {"camera": VideoFrameDescriptor(
                "one-video", 0, 1, i, -1, 0).serialize()}
                for i in {index for window in windows for index in window}}
            plans = [{"windows": {"camera": window}} for window in windows]
            collator = self.collator(4)
            calls = []

            class Decoder:
                def __init__(self, stream):
                    self.stream = stream

                def get_frames_at(self, indices):
                    calls.append(tuple(indices))
                    data = torch.tensor(
                        indices, dtype=torch.uint8)[:, None, None, None]
                    return SimpleNamespace(
                        data=data.expand(-1, 4, 5, 3).permute(0, 3, 1, 2))

                def close(self):
                    pass

            collator.decoder_factory = Decoder
            try:
                with patch("pypaimon.multimodal.video.Blob.from_file", self.streams), \
                        patch("torch.get_num_threads", return_value=1):
                    result = _decode_video_windows(
                        plans, rows, [collator], features, True,
                        return_batch=True, decode_parallelism=4,
                        decoders_per_payload=4)["camera"]
                target = torch.tensor(
                    windows, dtype=torch.uint8)[:, :, None, None, None]
                target = target.expand(-1, -1, 3, 4, 5)
                self.assertTrue(torch.equal(result, target))
                return calls
            finally:
                collator.close()

        repeated = decode([[0, 1, 2]] * 4)
        self.assertEqual(repeated, [(0, 1, 2)])

        overlapping = decode([list(range(i, i + 8)) for i in range(8)])
        self.assertEqual(len(overlapping), 2)
        self.assertEqual(sum(len(indices) for indices in overlapping), 22)
        self.assertEqual(set().union(*map(set, overlapping)), set(range(15)))

    def test_replica_cache_eviction_and_pickle(self):
        collator = self.collator(2)
        descriptor = BlobDescriptor("one-video", 0, 1)
        with patch("pypaimon.multimodal.video.Blob.from_file", self.streams):
            first = collator._decoder(descriptor, None, 0)
            second = collator._decoder(descriptor, None, 1)
            self.assertIs(collator._decoder(descriptor), first)
            third = collator._decoder(descriptor, None, 2)
            self.assertTrue(second.closed)
            self.assertFalse(first.closed)
            child = pickle.loads(pickle.dumps(collator))
            self.assertFalse(child._decoders)
            child.close()
            collator.close()
            self.assertTrue(first.closed and third.closed)

    def test_options(self):
        self.assertEqual(CoreOptions(Options({})).read_video_decoders_per_payload(), 4)
        self.assertEqual(CoreOptions(Options({"read.video.decoders-per-payload": "1"}))
                         .read_video_decoders_per_payload(), 1)
        for value in (0, -1, True, 1.5, "no", "1.5"):
            with self.subTest(replicas=value), self.assertRaises(ValueError):
                CoreOptions(Options({"read.video.decoders-per-payload": value})) \
                    .read_video_decoders_per_payload()
        self.assertEqual(CoreOptions(Options({})).read_video_decode_parallelism(), 16)
        self.assertEqual(CoreOptions(Options({"read.video.decode-parallelism": "1"}))
                         .read_video_decode_parallelism(), 1)
        self.assertEqual(CoreOptions(Options({"read.video.decode-parallelism": "4"}))
                         .read_video_decode_parallelism(), 4)
        for value in (0, -1, True, 1.5, "no", "1.5"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                CoreOptions(Options({"read.video.decode-parallelism": value})) \
                    .read_video_decode_parallelism()
