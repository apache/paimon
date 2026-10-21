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
from pypaimon.multimodal.video import VideoFrameCollator
from pypaimon.table.row.blob import BlobDescriptor


def identity(value):
    return value


def decode_worker(collator, connection):
    try:
        requests = OrderedDict(
            ((BlobDescriptor("child-%s" % i, 0, 1), None), i)
            for i in range(2))
        with patch("pypaimon.multimodal.video.Blob.from_file", side_effect=lambda *args:
                   SimpleNamespace(new_input_stream=lambda: io.BytesIO(b"video"))):
            with collator._map_decoders(
                    requests, lambda stream, value: value, 2) as values:
                result = list(values)
        connection.send((result, collator._owner_pid == os.getpid()))
    finally:
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

    def test_bounded_parallelism_and_eviction(self):
        collator = self.collator()
        barrier = threading.Barrier(2)
        active = [0, 0]
        lock = threading.Lock()

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

        with patch("pypaimon.multimodal.video.Blob.from_file", self.streams):
            with collator._map_decoders(self.requests(), decode, 4) as values:
                self.assertEqual(list(values), list(range(8)))
            self.assertEqual(active, [0, 2])
            executor = collator._decode_executor
            with collator._map_decoders(self.requests(2), decode, 4) as values:
                self.assertEqual(list(values), [0, 1])
            self.assertIs(executor, collator._decode_executor)
        streams = list(collator._decoders.values())
        collator.close()
        self.assertTrue(executor._shutdown)
        self.assertTrue(all(stream.closed for _, stream in streams))

    def test_error_drains_running_decodes(self):
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
                with collator._map_decoders(self.requests(2), decode, 2) as values:
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
                collator.close()
        self.assertTrue(finished.is_set())
        self.assertEqual(str(errors[0]), "decode failed")

    def test_serial_and_pickle(self):
        collator = self.collator(1)
        owner = threading.get_ident()
        with patch("pypaimon.multimodal.video.Blob.from_file", self.streams):
            with collator._map_decoders(
                    self.requests(3), lambda stream, value: threading.get_ident(), 4) as values:
                self.assertEqual(list(values), [owner] * 3)
        self.assertIsNone(collator._decode_executor)
        collator.close()
        collator = self.collator()
        with patch("pypaimon.multimodal.video.Blob.from_file", self.streams):
            with collator._map_decoders(
                    self.requests(2), lambda stream, value: value, 2) as values:
                self.assertEqual(list(values), [0, 1])
        restored = pickle.loads(pickle.dumps(collator))
        self.assertIsNone(restored._decode_executor)
        self.assertFalse(restored._decoders)
        collator.close()
        restored.close()

    @unittest.skipUnless(hasattr(os, "fork"), "requires fork")
    def test_fork_discards_inherited_executor(self):
        collator = self.collator()
        with patch("pypaimon.multimodal.video.Blob.from_file", self.streams):
            with collator._map_decoders(
                    self.requests(2), lambda stream, value: value, 2) as values:
                list(values)
            executor = collator._decode_executor
            # A child must discard the inherited pool, never join parent threads.
            with patch("pypaimon.multimodal.video.os.getpid", return_value=-1), \
                    patch.object(executor, "shutdown", side_effect=AssertionError("inherited pool")):
                collator._ensure_process_local_cache()
                self.assertIsNone(collator._decode_executor)
        executor.shutdown(wait=True)
        collator.close()

    def test_workers_rebuild_pool(self):
        collator = self.collator()
        with patch("pypaimon.multimodal.video.Blob.from_file", self.streams):
            with collator._map_decoders(
                    self.requests(2), lambda stream, value: value, 2) as values:
                list(values)
        try:
            for method in ("spawn", "fork"):
                if method not in multiprocessing.get_all_start_methods():
                    continue
                with self.subTest(method=method):
                    ctx = multiprocessing.get_context(method)
                    receive, send = ctx.Pipe(duplex=False)
                    worker = ctx.Process(target=decode_worker, args=(collator, send))
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

    def test_options(self):
        self.assertEqual(CoreOptions(Options({})).read_video_decode_parallelism(), 1)
        self.assertEqual(CoreOptions(Options({"read.video.decode-parallelism": "4"}))
                         .read_video_decode_parallelism(), 4)
        for value in (0, -1, True, 1.5, "no", "1.5"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                CoreOptions(Options({"read.video.decode-parallelism": value})) \
                    .read_video_decode_parallelism()
