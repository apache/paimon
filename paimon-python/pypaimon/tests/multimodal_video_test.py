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
import threading
from concurrent.futures import ThreadPoolExecutor
import os
import tempfile
import unittest
from types import SimpleNamespace

from parameterized import parameterized

from pypaimon.common.file_io import FileIO
from pypaimon.multimodal import VideoFrameCollator, VideoMetadataCache
from pypaimon.multimodal.lerobot.dataset import _decode_video_rows
from pypaimon.table.row.blob import VideoFrameDescriptor


class _CachedIndex:
    nbytes = 16


class _LockingFinalizerIndex(_CachedIndex):
    def __init__(self, lock):
        self.lock = lock

    def __del__(self):
        with self.lock:
            pass


def _read_with_inherited_metadata(cache, table, row, connection):
    def factory(stream):
        index = stream.video_metadata_cache.get_or_load("fork", _CachedIndex)
        return SimpleNamespace(index=index, close=lambda: None)

    collator = VideoFrameCollator(
        table, video_column="video", decoder_factory=factory,
        decode_fn=lambda decoder, ordinal, row: decoder.index.nbytes,
        collate_fn=list, metadata_cache=cache)
    try:
        try:
            collator([row])
        except RuntimeError as error:
            assert "new VideoMetadataCache" in str(error)
            connection.send("rejected")
        else:
            connection.send("unexpected inherited cache access")
    finally:
        collator.close()
        connection.close()


def _call_inherited_collator(collator, connection):
    try:
        connection.send("started")
        cache_id = id(collator._video_metadata_cache)
        for _ in range(2):
            try:
                collator([])
            except RuntimeError as error:
                assert "new VideoMetadataCache" in str(error)
            else:
                raise AssertionError("inherited shared cache accepted")
            assert id(collator._video_metadata_cache) == cache_id
            assert not collator._metadata_cache_owned
        connection.send("rejected")
    except BaseException as error:
        connection.send(repr(error))
    finally:
        connection.close()


def _read_inherited_cache_concurrently(cache, connection):
    from concurrent.futures import Future
    from unittest.mock import patch

    try:
        # Both first readers must reject inherited state without mutating it.
        barrier = threading.Barrier(2)

        def inherited_read():
            barrier.wait(timeout=5)
            try:
                cache.get_or_load("child", _CachedIndex)
            except RuntimeError as error:
                assert "new VideoMetadataCache" in str(error)
                return
            raise AssertionError("inherited cache accepted")

        with ThreadPoolExecutor(2) as executor:
            results = [executor.submit(inherited_read) for _ in range(2)]
            for result in results:
                result.result(timeout=5)
        assert cache._owner_pid != os.getpid()
        fresh_cache = VideoMetadataCache()
        started, waiting = threading.Event(), threading.Event()
        builds = []

        class WaitingFuture(Future):
            def result(self, timeout=None):
                waiting.set()
                return super().result(timeout=5)

        def load():
            builds.append(1)
            started.set()
            assert waiting.wait(5), "second reader did not join the pending load"
            return _CachedIndex()

        with patch("pypaimon.multimodal.video.Future", WaitingFuture):
            with ThreadPoolExecutor(2) as executor:
                first = executor.submit(fresh_cache.get_or_load, "child", load)
                assert started.wait(5)
                second = executor.submit(fresh_cache.get_or_load, "child", load)
                assert first.result(timeout=5) is second.result(timeout=5)
        assert builds == [1]
        connection.send("ok")
    except BaseException as error:
        connection.send(repr(error))
    finally:
        connection.close()


class _Decoder:

    def __init__(self, stream, calls):
        self._stream = stream
        self._calls = calls
        self.closed = False

    def decode(self, frame_index):
        self._stream.seek(0)
        return self._stream.read(), frame_index

    def close(self):
        self.closed = True
        self._calls.append("close")


class _FailingCloseDecoder(_Decoder):

    def close(self):
        super().close()
        raise RuntimeError("decoder close failed")


class RawVideoRemuxTest(unittest.TestCase):

    def test_read_only_packet_duration(self):
        from fractions import Fraction
        from unittest.mock import MagicMock, patch
        from pypaimon.multimodal.video import _remux_raw_hevc

        class Packet:
            size = 1

            def __init__(self, duration):
                self._duration = duration

            @property
            def duration(self):
                return self._duration

            def decode(self):
                return [SimpleNamespace(pts=self.pts, is_corrupt=False)]

        class Streams(list):
            @property
            def video(self):
                return self

        for duration in (48000, 0):
            with self.subTest(duration=duration), tempfile.TemporaryDirectory() as directory:
                packets = [Packet(24000), Packet(duration)]
                first, second, output = MagicMock(), MagicMock(), MagicMock()
                for container in (first, second, output):
                    container.__enter__.return_value = container
                    container.streams = Streams([SimpleNamespace()])
                first.format.name = "hevc"
                first.demux.return_value = [Packet(24000), Packet(48000)]
                second.demux.return_value = packets
                av = MagicMock()
                av.open.side_effect = [first, second, output]
                with patch("pypaimon.multimodal.video._require_video_av", return_value=av):
                    if duration == 0:
                        with self.assertRaisesRegex(ValueError, "no duration"):
                            _remux_raw_hevc(io.BytesIO(b"source"), os.path.join(directory, "out.mp4"), Fraction(1, 30))
                    else:
                        self.assertEqual(2, _remux_raw_hevc(
                            io.BytesIO(b"source"), os.path.join(directory, "out.mp4"), Fraction(1, 30)))
                        self.assertEqual(2, output.mux.call_count)
                        for i, packet in enumerate(packets):
                            self.assertEqual(Fraction(i, 30), packet.pts * packet.time_base)
                            self.assertEqual(Fraction(i, 30), packet.dts * packet.time_base)
                            self.assertEqual(Fraction(1, 30), packet.duration * packet.time_base)
                output.__exit__.assert_called_once()


class ImageVideoEncoderTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_encodes_ordered_image_bytes_as_video(self):
        try:
            import av
            from PIL import Image
        except ImportError:
            self.skipTest("PyAV and Pillow are required for video encoding")
        from pypaimon.multimodal.video import _encode_images_to_video

        images = []
        for color in ((255, 0, 0), (0, 255, 0)):
            output = io.BytesIO()
            Image.new("RGB", (8, 6), color).save(output, format="PNG")
            images.append(output.getvalue())
        video_path = os.path.join(self.temp_dir.name, "encoded.mp4")

        count = _encode_images_to_video(
            images,
            video_path,
            fps=10,
            codec="mpeg4",
            pixel_format="yuv420p",
            gop_size=1,
            codec_options={"qscale": "3"},
        )

        with av.open(video_path) as container:
            stream = container.streams.video[0]
            frames = list(container.decode(stream))
            size = stream.width, stream.height
        self.assertEqual(2, count)
        self.assertEqual(2, len(frames))
        self.assertEqual((8, 6), size)

    def test_image_video_encoder_rejects_invalid_required_options(self):
        from pypaimon.multimodal.video import _encode_images_to_video

        defaults = {
            "fps": 10,
            "codec": "mpeg4",
            "pixel_format": "yuv420p",
            "gop_size": 1,
        }
        cases = [
            ({"fps": True}, "fps must be a positive int"),
            ({"codec": ""}, "codec must be a non-empty string"),
            ({"pixel_format": ""},
             "pixel_format must be a non-empty string"),
            ({"gop_size": 0}, "gop_size must be a positive int"),
            ({"codec_options": []}, "codec_options must be a mapping"),
        ]
        for overrides, message in cases:
            options = dict(defaults)
            options.update(overrides)
            with self.subTest(options=options):
                with self.assertRaisesRegex(ValueError, message):
                    _encode_images_to_video(
                        [],
                        os.path.join(self.temp_dir.name, "invalid.mp4"),
                        **options,
                    )

    def test_image_video_encoder_rejects_mixed_dimensions_and_cleans_output(self):
        try:
            from PIL import Image
            import av  # noqa: F401
        except ImportError:
            self.skipTest("PyAV and Pillow are required for video encoding")
        from pypaimon.multimodal.video import _encode_images_to_video

        video_path = os.path.join(self.temp_dir.name, "mixed.mp4")
        with self.assertRaisesRegex(ValueError, "same dimensions"):
            _encode_images_to_video(
                [
                    Image.new("RGB", (8, 6), "red"),
                    Image.new("RGB", (10, 6), "green"),
                ],
                video_path,
                fps=10,
                codec="mpeg4",
                pixel_format="yuv420p",
                gop_size=1,
            )
        self.assertFalse(os.path.exists(video_path))


class VideoFrameCollatorTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.file_io = FileIO.get("file://" + self.temp_dir.name, {})
        self.table = SimpleNamespace(
            raw_table=SimpleNamespace(file_io=self.file_io)
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_metadata_cache_shared_across_collators(self):
        cache = VideoMetadataCache()
        builds = []

        class Index:
            nbytes = 16

        def factory(stream):
            key = ("container", stream.video_metadata_key)

            def load():
                builds.append(key)
                return Index()

            index = stream.video_metadata_cache.get_or_load(key, load)
            return SimpleNamespace(index=index, close=lambda: None)

        def collator():
            return VideoFrameCollator(
                self.table, video_column="video", decoder_factory=factory,
                decode_fn=lambda decoder, index, row: decoder.index,
                collate_fn=list, metadata_cache=cache)

        row = {"video": self._descriptor("shared", b"video", 0)}
        first, second = collator(), collator()
        try:
            original = first([row])[0]["frame"]
            first.close()
            self.assertIs(second([row])[0]["frame"], original)
            self.assertEqual(1, len(builds))
        finally:
            first.close()
            second.close()

    @unittest.skipUnless("fork" in multiprocessing.get_all_start_methods(), "requires fork")
    def test_metadata_cache_rejects_parent_load_in_new_child_collator(self):
        context = multiprocessing.get_context("fork")
        row = {"video": self._descriptor("fork", b"video", 0)}
        for hold_lock in (False, True):
            with self.subTest(hold_lock=hold_lock):
                cache = VideoMetadataCache()
                started, release = threading.Event(), threading.Event()

                def load():
                    if hold_lock:
                        cache._lock.acquire()
                    try:
                        started.set()
                        if not release.wait(15):
                            raise TimeoutError("parent loader not released")
                        return _CachedIndex()
                    finally:
                        if hold_lock:
                            cache._lock.release()

                with ThreadPoolExecutor(1) as executor:
                    pending = executor.submit(cache.get_or_load, "fork", load)
                    self.assertTrue(started.wait(5))
                    receive, send = context.Pipe(duplex=False)
                    child = context.Process(
                        target=_read_with_inherited_metadata,
                        args=(cache, self.table, row, send))
                    child.start()
                    send.close()
                    try:
                        self.assertTrue(receive.poll(10), "child waited on parent's cache")
                        self.assertEqual(receive.recv(), "rejected")
                        child.join(5)
                        self.assertEqual(child.exitcode, 0)
                    finally:
                        if child.is_alive():
                            child.terminate()
                        child.join(5)
                        receive.close()
                        release.set()
                    original = pending.result(timeout=5)
                    self.assertIs(cache.get("fork"), original)

    @unittest.skipUnless("fork" in multiprocessing.get_all_start_methods(), "requires fork")
    def test_metadata_cache_child_reads_require_fresh_cache(self):
        cache = VideoMetadataCache()
        original = _CachedIndex()
        cache["parent"] = original
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        child = context.Process(target=_read_inherited_cache_concurrently, args=(cache, send))
        child.start()
        send.close()
        try:
            self.assertTrue(receive.poll(15), "child readers hung")
            self.assertEqual(receive.recv(), "ok")
            child.join(5)
            self.assertEqual(child.exitcode, 0)
        finally:
            if child.is_alive():
                child.terminate()
            child.join(5)
            receive.close()
        self.assertIs(cache["parent"], original)
        self.assertIsNone(cache.get("child"))

    @unittest.skipUnless("fork" in multiprocessing.get_all_start_methods(), "requires fork")
    def test_metadata_cache_fork_does_not_finalize_inherited_values(self):
        cache = VideoMetadataCache()
        lock = threading.Lock()
        cache["parent"] = _LockingFinalizerIndex(lock)
        held, release = threading.Event(), threading.Event()

        def hold_lock():
            with lock:
                held.set()
                release.wait(20)

        thread = threading.Thread(target=hold_lock)
        thread.start()
        self.assertTrue(held.wait(5))
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        child = context.Process(target=_read_inherited_cache_concurrently, args=(cache, send))
        try:
            child.start()
            send.close()
            self.assertTrue(receive.poll(10), "fork ran a finalizer using an inherited lock")
            self.assertEqual(receive.recv(), "ok")
            child.join(5)
            self.assertEqual(child.exitcode, 0)
        finally:
            if child.is_alive():
                child.terminate()
            child.join(5)
            receive.close()
            send.close()
            release.set()
            thread.join(5)
        cache.clear()

    @unittest.skipUnless("fork" in multiprocessing.get_all_start_methods(), "requires fork")
    def test_inherited_collator_keeps_sole_shared_cache_reference(self):
        import weakref

        lock = threading.Lock()
        cache = VideoMetadataCache()
        cache["parent"] = _LockingFinalizerIndex(lock)
        reference = weakref.ref(cache)
        collator = VideoFrameCollator(
            self.table, video_column="video", collate_fn=list,
            decoder_factory=lambda stream: None,
            decode_fn=lambda decoder, index, row: None, metadata_cache=cache)
        del cache  # Only the collator retains the cache and its index.
        held, release = threading.Event(), threading.Event()

        def hold_lock():
            with lock:
                held.set()
                release.wait(20)

        thread = threading.Thread(target=hold_lock)
        thread.start()
        context = multiprocessing.get_context("fork")
        receive, send = context.Pipe(duplex=False)
        child = context.Process(target=_call_inherited_collator, args=(collator, send))
        try:
            self.assertTrue(held.wait(5))
            child.start()
            send.close()
            self.assertTrue(receive.poll(5), "child did not return from fork")
            self.assertEqual(receive.recv(), "started")
            self.assertTrue(receive.poll(5), "collator finalized the inherited cache")
            self.assertEqual(receive.recv(), "rejected")
            child.join(5)
            self.assertEqual(child.exitcode, 0)
        finally:
            if child.is_alive():
                child.terminate()
            if child.pid is not None:
                child.join(5)
            receive.close()
            send.close()
            release.set()
            thread.join(5)
            collator.close()
        self.assertIs(reference(), collator._video_metadata_cache)
        self.assertEqual(collator([]), [])
        collator._video_metadata_cache.clear()

    def test_metadata_cache_rejects_all_inherited_operations(self):
        cache = VideoMetadataCache()
        value = _CachedIndex()
        cache["parent"] = value
        cache._owner_pid = -1
        operations = [lambda: cache.get("parent"), lambda: cache["parent"],
                      lambda: cache.__setitem__("child", value),
                      lambda: cache.__delitem__("parent"), lambda: len(cache),
                      lambda: cache.get_or_load("child", _CachedIndex), cache.clear]
        for operation in operations:
            with self.assertRaisesRegex(RuntimeError, "new VideoMetadataCache"):
                operation()

    def test_metadata_cache_delete_releases_retention(self):
        import gc
        import weakref

        cache = VideoMetadataCache()
        value = _CachedIndex()
        reference = weakref.ref(value)
        cache["index"] = value
        self.assertIs(cache["index"], value)
        del value
        del cache["index"]
        gc.collect()
        self.assertIsNone(reference())
        self.assertEqual(cache._bytes, 0)
        self.assertEqual(len(cache), 0)
        with self.assertRaises(KeyError):
            cache["index"]
        with self.assertRaises(KeyError):
            del cache["index"]

    def test_metadata_cache_retries_failed_load(self):
        cache = VideoMetadataCache()

        class Index:
            nbytes = 16

        errors = []

        def fail():
            errors.append(1)
            raise ValueError("bad MP4")

        with self.assertRaisesRegex(ValueError, "bad MP4"):
            cache.get_or_load("broken", fail)
        self.assertEqual(1, len(errors))
        recovered = cache.get_or_load("broken", Index)
        self.assertIs(cache.get("broken"), recovered)

    def test_parallel_direct_calls_preserve_order_and_bound_cache(self):
        import threading

        barrier = threading.Barrier(2)
        calls = {}
        decoders = []

        def factory(stream):
            decoder = _Decoder(stream, [])
            decoders.append(decoder)
            return decoder

        def decode_batch(decoder, indices, rows):
            barrier.wait(timeout=5)
            self.assertFalse(decoder.closed)
            calls[rows[0]["video_id"]] = list(indices)
            return [decoder.decode(index) for index in indices]

        rows = [{"video_id": name, "video": self._descriptor(name, name.encode(), index)}
                for name, index in (("a", 2), ("b", 1), ("a", 0), ("c", 3), ("d", 0))]
        rows.insert(1, {"video_id": "null", "video": None})
        collator = VideoFrameCollator(
            self.table, video_column="video", decoder_factory=factory,
            decode_batch_fn=decode_batch, collate_fn=list,
            max_open_videos=2, decode_parallelism=4)
        try:
            result = collator(rows)
            self.assertEqual([row["video_id"] for row in rows],
                             [row["video_id"] for row in result])
            self.assertEqual([(b"a", 2), None, (b"b", 1), (b"a", 0), (b"c", 3), (b"d", 0)],
                             [row["frame"] for row in result])
            self.assertEqual({"a": [0, 2], "b": [1], "c": [3], "d": [0]}, calls)
            self.assertEqual(2, sum(not decoder.closed for decoder in decoders))
            executor = collator._decode_pool._executor
        finally:
            collator.close()
        self.assertTrue(executor._shutdown)
        self.assertTrue(all(decoder.closed for decoder in decoders))

    def test_parallel_direct_callbacks_preserve_context(self):
        try:
            from contextvars import ContextVar
        except ImportError:
            self.skipTest("contextvars requires Python 3.7+")
        import threading

        marker = ContextVar("decode_marker", default="default")
        token = marker.set("caller")
        rows = [{"video": self._descriptor(str(i), b"video", i)} for i in range(2)]
        try:
            for batch in (False, True):
                barrier = threading.Barrier(2)

                def decode(decoder, index, row):
                    barrier.wait(timeout=5)
                    return marker.get()

                def decode_batch(decoder, indices, group):
                    barrier.wait(timeout=5)
                    return [marker.get()] * len(indices)

                callback = ({"decode_batch_fn": decode_batch} if batch
                            else {"decode_fn": decode})
                collator = VideoFrameCollator(
                    self.table, video_column="video", collate_fn=list,
                    decoder_factory=lambda stream: _Decoder(stream, []),
                    decode_parallelism=2, **callback)
                try:
                    self.assertEqual(["caller", "caller"],
                                     [row["frame"] for row in collator(rows)])
                finally:
                    collator.close()
        finally:
            marker.reset(token)

    def test_parallel_direct_callbacks_preserve_torch_mode(self):
        try:
            import torch
        except ImportError as error:
            self.skipTest(str(error))

        rows = [{"video": self._descriptor(str(i), b"video", i)} for i in range(2)]
        collator = VideoFrameCollator(
            self.table, video_column="video", collate_fn=list,
            decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=lambda decoder, index, row: (
                torch.is_grad_enabled(), torch.is_inference_mode_enabled()),
            decode_parallelism=2)
        try:
            for mode, expected in ((torch.no_grad(), (False, False)),
                                   (torch.inference_mode(), (False, True))):
                with mode:
                    self.assertEqual([expected, expected],
                                     [row["frame"] for row in collator(rows)])
        finally:
            collator.close()

    def test_parallel_direct_callbacks_accept_legacy_torch_device(self):
        try:
            import torch
        except ImportError as error:
            self.skipTest(str(error))
        from unittest.mock import patch

        rows = [{"video": self._descriptor(str(i), b"video", i)} for i in range(2)]
        collator = VideoFrameCollator(
            self.table, video_column="video", collate_fn=list,
            decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=lambda decoder, index, row: index,
            decode_parallelism=2)
        try:
            # Torch 1.13 has torch.device(...) but no __enter__/__exit__.
            with patch.object(torch, "device", side_effect=lambda device: object()):
                self.assertEqual([0], [row["frame"] for row in collator(rows[:1])])
                self.assertEqual([0, 1], [row["frame"] for row in collator(rows)])
        finally:
            collator.close()

    def test_parallel_direct_callbacks_preserve_torch_device_and_autocast(self):
        try:
            import torch
        except ImportError as error:
            self.skipTest(str(error))

        rows = [{"video": self._descriptor(str(i), b"video", i)} for i in range(2)]
        collator = VideoFrameCollator(
            self.table, video_column="video", collate_fn=list,
            decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=lambda decoder, index, row: torch.empty(1).device.type,
            decode_parallelism=2)
        try:
            with torch.device("meta"):
                inline = collator(rows[:1])[0]["frame"]
                parallel = [row["frame"] for row in collator(rows)]
            self.assertEqual([inline, inline], parallel)
            self.assertEqual("meta", inline)

            collator.decode_fn = lambda decoder, index, row: torch.mm(
                torch.ones((2, 2)), torch.ones((2, 2))).dtype
            with torch.device("cpu"), torch.autocast("cpu", dtype=torch.bfloat16):
                inline = collator(rows[:1])[0]["frame"]
                parallel = [row["frame"] for row in collator(rows)]
            self.assertEqual([inline, inline], parallel)
            self.assertEqual(torch.bfloat16, inline)
        finally:
            collator.close()

    def test_direct_pool_survives_variable_video_counts(self):
        rows = [{"video": self._descriptor(str(i), b"video", i)} for i in range(4)]
        collator = VideoFrameCollator(
            self.table, video_column="video", collate_fn=list,
            decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=lambda decoder, index, row: decoder.decode(index),
            max_open_videos=4, decode_parallelism=4)
        try:
            collator(rows)
            executor = collator._decode_pool._executor
            self.assertIsNotNone(executor)
            collator(rows[:1])
            self.assertIs(executor, collator._decode_pool._executor)
            self.assertFalse(executor._shutdown)
            collator(rows)
            self.assertIs(executor, collator._decode_pool._executor)
        finally:
            collator.close()

    def test_parallel_direct_callback_failure_drains_before_reuse(self):
        import threading

        barrier = threading.Barrier(2)
        finished = threading.Event()

        def decode(decoder, index, row):
            barrier.wait(timeout=5)
            if index == 0:
                raise ValueError("decode failed")
            self.assertFalse(decoder.closed)
            finished.set()
            return decoder.decode(index)

        rows = [{"video": self._descriptor(str(index), b"video", index)}
                for index in (0, 1)]
        collator = VideoFrameCollator(
            self.table, video_column="video",
            decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=decode, collate_fn=list, decode_parallelism=2)
        try:
            with self.assertRaisesRegex(ValueError, "decode failed"):
                collator(rows)
            self.assertTrue(finished.is_set())
            collator.decode_fn = lambda decoder, index, row: decoder.decode(index)
            self.assertEqual([(b"video", 0), (b"video", 1)],
                             [row["frame"] for row in collator(rows)])
        finally:
            collator.close()

    def test_direct_parallelism_validation_and_serial_default(self):
        import threading

        options = dict(table=self.table, video_column="video",
                       decoder_factory=lambda stream: _Decoder(stream, []),
                       decode_fn=lambda decoder, index, row: threading.get_ident(),
                       collate_fn=list)
        for value in (0, -1, True, 1.5, None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "decode_parallelism"):
                VideoFrameCollator(**options, decode_parallelism=value)
        collator = VideoFrameCollator(**options)
        try:
            self.assertEqual([], collator([]))
            row = {"video": self._descriptor("serial", b"video", 0)}
            self.assertEqual(threading.get_ident(), collator(row)["frame"])
            self.assertIsNone(collator._decode_pool._executor)
        finally:
            collator.close()

    def test_accepts_batch_callback_without_single_frame_callback(self):
        descriptors = [
            self._descriptor("episode.mp4", b"video", index)
            for index in (2, 0)
        ]
        for kwargs in ({}, {"decode_fn": None}):
            with self.subTest(kwargs=kwargs):
                collator = VideoFrameCollator(
                    self.table,
                    video_column="video",
                    decoder_factory=lambda stream: _Decoder(stream, []),
                    decode_batch_fn=lambda decoder, indices, rows: [
                        decoder.decode(index) for index in indices
                    ],
                    collate_fn=lambda rows: rows,
                    **kwargs,
                )
                try:
                    result = collator([{"video": value} for value in descriptors])
                    self.assertEqual([(b"video", 2), (b"video", 0)],
                                     [row["frame"] for row in result])
                finally:
                    collator.close()

    def test_rejects_missing_or_invalid_decode_callbacks(self):
        callback = lambda *args: None
        cases = [
            ({}, "At least one"),
            ({"decode_fn": None, "decode_batch_fn": None}, "At least one"),
            ({"decode_fn": False}, "decode_fn must be callable"),
            ({"decode_fn": False, "decode_batch_fn": callback},
             "decode_fn must be callable"),
            ({"decode_batch_fn": False}, "decode_batch_fn must be callable"),
            ({"decode_fn": callback, "decode_batch_fn": False},
             "decode_batch_fn must be callable"),
        ]
        for kwargs, message in cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    VideoFrameCollator(
                        self.table,
                        video_column="video",
                        decoder_factory=lambda stream: _Decoder(stream, []),
                        **kwargs,
                    )

    def test_reuses_decoder_for_rows_with_same_descriptor(self):
        descriptors = [
            self._descriptor("episode-1.mp4", b"video-one", frame)
            for frame in (0, 1)
        ]
        factory_calls = []

        def factory(stream):
            factory_calls.append("open")
            decoder = _Decoder(stream, factory_calls)
            decoder.get_frames_at = lambda indices: self.fail(
                "must preserve the custom single-frame callback")
            return decoder

        collator = VideoFrameCollator(
            self.table,
            video_column="video",
            decoder_factory=factory,
            decode_fn=lambda decoder, frame, row: decoder.decode(frame),
            collate_fn=lambda rows: rows,
        )
        try:
            result = collator([
                {"episode_id": 1, "video": descriptors[0]},
                {"episode_id": 1, "video": descriptors[1]},
            ])
        finally:
            collator.close()

        self.assertEqual(["open", "close"], factory_calls)
        self.assertEqual(
            [(b"video-one", 0), (b"video-one", 1)],
            [row["frame"] for row in result],
        )
        self.assertEqual(descriptors[0], result[0]["video"])

    def test_groups_and_sorts_frames_while_restoring_row_order(self):
        descriptors = {
            (video, frame): self._descriptor(
                "episode-%s.mp4" % video,
                ("video-%s" % video).encode(),
                frame,
            )
            for video in ("one", "two")
            for frame in (0, 1, 2, 3)
        }
        rows = [
            {"request": "a", "video": descriptors["one", 3]},
            {"request": "b", "video": descriptors["two", 2]},
            {"request": "c", "video": descriptors["one", 1]},
            {"request": "d", "video": descriptors["two", 0]},
        ]
        calls = []

        def decode(decoder, frame, row):
            video, decoded_frame = decoder.decode(frame)
            calls.append((video, decoded_frame))
            return row["request"], video, decoded_frame

        collator = VideoFrameCollator(
            self.table,
            video_column="video",
            decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=decode,
            collate_fn=lambda decoded_rows: decoded_rows,
        )
        try:
            result = collator(rows)
        finally:
            collator.close()

        self.assertEqual(
            [
                (b"video-one", 1),
                (b"video-one", 3),
                (b"video-two", 0),
                (b"video-two", 2),
            ],
            calls,
        )
        self.assertEqual(["a", "b", "c", "d"], [
            row["request"] for row in result
        ])
        self.assertEqual(
            [3, 2, 1, 0],
            [row["frame"][2] for row in result],
        )

    def test_decodes_dataset_row_groups_in_one_batch(self):
        row_groups = [
            {4: {"request": "base", "video": b"base"}},
            {1: {"request": "delta", "video": b"delta"}},
        ]

        class Collator:
            video_column = "video"
            output_column = "video"

            def __init__(self):
                self.calls = []

            def __call__(self, rows):
                self.calls.append([row["request"] for row in rows])
                return [
                    dict(row, video="decoded-" + row["request"])
                    for row in rows
                ]

        collator = Collator()
        _decode_video_rows(row_groups, [collator])

        self.assertEqual([["base", "delta"]], collator.calls)
        self.assertEqual("decoded-base", row_groups[0][4]["video"])
        self.assertEqual("decoded-delta", row_groups[1][1]["video"])

    def test_batch_decode_groups_payload_ranges_and_restores_order(self):
        path = os.path.join(self.temp_dir.name, "shared.video")
        with open(path, "wb") as output:
            output.write(b"firstsecond")
        calls = []
        created = []

        def factory(stream):
            decoder = _Decoder(stream, [])
            created.append(decoder)
            return decoder

        def decode_batch(decoder, indices, rows):
            calls.append((decoder.decode(0)[0], indices))
            return [
                (row["request"], decoder.decode(index))
                for index, row in zip(indices, rows)
            ]

        def row(request, offset, length, index):
            return {"request": request, "video": VideoFrameDescriptor(
                path, offset, length, index).serialize()}

        collator = VideoFrameCollator(
            self.table,
            video_column="video",
            decoder_factory=factory,
            decode_fn=lambda *args: self.fail("unexpected single decode"),
            decode_batch_fn=decode_batch,
            max_open_videos=2,
            collate_fn=lambda rows: rows,
        )
        rows = [
            row("a", 0, 5, 3), row("b", 5, 6, 2),
            row("c", 0, 5, 1), row("d", 0, 5, 3),
            {"request": "e", "video": None}, row("f", 0, 11, 0),
        ]
        try:
            result = collator(rows)
            self.assertEqual(
                [(b"first", [1, 3, 3]), (b"second", [2]),
                 (b"firstsecond", [0])], calls)
            self.assertEqual([r["request"] for r in rows],
                             [r["request"] for r in result])
            self.assertEqual(
                [("a", (b"first", 3)), ("b", (b"second", 2)),
                 ("c", (b"first", 1)), ("d", (b"first", 3)),
                 None, ("f", (b"firstsecond", 0))],
                [r["frame"] for r in result])
            self.assertTrue(all("frame" not in r for r in rows))
            self.assertEqual(3, len(created))
            self.assertTrue(created[0].closed)
            self.assertEqual(2, len(collator._decoders))
            self.assertEqual(
                ("g", (b"second", 1)),
                collator(row("g", 5, 6, 1))["frame"])
            self.assertEqual(3, len(created))
            self.assertEqual([], collator([]))
            self.assertEqual(4, len(calls))
        finally:
            collator.close()
        self.assertTrue(all(decoder.closed for decoder in created))

    def test_batch_decode_rejects_wrong_result_count(self):
        descriptor = self._descriptor("episode.mp4", b"video", 0)
        for count in (0, 2):
            with self.subTest(count=count):
                collator = VideoFrameCollator(
                    self.table,
                    video_column="video",
                    decoder_factory=lambda stream: _Decoder(stream, []),
                    decode_fn=lambda *args: self.fail("unexpected fallback"),
                    decode_batch_fn=lambda *args: [None] * count,
                    collate_fn=lambda rows: rows,
                )
                try:
                    with self.assertRaisesRegex(ValueError, "one frame per row"):
                        collator([{"video": descriptor}])
                finally:
                    collator.close()

    def test_evicts_least_recently_used_decoder(self):
        descriptors = [
            self._descriptor("episode-%d.mp4" % index, bytes([index]), index)
            for index in range(3)
        ]
        factory_calls = []

        def factory(stream):
            factory_calls.append("open")
            return _Decoder(stream, factory_calls)

        collator = VideoFrameCollator(
            self.table,
            video_column="video",
            decoder_factory=factory,
            decode_fn=lambda decoder, frame, row: decoder.decode(frame),
            max_open_videos=2,
            collate_fn=lambda rows: rows,
        )
        try:
            collator([
                {"episode_id": index, "video": descriptor}
                for index, descriptor in enumerate(descriptors)
            ])
            self.assertEqual(3, factory_calls.count("open"))
            self.assertEqual(1, factory_calls.count("close"))

            collator([{"episode_id": 3, "video": descriptors[0]}])
            self.assertEqual(4, factory_calls.count("open"))
            self.assertEqual(2, factory_calls.count("close"))
        finally:
            collator.close()

        self.assertEqual(4, factory_calls.count("close"))

    def test_rejects_non_descriptor_video_cell(self):
        collator = VideoFrameCollator(
            self.table,
            video_column="video",
            decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=lambda decoder, frame, row: decoder.decode(frame),
            collate_fn=lambda rows: rows,
        )
        valid = self._descriptor("valid.mp4", b"video", 0)
        for value in (b"inline-mp4", valid + b"trailing"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "VideoFrameDescriptor"):
                    collator([{"episode_id": 0, "video": value}])

    def test_close_releases_all_resources_after_decoder_failure(self):
        descriptors = [
            self._descriptor("episode-%d.mp4" % index, bytes([index]), index)
            for index in range(2)
        ]
        calls = []
        created = []

        def factory(stream):
            decoder = (
                _FailingCloseDecoder(stream, calls)
                if not created
                else _Decoder(stream, calls)
            )
            created.append(decoder)
            return decoder

        collator = VideoFrameCollator(
            self.table,
            video_column="video",
            decoder_factory=factory,
            decode_fn=lambda decoder, frame, row: decoder.decode(frame),
            collate_fn=lambda rows: rows,
        )
        collator([
            {"episode_id": index, "video": descriptor}
            for index, descriptor in enumerate(descriptors)
        ])

        with self.assertRaisesRegex(RuntimeError, "decoder close failed"):
            collator.close()

        self.assertEqual(2, calls.count("close"))
        self.assertTrue(all(decoder.closed for decoder in created))
        self.assertEqual(0, len(collator._decoders))

    def test_reuses_resolved_table_file_io(self):
        class ResolvedFileIO:

            @property
            def uri_reader_factory(self):
                raise AssertionError("must not rebuild a URI reader")

            def new_input_stream(self, path):
                self.path = path
                return io.BytesIO(b"resolved-video")

        file_io = ResolvedFileIO()
        table = SimpleNamespace(raw_table=SimpleNamespace(file_io=file_io))
        descriptor = VideoFrameDescriptor(
            "oss://bucket/internal.video", 0, 14, 2
        ).serialize()
        collator = VideoFrameCollator(
            table,
            video_column="video",
            decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=lambda decoder, frame, row: decoder.decode(frame),
            collate_fn=lambda rows: rows,
        )
        try:
            result = collator([{"episode_id": 1, "video": descriptor}])
        finally:
            collator.close()

        self.assertEqual("oss://bucket/internal.video", file_io.path)
        self.assertEqual((b"resolved-video", 2), result[0]["frame"])

    @parameterized.expand([0, 5])
    def test_unused_index_is_not_read_on_decode_eviction_or_close(self, prefetch_max_bytes):
        streams = []

        class PayloadOnlyIO:
            def new_input_stream(self, path):
                stream = io.BytesIO(b"video")
                streams.append(stream)
                return stream

            def read_file_range(self, *args):
                raise AssertionError("unused index must not be read")

        collator = VideoFrameCollator(
            SimpleNamespace(file_io=PayloadOnlyIO()), video_column="video",
            decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=lambda decoder, index, row: decoder.decode(index),
            max_open_videos=1, collate_fn=lambda rows: rows,
            prefetch_max_bytes=prefetch_max_bytes)
        try:
            for name in ("first", "second"):
                descriptor = VideoFrameDescriptor(name, 0, 5, 2)
                result = collator([{"video": descriptor.serialize()}])
                self.assertEqual((b"video", 2), result[0]["frame"])
            self.assertTrue(streams[0].closed)
        finally:
            collator.close()
        self.assertTrue(all(stream.closed for stream in streams))

    def test_prefetch_bounds_retained_payloads_and_reuses_reads(self):
        reads = []
        opened = []

        class Source(io.BytesIO):
            def read(self, size=-1):
                reads.append(size)
                return super().read(size)

        class CountingIO:
            def new_input_stream(self, path):
                stream = Source(b"video")
                opened.append(stream)
                return stream

        decoders = []

        def factory(stream):
            decoder = _Decoder(stream, [])
            decoders.append(decoder)
            return decoder

        collator = VideoFrameCollator(
            SimpleNamespace(file_io=CountingIO()), video_column="video",
            decoder_factory=factory,
            decode_fn=lambda decoder, index, row: decoder.decode(index),
            collate_fn=lambda rows: rows, prefetch_max_bytes=5)
        first = VideoFrameDescriptor("first", 0, 5, 0, 0, 0).serialize()
        second = VideoFrameDescriptor("second", 0, 5, 0, 0, 0).serialize()
        try:
            for _ in range(2):
                result = collator([{"video": first}])
                self.assertEqual((b"video", 0), result[0]["frame"])
            self.assertEqual([5], reads)
            self.assertFalse(opened[0].closed)
            self.assertEqual([b"id", b"o"],
                             decoders[0]._stream.video_read_ranges([(1, 2), (4, 1)]))
            self.assertEqual([5], reads)
            collator([{"video": second}])
            self.assertEqual(0, getattr(decoders[1]._stream, "video_prefetch_bytes", 0))
            self.assertFalse(opened[1].closed)
            self.assertEqual(5, sum(getattr(stream, "video_prefetch_bytes", 0)
                                    for _, stream in collator._decoders.values()))
            collator.close()
            collator([{"video": second}])
            self.assertEqual(5, decoders[-1]._stream.video_prefetch_bytes)
        finally:
            collator.close()
        self.assertTrue(all(stream.closed for stream in opened))

    def test_disabled_or_oversized_prefetch_keeps_range_reads(self):
        for budget in (0, 4):
            with self.subTest(budget=budget):
                source = io.BytesIO(b"video")
                captured = []

                def factory(stream):
                    self.assertEqual(0, source.tell())
                    captured.append(stream)
                    return _Decoder(stream, [])

                collator = VideoFrameCollator(
                    SimpleNamespace(file_io=SimpleNamespace(
                        new_input_stream=lambda path: source)),
                    video_column="video", decoder_factory=factory,
                    decode_fn=lambda decoder, index, row: decoder.decode(index),
                    collate_fn=lambda rows: rows, prefetch_max_bytes=budget)
                try:
                    descriptor = VideoFrameDescriptor("video", 0, 5, 0, 0, 0)
                    result = collator([{"video": descriptor.serialize()}])
                    self.assertEqual((b"video", 0), result[0]["frame"])
                    self.assertEqual(0, getattr(captured[0], "video_prefetch_bytes", 0))
                finally:
                    collator.close()

    def test_prefetch_rejects_truncated_payload_and_closes_source(self):
        source = io.BytesIO(b"short")
        collator = VideoFrameCollator(
            SimpleNamespace(file_io=SimpleNamespace(new_input_stream=lambda path: source)),
            video_column="video", decoder_factory=lambda stream: self.fail("truncated"),
            decode_fn=lambda *args: None, prefetch_max_bytes=10)
        descriptor = VideoFrameDescriptor("video", 0, 10, 0, 0, 0)
        with self.assertRaisesRegex(IOError, "Incomplete video"):
            collator([{"video": descriptor.serialize()}])
        self.assertTrue(source.closed)
        self.assertFalse(collator._decoders)
        collator.close()

    def test_prefetch_accepts_short_reads(self):
        reads = []

        class Source(io.BytesIO):
            def read(self, size=-1):
                reads.append(size)
                return super().read(min(size, 2))

        source = Source(b"video")
        collator = VideoFrameCollator(
            SimpleNamespace(file_io=SimpleNamespace(new_input_stream=lambda path: source)),
            video_column="video", decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=lambda decoder, index, row: decoder.decode(index),
            collate_fn=lambda rows: rows, prefetch_max_bytes=5)
        try:
            descriptor = VideoFrameDescriptor("video", 0, 5, 0, 0, 0)
            result = collator([{"video": descriptor.serialize()}])
            self.assertEqual((b"video", 0), result[0]["frame"])
            self.assertEqual([5, 3, 1], reads)
        finally:
            collator.close()
        self.assertTrue(source.closed)

    def test_prefetch_reuses_evicted_decoder_budget(self):
        opened = []
        buffered = []

        def open_source(path):
            source = io.BytesIO(b"video")
            opened.append(source)
            return source

        def factory(stream):
            buffered.append(getattr(stream, "video_prefetch_bytes", 0))
            self.assertTrue(all(source.closed for source in opened[:-1]))
            return _Decoder(stream, [])

        collator = VideoFrameCollator(
            SimpleNamespace(file_io=SimpleNamespace(new_input_stream=open_source)),
            video_column="video", decoder_factory=factory,
            decode_fn=lambda decoder, index, row: decoder.decode(index),
            collate_fn=lambda rows: rows, prefetch_max_bytes=5, max_open_videos=1)
        try:
            for name in ("first", "second", "third"):
                descriptor = VideoFrameDescriptor(name, 0, 5, 0, 0, 0)
                result = collator([{"video": descriptor.serialize()}])
                self.assertEqual((b"video", 0), result[0]["frame"])
                self.assertEqual(1, len(collator._decoders))
            self.assertEqual([5, 5, 5], buffered)
        finally:
            collator.close()
        self.assertTrue(all(source.closed for source in opened))

    def test_invalid_prefetch_budget(self):
        for budget in (-1, True, 1.5, "10"):
            with self.subTest(budget=budget):
                with self.assertRaisesRegex(ValueError, "prefetch_max_bytes"):
                    VideoFrameCollator(
                        self.table, video_column="video", decoder_factory=lambda stream: None,
                        decode_fn=lambda *args: None, prefetch_max_bytes=budget)

    def test_prefetch_preserves_real_indexed_video_frames(self):
        try:
            import av
            import numpy as np
            import torch
        except ImportError as error:
            self.skipTest(str(error))
        from pypaimon.multimodal.lerobot.dataset import _open_video_decoder

        target = io.BytesIO()
        with av.open(target, mode="w", format="mp4") as container:
            stream = container.add_stream("mpeg4", rate=30)
            stream.width = stream.height = 16
            stream.pix_fmt = "yuv420p"
            stream.codec_context.gop_size = 4
            for value in range(12):
                pixels = np.full((16, 16, 3), value * 15, dtype=np.uint8)
                for packet in stream.encode(av.VideoFrame.from_ndarray(pixels, format="rgb24")):
                    container.mux(packet)
            for packet in stream.encode():
                container.mux(packet)
        payload = target.getvalue()
        from pypaimon.common.mp4_moov import MoovLocator
        locator = MoovLocator()
        locator.feed(payload)
        moov_offset, moov_length = locator.finish()
        full_file = payload
        outputs = []
        for budget in (0, len(payload)):
            reads = []

            class Source(io.BytesIO):
                def read(self, size=-1):
                    reads.append((self.tell(), size))
                    return super().read(size)

            class CountingIO:
                def new_input_stream(self, path):
                    return Source(full_file)

                def read_file_range(self, path, offset, size):
                    reads.append((offset, size))
                    return full_file[offset:offset + size]

                def read_ranges_coalesced(self, ranges, parallelism):
                    return [self.read_file_range(*entry) for entry in ranges]

            collator = VideoFrameCollator(
                SimpleNamespace(file_io=CountingIO()), video_column="video",
                decoder_factory=lambda source: _open_video_decoder(source, backend="pyav"),
                decode_batch_fn=lambda decoder, indices, rows: list(
                    decoder.get_frames_at(indices=indices).data),
                collate_fn=lambda rows: rows, prefetch_max_bytes=budget)
            try:
                rows = [{"video": VideoFrameDescriptor(
                    "video", 0, len(payload), frame, moov_offset, moov_length
                ).serialize()} for frame in (9, 1, 9, 7)]
                outputs.append(torch.stack([row["frame"] for row in collator(rows)]))
                if budget:
                    media_reads = [entry for entry in reads if entry[0] < len(payload)]
                    self.assertEqual([(0, len(payload))], media_reads)
                    count = len(reads)
                    repeated = torch.stack([row["frame"] for row in collator(rows)])
                    self.assertTrue(torch.equal(outputs[-1], repeated))
                    self.assertEqual(count, len(reads))
            finally:
                collator.close()
        self.assertTrue(torch.equal(outputs[0], outputs[1]))

    def _descriptor(self, name, data, frame_index):
        path = os.path.join(self.temp_dir.name, name)
        with open(path, "wb") as output:
            output.write(data)
        return VideoFrameDescriptor(
            path, 0, len(data), frame_index
        ).serialize()


if __name__ == "__main__":
    unittest.main()
