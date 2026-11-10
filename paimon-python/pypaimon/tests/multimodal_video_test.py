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
import os
import tempfile
import unittest
from types import SimpleNamespace

from parameterized import parameterized

from pypaimon.common.file_io import FileIO
from pypaimon.multimodal import VideoFrameCollator
from pypaimon.multimodal.lerobot.dataset import _decode_video_rows
from pypaimon.table.row.blob import VideoFrameDescriptor


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
