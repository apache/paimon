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


class VideoFrameCollatorTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.file_io = FileIO.get("file://" + self.temp_dir.name, {})
        self.table = SimpleNamespace(
            raw_table=SimpleNamespace(file_io=self.file_io)
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_reuses_decoder_for_rows_with_same_descriptor(self):
        descriptors = [
            self._descriptor("episode-1.mp4", b"video-one", frame)
            for frame in (0, 1)
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

    def _descriptor(self, name, data, frame_index):
        path = os.path.join(self.temp_dir.name, name)
        with open(path, "wb") as output:
            output.write(data)
        return VideoFrameDescriptor(
            path, 0, len(data), frame_index
        ).serialize()


if __name__ == "__main__":
    unittest.main()
