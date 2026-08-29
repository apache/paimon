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
from pypaimon.table.row.blob import BlobDescriptor


class _Decoder:

    def __init__(self, stream, calls):
        self._stream = stream
        self._calls = calls
        self.closed = False

    def decode(self, row):
        self._stream.seek(0)
        return self._stream.read(), row["frame_index"]

    def close(self):
        self.closed = True
        self._calls.append("close")


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
        descriptor = self._descriptor("episode-1.mp4", b"video-one")
        factory_calls = []

        def factory(stream):
            factory_calls.append("open")
            return _Decoder(stream, factory_calls)

        collator = VideoFrameCollator(
            self.table,
            video_column="video",
            decoder_factory=factory,
            decode_fn=lambda decoder, row: decoder.decode(row),
            collate_fn=lambda rows: rows,
        )
        try:
            result = collator([
                {"frame_index": 0, "video": descriptor},
                {"frame_index": 1, "video": descriptor},
            ])
        finally:
            collator.close()

        self.assertEqual(["open", "close"], factory_calls)
        self.assertEqual(
            [(b"video-one", 0), (b"video-one", 1)],
            [row["frame"] for row in result],
        )
        self.assertEqual(descriptor, result[0]["video"])

    def test_evicts_least_recently_used_decoder(self):
        descriptors = [
            self._descriptor("episode-%d.mp4" % index, bytes([index]))
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
            decode_fn=lambda decoder, row: decoder.decode(row),
            max_open_videos=2,
            collate_fn=lambda rows: rows,
        )
        try:
            collator([
                {"frame_index": index, "video": descriptor}
                for index, descriptor in enumerate(descriptors)
            ])
            self.assertEqual(3, factory_calls.count("open"))
            self.assertEqual(1, factory_calls.count("close"))

            collator([{"frame_index": 3, "video": descriptors[0]}])
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
            decode_fn=lambda decoder, row: decoder.decode(row),
            collate_fn=lambda rows: rows,
        )
        valid = self._descriptor("valid.mp4", b"video")
        for value in (b"inline-mp4", valid + b"trailing"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "serialized BlobDescriptor"):
                    collator([{"frame_index": 0, "video": value}])

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
        descriptor = BlobDescriptor(
            "oss://bucket/internal.shared-blob", 0, 14
        ).serialize()
        collator = VideoFrameCollator(
            table,
            video_column="video",
            decoder_factory=lambda stream: _Decoder(stream, []),
            decode_fn=lambda decoder, row: decoder.decode(row),
            collate_fn=lambda rows: rows,
        )
        try:
            result = collator([{"frame_index": 2, "video": descriptor}])
        finally:
            collator.close()

        self.assertEqual("oss://bucket/internal.shared-blob", file_io.path)
        self.assertEqual((b"resolved-video", 2), result[0]["frame"])

    def _descriptor(self, name, data):
        path = os.path.join(self.temp_dir.name, name)
        with open(path, "wb") as output:
            output.write(data)
        return BlobDescriptor(path, 0, len(data)).serialize()


if __name__ == "__main__":
    unittest.main()
