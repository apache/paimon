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

import pyarrow as pa
import pytest

from pypaimon.common import uri_reader
from pypaimon.common.uri_reader import HttpRangeInputStream
from pypaimon.function import builtins
from pypaimon.table.row.blob import BlobDescriptor


class _Response:

    def __init__(self, status_code, content=b"", headers=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}


def _write_sample_video(video_path):
    av = pytest.importorskip("av")
    image_module = pytest.importorskip("PIL.Image")

    container = av.open(str(video_path), "w")
    stream = container.add_stream("mpeg4", rate=1)
    stream.width = 16
    stream.height = 16
    stream.pix_fmt = "yuv420p"

    image = image_module.new("RGB", (16, 16), color=(255, 0, 0))
    frame = av.VideoFrame.from_image(image)
    for packet in stream.encode(frame):
        container.mux(packet)
    for packet in stream.encode():
        container.mux(packet)
    container.close()


def test_http_range_input_stream_uses_range_requests(monkeypatch):
    data = b"0123456789"

    class FakeSession:

        def __init__(self):
            self.requests = []

        def get(self, uri, headers=None):
            self.requests.append((uri, headers or {}))
            range_header = (headers or {}).get("Range")
            assert range_header is not None
            start_text, end_text = range_header[len("bytes="):].split("-", 1)
            start = int(start_text)
            end = len(data) - 1 if end_text == "" else int(end_text)
            content = data[start:end + 1]
            return _Response(
                206,
                content,
                {"Content-Range": f"bytes {start}-{end}/{len(data)}"},
            )

        def head(self, uri):
            return _Response(200, headers={"Content-Length": str(len(data))})

        def close(self):
            pass

    fake_session = FakeSession()
    monkeypatch.setattr(uri_reader.requests, "Session", lambda: fake_session)

    stream = HttpRangeInputStream("http://example.com/video.mp4")
    assert stream.read(4) == b"0123"
    assert stream.seek(6) == 6
    assert stream.read(2) == b"67"
    assert stream.seek(-2, io.SEEK_END) == 8
    assert stream.read() == b"89"
    stream.close()

    ranges = [headers["Range"] for _, headers in fake_session.requests]
    assert ranges == ["bytes=0-3", "bytes=6-7", "bytes=8-"]


def test_first_frame_udf_opens_blob_descriptor_stream(tmp_path, monkeypatch):
    payload = b"video-payload"
    blob_file = tmp_path / "blob.data"
    blob_file.write_bytes(b"prefix-" + payload + b"-suffix")
    descriptor = BlobDescriptor(str(blob_file), len(b"prefix-"), len(payload))

    decoded = []

    def decode_first_frame(stream, image_format):
        decoded.append((stream.read(), image_format))
        return b"frame"

    monkeypatch.setattr(builtins, "_decode_first_frame", decode_first_frame)

    first_frame = builtins.make_first_frame({}, image_format="PNG")
    result = first_frame(pa.array([None, descriptor.serialize()], type=pa.binary()))

    assert result.to_pylist() == [None, b"frame"]
    assert decoded == [(payload, "PNG")]


def test_first_frame_udf_decodes_video_descriptor(tmp_path):
    video_path = tmp_path / "sample.mp4"
    _write_sample_video(video_path)

    descriptor = BlobDescriptor(str(video_path), 0, video_path.stat().st_size)
    first_frame = builtins.make_first_frame({}, image_format="PNG")

    result = first_frame(pa.array([descriptor.serialize()], type=pa.binary()))

    frame = result.to_pylist()[0]
    assert frame.startswith(b"\x89PNG\r\n\x1a\n")


def test_first_frame_sql_udf_decodes_video_descriptor(tmp_path):
    from pypaimon.sql.functions import register_first_frame
    from pypaimon_rust.datafusion import SQLContext

    video_path = tmp_path / "sample.mp4"
    _write_sample_video(video_path)
    descriptor = BlobDescriptor(str(video_path), 0, video_path.stat().st_size)

    ctx = SQLContext()
    ctx.register_catalog("paimon", {"warehouse": str(tmp_path / "warehouse")})
    ctx.register_batch(
        "videos",
        pa.record_batch([[1], [descriptor.serialize()]], names=["id", "video"]),
    )

    register_first_frame(ctx, {}, image_format="PNG")
    batches = ctx.sql(
        """
        SELECT id, first_frame(video) AS cover_png
        FROM paimon.default.videos
        """
    )

    table = pa.Table.from_batches(batches)
    assert table["id"].to_pylist() == [1]
    assert table["cover_png"].to_pylist()[0].startswith(b"\x89PNG\r\n\x1a\n")
