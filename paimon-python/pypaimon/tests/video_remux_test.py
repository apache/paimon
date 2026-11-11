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

import io
from fractions import Fraction

import numpy as np
import pytest

from pypaimon.multimodal.video_remux import remux_video_interval

av = pytest.importorskip("av")


@pytest.fixture(params=[0, 2])
def video(tmp_path, request):
    path = tmp_path / "input.mp4"
    with av.open(str(path), "w") as container:
        stream = container.add_stream("libx264", rate=10)
        stream.width = 64
        stream.height = 48
        stream.pix_fmt = "yuv420p"
        stream.codec_context.gop_size = 10
        stream.codec_context.max_b_frames = request.param
        for i in range(50):
            pixels = np.full((48, 64, 3), (i * 5) % 255, dtype=np.uint8)
            frame = av.VideoFrame.from_ndarray(pixels, format="rgb24")
            frame.pts = i
            frame.time_base = Fraction(1, 10)
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)
    return path


@pytest.mark.parametrize("start,end", [(0, 0.4), (0.7, 2.3), (4.4, 5)])
def test_remux_preserves_requested_frames(video, start, end):
    with av.open(str(video)) as source:
        expected = {f.pts * f.time_base: f.to_ndarray(format="rgb24")
                    for f in source.decode(video=0)
                    if start <= float(f.pts * f.time_base) < end}
    payload, mapping = remux_video_interval(str(video), start, end)
    with av.open(io.BytesIO(payload)) as source:
        frames = list(source.decode(video=0))
    mapping = [(t, ordinal) for t, ordinal in mapping if start <= float(t) < end]
    assert set(t for t, unused in mapping) == set(expected)
    for timestamp, ordinal in mapping:
        np.testing.assert_array_equal(frames[ordinal].to_ndarray(format="rgb24"),
                                      expected[timestamp])


def test_remux_bounds_output(video):
    with pytest.raises(ValueError, match="memory limit"):
        remux_video_interval(str(video), 0, 5, max_bytes=32)


def test_remux_rejects_empty_interval(video):
    with pytest.raises(ValueError, match="Invalid video interval"):
        remux_video_interval(str(video), 2, 1)


def test_remux_accepts_episode_boundary_roundoff(video):
    payload, mapping = remux_video_interval(str(video), 0.70000000000002, 2.30000000000002)
    assert set(Fraction(i, 10) for i in range(7, 23)).issubset(t for t, unused in mapping)


@pytest.mark.parametrize("version", ["12.0.0", "13.1.0", "14.1.0"])
def test_remux_requires_supported_av_before_open(monkeypatch, version):
    from unittest.mock import Mock
    from pypaimon.multimodal.lerobot.loader import _prepare_dataset

    monkeypatch.setattr(av, "__version__", version)
    opened = Mock(side_effect=AssertionError("Source must not be opened"))
    monkeypatch.setattr(av, "open", opened)
    with pytest.raises(ImportError, match="PyAV 14.2"):
        remux_video_interval("unused.mp4", 0, 1)
    table = Mock()
    table.options.options.get.return_value = True
    with pytest.raises(ImportError, match="PyAV 14.2"):
        _prepare_dataset(table, Mock(), {}, None, None, 1, {}, video_fields=("camera",))
    table.new_batch_write_builder.assert_not_called()
    opened.assert_not_called()


@pytest.mark.parametrize("b_frames", [0, 2])
def test_remux_preserves_float32_episode_bounds(tmp_path, b_frames):
    import pyarrow as pa
    from pypaimon.multimodal.lerobot.loader import (
        _video_frame_ordinal, _validate_video_timestamp_range)
    from pypaimon.multimodal.video_remux import _EpisodeVideoSources
    from pypaimon.table.row.blob import VideoFrameDescriptor
    from pypaimon.common.uri_reader import UriReaderFactory

    path = tmp_path / "float32.mp4"
    with av.open(str(path), "w") as container:
        stream = container.add_stream("libx264", rate=30)
        stream.width, stream.height = 64, 48
        stream.pix_fmt = "yuv420p"
        stream.codec_context.gop_size = 12
        stream.codec_context.max_b_frames = b_frames
        for i in range(156):
            frame = av.VideoFrame.from_ndarray(
                np.full((48, 64, 3), i, dtype=np.uint8), format="rgb24")
            frame.pts, frame.time_base = i, Fraction(1, 30)
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)
    with av.open(str(path)) as container:
        original = list(container.decode(video=0))
    times = [float(frame.pts * frame.time_base) for frame in original]
    start, end = float(np.float32(4.8)), float(np.float32(4.9))
    _validate_video_timestamp_range(start, end, 30, 3, "camera")
    ordinals = [_video_frame_ordinal(times, float(np.float32(i / 30)), start,
                                     pa.float32(), "camera") for i in range(3)]
    assert ordinals == [144, 145, 146]
    factory = UriReaderFactory({})
    sources = _EpisodeVideoSources(factory)
    try:
        # Split calls also exercise reuse of the Episode mapping across batches.
        rows = [sources.descriptors(path.as_uri(), start, end, [times[i]])[0] for i in ordinals]
        descriptors = [VideoFrameDescriptor.deserialize(row) for row in rows]
        payload = sources.payloads[descriptors[0].uri][:descriptors[0].length]
        with av.open(io.BytesIO(payload)) as container:
            decoded = list(container.decode(video=0))
        for ordinal, descriptor in zip(ordinals, descriptors):
            np.testing.assert_array_equal(
                decoded[descriptor.frame_index].to_ndarray(format="rgb24"),
                original[ordinal].to_ndarray(format="rgb24"))
    finally:
        factory.close()
