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
    assert [float(timestamp) for timestamp, unused in mapping] == [i / 10 for i in range(7, 23)]
