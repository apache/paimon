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
import struct
from fractions import Fraction
from unittest.mock import patch

import numpy as np
import pytest

from pypaimon.multimodal.lerobot.indexed_torchcodec import (
    _IndexedTorchCodecDecoder, _boxes, _runs, _timeline)
from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex


@pytest.fixture(params=[False, True])
def mp4(request):
    av = pytest.importorskip("av")
    output = io.BytesIO()
    with av.open(output, "w", format="mp4") as container:
        stream = container.add_stream("libx264", rate=30)
        stream.width, stream.height = 64, 48
        stream.pix_fmt = "yuv420p"
        stream.codec_context.gop_size = 12
        stream.codec_context.max_b_frames = 2
        pts = 0
        for i in range(60):
            pixels = np.random.RandomState(i).randint(0, 256, (48, 64, 3), dtype=np.uint8)
            frame = av.VideoFrame.from_ndarray(pixels, format="rgb24")
            frame.pts, frame.time_base = pts, Fraction(1, 30)
            pts += 1 + (i % 3 == 0) if request.param else 1
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)
    return output.getvalue()


def indexed_source(payload):
    source = io.BytesIO(payload)
    source.video_length = len(payload)
    source.video_keyframe_index = VideoKeyframeIndex.inspect(source, len(payload))
    source.seek(0)
    calls = []

    def read_ranges(ranges):
        calls.extend(ranges)
        return [payload[offset:offset + length] for offset, length in ranges]

    source.video_read_ranges = read_ranges
    return source, calls


def require_torchcodec():
    try:
        from torchcodec.decoders import VideoDecoder
    except (ImportError, RuntimeError) as error:
        pytest.skip("TorchCodec unavailable: %s" % error)
    return VideoDecoder


def test_mp4_timeline_matches_full_decode(mp4):
    import av
    pts, durations, scale, stream = _timeline(io.BytesIO(mp4), len(mp4))
    with av.open(io.BytesIO(mp4)) as container:
        expected = [float(frame.pts * frame.time_base) for frame in container.decode(video=0)]
    np.testing.assert_allclose(pts / scale, expected, rtol=0, atol=1e-9)
    assert len(durations) == len(expected)
    assert stream == 0


def test_indexed_torchcodec_matches_exact_frames(mp4):
    VideoDecoder = require_torchcodec()
    source, calls = indexed_source(mp4)
    decoder = _IndexedTorchCodecDecoder(source, source.video_keyframe_index)
    try:
        assert decoder._indexed
        indices = [59, 0, 24, 24, 13, 35, 1]
        expected = VideoDecoder(io.BytesIO(mp4), seek_mode="exact").get_frames_at(indices=indices)
        actual = decoder.get_frames_at(indices=indices)
        np.testing.assert_array_equal(actual.data.numpy(), expected.data.numpy())
        np.testing.assert_allclose(actual.pts_seconds.numpy(), expected.pts_seconds.numpy())
        assert calls
        # CFR stays indexed. TorchCodec may reject VFR timestamp seeks;
        # those must still return the exact requested frames through fallback.
        pts, unused, unused, unused = _timeline(io.BytesIO(mp4), len(mp4))
        if len(np.unique(np.diff(pts))) == 1:
            assert decoder._indexed
        # Exact-mode TorchCodec versions differ in their out-of-range exception.
        errors = IndexError if decoder._indexed else (IndexError, RuntimeError)
        with pytest.raises(errors, match="[Ii]nvalid frame index|out of range"):
            decoder.get_frames_at(indices=[60])
    finally:
        decoder.close()
    assert not source.closed


def test_wrong_returned_pts_falls_back_to_exact(mp4):
    VideoDecoder = require_torchcodec()
    source, unused = indexed_source(mp4)
    decoder = _IndexedTorchCodecDecoder(source, source.video_keyframe_index)
    expected = VideoDecoder(io.BytesIO(mp4), seek_mode="exact").get_frames_at(indices=[20])
    wrong = VideoDecoder(io.BytesIO(mp4), seek_mode="exact").get_frames_at(indices=[21])
    try:
        with patch.object(decoder._decoder, "get_frames_played_at", return_value=wrong):
            actual = decoder.get_frames_at(indices=[20])
        np.testing.assert_array_equal(actual.data.numpy(), expected.data.numpy())
        assert not decoder._indexed
    finally:
        decoder.close()


def test_unsupported_timeline_falls_back_to_exact(mp4):
    VideoDecoder = require_torchcodec()
    source, unused = indexed_source(mp4)
    with patch("pypaimon.multimodal.lerobot.indexed_torchcodec._timeline",
               side_effect=ValueError("unsupported edit")):
        decoder = _IndexedTorchCodecDecoder(source, source.video_keyframe_index)
    try:
        assert not decoder._indexed
        actual = decoder[7]
        expected = VideoDecoder(io.BytesIO(mp4), seek_mode="exact")[7]
        np.testing.assert_array_equal(actual.numpy(), expected.numpy())
    finally:
        decoder.close()


def test_timing_limits_before_expanding():
    data = struct.pack(">IIII", 0, 1, 1000001, 1)
    with pytest.raises(ValueError, match="bounds"):
        _runs(data)
    with pytest.raises(ValueError, match="count"):
        _runs(data[:-1])


@pytest.mark.parametrize("data", [b"x", struct.pack(">I4s", 4, b"moov"),
                                  struct.pack(">I4s", 1, b"moov"),
                                  struct.pack(">I4s", 100, b"moov")])
def test_malformed_box_is_rejected(data):
    with pytest.raises(ValueError):
        list(_boxes(data))


def test_metadata_limit_before_reading_body():
    class Source(io.BytesIO):
        def read(self, size=-1):
            assert size <= 8
            return super().read(size)

    size = 17 * 1024 * 1024
    source = Source(struct.pack(">I4s", size, b"moov"))
    with pytest.raises(ValueError, match="bounds"):
        _timeline(source, size)


def test_first_decode_error_falls_back(mp4):
    VideoDecoder = require_torchcodec()
    source, unused = indexed_source(mp4)
    decoder = _IndexedTorchCodecDecoder(source, source.video_keyframe_index)
    try:
        with patch.object(decoder._decoder, "get_frames_played_at",
                          side_effect=IndexError("approximate timestamp")):
            actual = decoder.get_frames_at(indices=[5, 0, 5])
        expected = VideoDecoder(io.BytesIO(mp4), seek_mode="exact").get_frames_at(indices=[5, 0, 5])
        np.testing.assert_array_equal(actual.data.numpy(), expected.data.numpy())
        assert not decoder._indexed
    finally:
        decoder.close()


def test_explicit_backend_routes_indexed_videos(mp4):
    require_torchcodec()
    from pypaimon.multimodal.lerobot.dataset import _open_video_decoder
    source, unused = indexed_source(mp4)
    decoder = _open_video_decoder(source, backend="torchcodec", codec_threads=1)
    try:
        assert isinstance(decoder, _IndexedTorchCodecDecoder)
        assert decoder._indexed
    finally:
        decoder.close()


@pytest.mark.parametrize("fragmented,audio_first", [(True, False), (False, True)])
def test_fragmented_and_audio_first_video(fragmented, audio_first):
    av = pytest.importorskip("av")
    VideoDecoder = require_torchcodec()
    output = io.BytesIO()
    options = {"movflags": "frag_keyframe+empty_moov+default_base_moof"} if fragmented else None
    with av.open(output, "w", format="mp4", options=options) as container:
        if audio_first:
            audio = container.add_stream("aac", rate=48000)
            audio.layout = "mono"
        video = container.add_stream("libx264", rate=30)
        video.width, video.height = 64, 48
        video.pix_fmt = "yuv420p"
        video.codec_context.gop_size = 12
        video.codec_context.max_b_frames = 2
        if audio_first:
            for i in range(30):
                frame = av.AudioFrame.from_ndarray(np.zeros((1, 1600), dtype=np.float32),
                                                   format="fltp", layout="mono")
                frame.sample_rate = 48000
                frame.pts = i * 1600
                frame.time_base = Fraction(1, 48000)
                for packet in audio.encode(frame):
                    container.mux(packet)
            for packet in audio.encode():
                container.mux(packet)
        for i in range(30):
            frame = av.VideoFrame.from_ndarray(np.full((48, 64, 3), i * 7, dtype=np.uint8), format="rgb24")
            for packet in video.encode(frame):
                container.mux(packet)
        for packet in video.encode():
            container.mux(packet)
    payload = output.getvalue()
    source, unused = indexed_source(payload)
    decoder = _IndexedTorchCodecDecoder(source, source.video_keyframe_index)
    try:
        assert decoder._indexed is (not fragmented)
        positions = [29, 0, 16, 4]
        actual = decoder.get_frames_at(indices=positions)
        expected = VideoDecoder(io.BytesIO(payload), stream_index=1 if audio_first else 0,
                                seek_mode="exact").get_frames_at(indices=positions)
        np.testing.assert_array_equal(actual.data.numpy(), expected.data.numpy())
        np.testing.assert_allclose(actual.pts_seconds.numpy(), expected.pts_seconds.numpy())
    finally:
        decoder.close()


def test_multiple_video_tracks_preserve_exact_selection():
    av = pytest.importorskip("av")
    VideoDecoder = require_torchcodec()
    output = io.BytesIO()
    with av.open(output, "w", format="mp4") as container:
        streams = [container.add_stream("libx264", rate=30) for _ in range(2)]
        for i, stream in enumerate(streams):
            stream.width, stream.height = 32 * (i + 1), 32 * (i + 1)
            stream.pix_fmt = "yuv420p"
        for i in range(12):
            for stream in streams:
                pixels = np.full((stream.height, stream.width, 3), i * 16, dtype=np.uint8)
                frame = av.VideoFrame.from_ndarray(pixels, format="rgb24")
                for packet in stream.encode(frame):
                    container.mux(packet)
        for stream in streams:
            for packet in stream.encode():
                container.mux(packet)
    payload = output.getvalue()
    source, unused = indexed_source(payload)
    decoder = _IndexedTorchCodecDecoder(source, source.video_keyframe_index)
    try:
        assert not decoder._indexed
        actual = decoder.get_frames_at(indices=[11, 0, 5])
        expected = VideoDecoder(io.BytesIO(payload), seek_mode="exact").get_frames_at(indices=[11, 0, 5])
        np.testing.assert_array_equal(actual.data.numpy(), expected.data.numpy())
    finally:
        decoder.close()


def test_signed_composition_offsets():
    counts, values = _runs(struct.pack(">IIIi", 1 << 24, 1, 3, -2), composition=True)
    np.testing.assert_array_equal(counts, [3])
    np.testing.assert_array_equal(values, [-2])


def timing_only_mp4(count, variable=False):
    def box(kind, body):
        return struct.pack('>I4s', len(body) + 8, kind) + body

    if variable:
        runs = np.ones((count, 2), dtype='>u4')
        runs[:, 1] = 1001
        runs[1::2, 1] = 1002
        stts = box(b'stts', struct.pack('>II', 0, count) + runs.tobytes())
    else:
        stts = box(b'stts', struct.pack('>IIII', 0, 1, count, 1001))
    stsz = box(b'stsz', struct.pack('>III', 0, 1, count))
    media = box(b'mdhd', b'\0' * 12 + struct.pack('>I', 30000))
    media += box(b'hdlr', b'\0' * 8 + b'vide')
    media += box(b'minf', box(b'stbl', stts + stsz))
    return box(b'moov', box(b'trak', box(b'mdia', media)))


@pytest.mark.parametrize("variable", [False, True])
def test_million_sample_timeline_memory(variable):
    import tracemalloc
    from pypaimon.multimodal.lerobot.indexed_torchcodec import _MAX_SAMPLES, _SharedTimeline

    payload = timing_only_mp4(_MAX_SAMPLES, variable=variable)
    tracemalloc.start()
    try:
        timeline = _SharedTimeline(io.BytesIO(payload), len(payload))
        retained, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    if variable:
        assert timeline.pts.values.nbytes + timeline.durations.values.nbytes == 16_000_000
        assert retained < 17 * 1024 * 1024
        assert peak < 64 * 1024 * 1024
    else:
        assert timeline.pts.values is None
        assert timeline.durations.values is None
        assert retained < 1024 * 1024
        assert peak < 24 * 1024 * 1024
    positions = np.array([0, 500000, _MAX_SAMPLES - 1, 0])
    np.testing.assert_array_equal(timeline.pts[positions], positions * 1001 + (positions // 2 if variable else 0))
    np.testing.assert_array_equal(timeline.durations[positions], 1001 + positions % 2 if variable else [1001] * 4)


def test_irregular_timing_arrays_are_read_only():
    from pypaimon.multimodal.lerobot.indexed_torchcodec import _TimingValues

    original = np.array([0, 1001, 3003, 4004], dtype=np.int64)
    timing = _TimingValues(original)
    assert timing.values is original
    assert not timing.values.flags.writeable
    np.testing.assert_array_equal(timing[[3, 0, 1]], [4004, 0, 1001])
    with pytest.raises(ValueError):
        timing.values[0] = 1


@pytest.mark.parametrize("variable", [False, True])
def test_replica_decoders_share_and_release_timeline(tmp_path, variable):
    import sys
    import weakref
    from types import SimpleNamespace
    from unittest.mock import Mock
    from pypaimon.common.file_io import FileIO
    from pypaimon.multimodal.video import VideoFrameCollator
    from pypaimon.table.row.blob import VideoFrameDescriptor

    payload = timing_only_mp4(1000000, variable=variable)
    mapping = VideoKeyframeIndex([(0, len(payload))], [(0, 0, 0)]).serialize()
    path = tmp_path / 'replicas.video'
    path.write_bytes(payload + mapping)
    frame = VideoFrameDescriptor(path.as_uri(), 0, len(payload), 0, len(payload), len(mapping))
    table = SimpleNamespace(file_io=FileIO.get(path.as_uri(), {}))
    collator = VideoFrameCollator(
        table, video_column='video', max_open_videos=4,
        decoder_factory=lambda stream: _IndexedTorchCodecDecoder(stream, stream.video_keyframe_index),
        decode_fn=lambda *args: None)
    fake = SimpleNamespace(VideoDecoder=Mock())
    with patch.dict(sys.modules, {'torchcodec.decoders': fake}), patch(
            'pypaimon.multimodal.lerobot.indexed_torchcodec._timeline', wraps=_timeline) as parse:
        try:
            decoders = [collator._decoder(frame.payload_descriptor, frame.keyframe_index_descriptor, replica=i)
                        for i in range(4)]
            assert all(decoder._indexed for decoder in decoders)
            assert parse.call_count == 1
            assert all(decoder._timeline is decoders[0]._timeline for decoder in decoders)
            reference = weakref.ref(decoders[0]._timeline)
            state = collator.__getstate__()
            assert state['_video_metadata_cache'] is None
            worker = VideoFrameCollator.__new__(VideoFrameCollator)
            worker.__dict__.update(state)
            worker._ensure_process_local_cache()
            assert len(worker._video_metadata_cache) == 0
            assert len(collator._video_metadata_cache) == 1
            worker.close()
            decoders[0]._fallback()
            assert reference() is not None
            assert decoders[1]._pts[999999] == 999999 * 1001 + (499999 if variable else 0)
            if variable:
                assert decoders[1]._pts.values.nbytes == 8_000_000
                assert all(decoder._pts is decoders[1]._pts for decoder in decoders[1:])
                assert all(decoder._durations is decoders[1]._durations for decoder in decoders[1:])
            other_path = tmp_path / 'another.video'
            other_path.write_bytes(payload + mapping)
            other_frame = VideoFrameDescriptor(other_path.as_uri(), 0, len(payload), 0, len(payload), len(mapping))
            other = collator._decoder(other_frame.payload_descriptor, other_frame.keyframe_index_descriptor)
            assert other._timeline is not reference()
            assert parse.call_count == 2
        finally:
            collator.close()
        assert reference() is None
