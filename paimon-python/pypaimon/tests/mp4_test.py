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
import weakref
from unittest.mock import patch

import numpy as np
import pytest

from pypaimon.multimodal.mp4 import ContainerFrameIndex
from pypaimon.tests import indexed_torchcodec_test

mp4 = indexed_torchcodec_test.mp4


def test_container_locations_match_decoded_keyframes(mp4):
    import av
    mapping = ContainerFrameIndex(io.BytesIO(mp4), len(mp4))
    with av.open(io.BytesIO(mp4)) as source:
        frames = list(source.decode(video=0))
    expected = [(i, frame.pts) for i, frame in enumerate(frames) if frame.key_frame]
    assert mapping.keyframes[:, :2].tolist() == [list(entry) for entry in expected]
    np.testing.assert_array_equal(mapping.pts[list(range(len(frames)))], [f.pts for f in frames])
    with av.open(io.BytesIO(mp4)) as source:
        packets = {packet.pts: packet.pos for packet in source.demux(video=0) if packet.is_keyframe}
    assert [packets[int(pts)] for _, pts, _ in mapping.keyframes] == mapping.keyframes[:, 2].tolist()


def test_container_reader_avoids_packet_scan_and_shares_mapping(mp4):
    import av
    from pypaimon.multimodal.lerobot.dataset import _open_video_decoder
    with av.open(io.BytesIO(mp4)) as source:
        expected = [frame.to_ndarray(format="rgb24") for frame in source.decode(video=0)]
    cache = weakref.WeakValueDictionary()
    readers = []
    try:
        for unused in range(2):
            stream = io.BytesIO(mp4)
            stream.video_length = len(mp4)
            stream.video_metadata_cache = cache
            stream.video_metadata_key = 'same-video'
            stream.video_read_ranges = lambda ranges: [mp4[o:o + n] for o, n in ranges]
            decoder = _open_video_decoder(stream, backend="pyav")
            readers.append(decoder)
            assert decoder._keyframe_index is not None
            decoder._tensor = lambda frame: frame.to_ndarray(format="rgb24")
            with patch.object(decoder, '_index_packets', side_effect=AssertionError('packet scan')):
                for ordinal in [59, 0, 31, 12, 1, 48]:
                    np.testing.assert_array_equal(decoder[ordinal], expected[ordinal])
        assert readers[0]._keyframe_index is readers[1]._keyframe_index
    finally:
        for decoder in readers:
            decoder.close()


def test_chunk_extent_is_checked_before_decode(mp4):
    data = bytearray(mp4)
    position = data.index(b'stco')
    struct.pack_into('>I', data, position + 12, len(data) + 1)
    with pytest.raises(ValueError, match='exceeds video payload'):
        ContainerFrameIndex(io.BytesIO(data), len(data))


def test_missing_sample_locations_are_rejected():
    from pypaimon.tests.indexed_torchcodec_test import timing_only_mp4
    data = timing_only_mp4(100)
    with pytest.raises(ValueError, match='chunk offsets'):
        ContainerFrameIndex(io.BytesIO(data), len(data))


def test_pyav_timestamp_mismatch_falls_back_to_scan(mp4):
    import av
    from pypaimon.multimodal.lerobot.dataset import _PyAVVideoDecoder
    from pypaimon.multimodal.mp4 import _TimingValues
    mapping = ContainerFrameIndex(io.BytesIO(mp4), len(mp4))
    timestamps = mapping.pts[list(range(len(mapping.pts)))]
    timestamps[31] += 1
    mapping.pts = _TimingValues(timestamps)
    with av.open(io.BytesIO(mp4)) as container:
        expected = list(container.decode(video=0))[31].to_ndarray(format="rgb24")
    decoder = _PyAVVideoDecoder(io.BytesIO(mp4), mapping)
    decoder._tensor = lambda frame: frame.to_ndarray(format="rgb24")
    try:
        first = decoder[0]
        np.testing.assert_array_equal(expected, decoder[31])
        assert decoder._keyframe_index is None
        np.testing.assert_array_equal(first, decoder[0])
    finally:
        decoder.close()


def test_stored_moov_skips_top_level_walk(mp4):
    from pypaimon.common.mp4_moov import MoovLocator
    locator = MoovLocator()
    for offset in range(0, len(mp4), 7):
        locator.feed(mp4[offset:offset + 7])
    hint = locator.finish()
    assert hint[1] > 0
    expected = ContainerFrameIndex(io.BytesIO(mp4), len(mp4))

    class Source(io.BytesIO):
        def read(self, size=-1):
            assert self.tell() >= hint[0]
            return super().read(size)

    source = Source(mp4)
    source.video_moov_range = hint
    actual = ContainerFrameIndex(source, len(mp4))
    np.testing.assert_array_equal(actual.keyframes, expected.keyframes)
    assert actual.metadata_ranges == (hint,)


@pytest.mark.parametrize('hint', [(0, 8), (1, 8), (0, 2**63 - 1)])
def test_bad_moov_hint_falls_back_to_box_walk(mp4, hint):
    source = io.BytesIO(mp4)
    source.video_moov_range = hint
    actual = ContainerFrameIndex(source, len(mp4))
    expected = ContainerFrameIndex(io.BytesIO(mp4), len(mp4))
    np.testing.assert_array_equal(actual.keyframes, expected.keyframes)


def test_writer_retains_moov_on_rewrite(mp4, tmp_path):
    from pypaimon.common.options import Options
    from pypaimon.execution.python.write.video_format_writer import VideoFormatWriter
    from pypaimon.execution.python.read.reader.video_format_reader import VideoFileMeta
    from pypaimon.filesystem.local_file_io import LocalFileIO
    from pypaimon.table.row.blob import Blob, VideoFrameDescriptor
    from pypaimon.table.row.generic_row import GenericRow
    from pypaimon.schema.data_types import AtomicType, DataField
    file_io = LocalFileIO(str(tmp_path), Options({}))
    source = tmp_path / 'source.mp4'
    source.write_bytes(mp4)
    frame = VideoFrameDescriptor(source.as_uri(), 0, len(mp4), 7)
    expected = ContainerFrameIndex(io.BytesIO(mp4), len(mp4)).metadata_ranges[0]
    for name in ('first.video', 'rewrite.video'):
        target = tmp_path / name
        writer = VideoFormatWriter(file_io.new_output_stream(target.as_uri()))
        blob = Blob.from_descriptor(file_io.uri_reader_factory.create(frame.uri), frame)
        writer.add_element(GenericRow([blob], [DataField(0, 'video', AtomicType('BLOB'))]))
        writer.close()
        raw = target.read_bytes()
        assert raw[:len(mp4)] == mp4
        meta = VideoFileMeta(io.BytesIO(raw), len(raw))
        assert meta.frame(0) == (0, len(mp4), 7)
        assert meta.moov_range(0) == expected
        frame = VideoFrameDescriptor(target.as_uri(), *meta.frame(0), *meta.moov_range(0))
        assert VideoFrameDescriptor.deserialize(frame.serialize()) == frame


@pytest.mark.parametrize('data', [b'abc', struct.pack('>I4s', 99, b'moov'),
                                  struct.pack('>I4s', 8, b'moov') * 2])
def test_moov_locator_rejects_malformed_or_duplicate_boxes(data):
    from pypaimon.common.mp4_moov import MoovLocator
    locator = MoovLocator()
    locator.feed(data)
    assert locator.finish() == (0, 0)


@pytest.mark.parametrize("backend", ["pyav", "torchcodec"])
def test_stored_moov_bootstrap_decodes_without_refetch(mp4, backend):
    pytest.importorskip("torch")
    import av
    indexed = True
    if backend == "torchcodec":
        from pypaimon.tests.indexed_torchcodec_test import require_torchcodec, supports_file_like
        require_torchcodec()
        indexed = supports_file_like()
    from pypaimon.multimodal.lerobot.dataset import _open_video_decoder
    mapping = ContainerFrameIndex(io.BytesIO(mp4), len(mp4))
    hint = mapping.metadata_ranges[0]
    calls = []
    source = io.BytesIO(mp4)
    source.video_length = len(mp4)
    source.video_moov_range = hint

    def ranges(regions):
        calls.append(regions)
        return [mp4[o:o + n] for o, n in regions]

    source.video_read_ranges = ranges
    with av.open(io.BytesIO(mp4)) as container:
        expected = list(container.decode(video=0))[31].to_ndarray(format="rgb24")
    decoder = _open_video_decoder(source, backend=backend)
    try:
        if backend == "pyav":
            assert decoder._keyframe_index is not None
        else:
            assert decoder._indexed is indexed
        frame = decoder.get_frames_at(indices=[31]).data[0].permute(1, 2, 0).numpy()
        np.testing.assert_array_equal(frame, expected)
        assert calls
        assert any(o <= hint[0] and o + n >= sum(hint) for o, n in calls[0])
        if indexed:
            assert not any(o < sum(hint) and o + n > hint[0]
                           for request in calls[1:] for o, n in request)
    finally:
        decoder.close()


def test_first_sync_sample_must_cover_first_frame(mp4):
    data = bytearray(mp4)
    position = data.index(b'stss')
    struct.pack_into('>I', data, position + 12, 2)
    with pytest.raises(ValueError, match='keyframe order'):
        ContainerFrameIndex(io.BytesIO(data), len(data))


def test_transient_io_and_invalid_frame_keep_mapping(mp4):
    pytest.importorskip("torch")
    from pypaimon.multimodal.lerobot.dataset import _PyAVVideoDecoder
    mapping = ContainerFrameIndex(io.BytesIO(mp4), len(mp4))
    decoder = _PyAVVideoDecoder(io.BytesIO(mp4), mapping)
    try:
        with patch.object(decoder, '_read_video_ranges', side_effect=OSError('temporary')):
            with pytest.raises(OSError, match='temporary'):
                decoder.get_frames_at(indices=[31])
        assert decoder._keyframe_index is mapping
        with patch.object(decoder, '_decode_indexed', side_effect=AssertionError('decode')):
            with pytest.raises(IndexError):
                decoder.get_frames_at(indices=[len(mapping.pts)])
        assert decoder._keyframe_index is mapping
        assert decoder.get_frames_at(indices=[31]).data.shape[0] == 1
    finally:
        decoder.close()


def test_open_runtime_error_falls_back(mp4):
    pytest.importorskip("torch")
    from pypaimon.multimodal.lerobot.dataset import _open_video_decoder
    source = io.BytesIO(mp4)
    source.video_length = len(mp4)
    with patch('pypaimon.multimodal.lerobot.dataset.ContainerFrameIndex',
               side_effect=RuntimeError('initialization')):
        decoder = _open_video_decoder(source, backend='pyav')
    try:
        assert decoder._keyframe_index is None
        assert decoder.get_frames_at(indices=[0]).data.shape[0] == 1
    finally:
        decoder.close()


def test_cached_mapping_prefetches_header_and_moov(mp4):
    pytest.importorskip("torch")
    from pypaimon.multimodal.lerobot.dataset import _open_video_decoder
    mapping = ContainerFrameIndex(io.BytesIO(mp4), len(mp4))
    calls = []
    source = io.BytesIO(mp4)
    source.video_length = len(mp4)
    source.video_metadata_key = 'video'
    source.video_metadata_cache = {('container', 'video'): mapping}

    def ranges(regions):
        calls.append(regions)
        return [mp4[o:o + n] for o, n in regions]

    source.video_read_ranges = ranges
    with patch('pypaimon.multimodal.lerobot.dataset.ContainerFrameIndex',
               side_effect=AssertionError('reparse')):
        decoder = _open_video_decoder(source, backend='pyav')
    try:
        decoder.get_frames_at(indices=[31])
        assert calls[0][0][0] == 0
        offset, length = mapping.metadata_ranges[0]
        assert any(o <= offset and o + n >= offset + length for o, n in calls[0])
    finally:
        decoder.close()


def test_metadata_cache_retention_and_budget(mp4):
    import gc
    from pypaimon.multimodal.video import _VideoMetadataCache
    mapping = ContainerFrameIndex(io.BytesIO(mp4), len(mp4))
    size = mapping.nbytes
    cache = _VideoMetadataCache(max_bytes=size, max_entries=1)
    cache['first'] = mapping
    reference = weakref.ref(mapping)
    del mapping
    gc.collect()
    assert cache.get('first') is reference()
    assert reference() is not None
    cache['second'] = ContainerFrameIndex(io.BytesIO(mp4), len(mp4))
    gc.collect()
    assert reference() is None
    assert cache._bytes <= size
    cache.clear()
    assert len(cache) == 0


def test_writer_close_failure_is_not_retried():
    from pypaimon.execution.python.write.video_format_writer import VideoFormatWriter

    class FailingOutput(io.BytesIO):
        writes = 0

        def write(self, data):
            self.writes += 1
            raise OSError('footer write')

    output = FailingOutput()
    writer = VideoFormatWriter(output)
    with pytest.raises(OSError, match='footer write'):
        writer.close()
    assert output.closed
    writes = output.writes
    writer.close()
    assert output.writes == writes


@pytest.mark.parametrize('wide', [False, True])
def test_vectorized_chunk_offsets(wide):
    from pypaimon.multimodal.mp4 import _sample_offsets

    def box(kind, content):
        return struct.pack('>I4s', len(content) + 8, kind) + content

    sizes = [3, 5, 7, 11, 13, 17, 19]
    offsets = [100, 200, 350]
    samples = box(b'stsz', struct.pack('>III7I', 0, 0, 7, *sizes))
    samples += box(b'co64' if wide else b'stco', struct.pack(
        '>II3' + ('Q' if wide else 'I'), 0, 3, *offsets))
    samples += box(b'stsc', struct.pack('>II6I', 0, 2, 1, 2, 1, 3, 3, 1))
    np.testing.assert_array_equal(
        _sample_offsets(samples, 7, 500), [100, 103, 200, 207, 350, 363, 380])
    with pytest.raises(ValueError, match='exceeds video payload'):
        _sample_offsets(samples, 7, 398)


def test_chunk_count_is_checked_before_expansion():
    from pypaimon.multimodal.mp4 import _sample_offsets

    def box(kind, content):
        return struct.pack('>I4s', len(content) + 8, kind) + content

    samples = box(b'stsz', struct.pack('>III', 0, 1, 1))
    samples += box(b'co64', struct.pack('>IIQ', 0, 1, 2**64 - 1))
    samples += box(b'stsc', struct.pack('>II3I', 0, 1, 1, 2**32 - 1, 1))
    with pytest.raises(ValueError, match='cover samples'):
        _sample_offsets(samples, 1, 100)
