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

import errno
import logging
import shutil
import tempfile
import struct
import sys

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.schema.data_types import is_blob_type
from pypaimon.table.row.blob import (
    Blob,
    BlobRef,
    VideoFrameDescriptor,
)
from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex, UnsupportedVideoIndex
from pypaimon.execution.python.write.blob_format_writer import BlobFormatWriter


def _video_index_fallback_errors():
    errors = (ImportError, OSError, ValueError, EOFError)
    av = sys.modules.get("av")
    ffmpeg_error = getattr(getattr(av, "error", None), "FFmpegError", None)
    return errors if ffmpeg_error is None else errors + (ffmpeg_error,)


class VideoFormatWriter(BlobFormatWriter):
    """Pack complete encoded videos and an embedded logical frame-run index."""

    VERSION = 1
    FOOTER_MAGIC_NUMBER = 0x4F454449
    FOOTER_SIZE = 25
    NULL_REFERENCE = -1
    PLACE_HOLDER_REFERENCE = -2

    def __init__(
            self,
            output_stream,
            file_path=None,
            copy_buffer_size=BlobFormatWriter.BUFFER_SIZE,
            index_compression="delta-zlib"):
        super().__init__(
            output_stream,
            blob_consumer=None,
            file_path=file_path,
            copy_buffer_size=copy_buffer_size,
        )
        if index_compression not in ('none', 'delta-zlib'):
            raise ValueError("Video index compression must be 'none' or 'delta-zlib'.")
        self._index_compression = index_compression
        self._physical_lengths = []
        self._keyframe_indexes = []
        self._index_spool = tempfile.SpooledTemporaryFile(max_size=1024 * 1024)
        self._physical_videos = {}
        self._physical_video_keyframe_indexes = {}
        self._run_lengths = []
        self._run_references = []
        self._run_first_frames = []
        self._current_run_length = 0
        self._current_run_reference = None
        self._current_run_first_frame = 0
        self._current_run_last_frame = 0
        self._keyframe_index_bytes = 0
        self._closed = False

    def add_element(self, row) -> None:
        if not hasattr(row, 'values') or len(row.values) != 1:
            raise ValueError("VideoFormatWriter only supports one field")
        if not is_blob_type(row.fields[0].type):
            raise ValueError(
                "VideoFormatWriter only supports one scalar BLOB field"
            )

        value = row.values[0]
        if value is None:
            self._append(self.NULL_REFERENCE, 0)
            return
        if value is Blob.PLACE_HOLDER:
            self._append(self.PLACE_HOLDER_REFERENCE, 0)
            return
        if type(value) is not BlobRef:
            raise ValueError(
                "Video fields require an exact BlobRef containing a "
                "VideoFrameDescriptor."
            )

        frame = value.to_descriptor()
        if not isinstance(frame, VideoFrameDescriptor):
            raise ValueError(
                "Video fields require an exact BlobRef containing a "
                "VideoFrameDescriptor."
            )
        payload = frame.payload_descriptor
        keyframe_index_descriptor = frame.keyframe_index_descriptor
        ordinal = self._physical_videos.get(payload)
        if ordinal is None:
            keyframe_index = self._keyframe_index(value, frame)
            try:
                length = self._write_video_payload(value)
            except BaseException:
                self._index_spool.seek(self._keyframe_index_bytes)
                self._index_spool.truncate()
                raise
            ordinal = len(self._physical_lengths)
            self._physical_lengths.append(length)
            self._keyframe_indexes.append(keyframe_index)
            self._keyframe_index_bytes += keyframe_index
            self._physical_videos[payload] = ordinal
            self._physical_video_keyframe_indexes[
                payload] = keyframe_index_descriptor
        elif (self._physical_video_keyframe_indexes[payload]
              != keyframe_index_descriptor):
            raise ValueError(
                "Video frames for the same payload must use the same "
                "keyframe index."
            )
        self._append(ordinal, frame.frame_index)

    def reach_target_size(self, target_size: int) -> bool:
        return self.position + self._keyframe_index_bytes >= target_size

    @property
    def physical_video_count(self) -> int:
        return len(self._physical_lengths)

    @property
    def run_count(self) -> int:
        return len(self._run_lengths) + (1 if self._current_run_length else 0)

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._flush_run()
            self._index_spool.seek(0)
            shutil.copyfileobj(self._index_spool, self.output_stream, self.copy_buffer_size)
            physical_index = DeltaVarintCompressor.compress(self._physical_lengths)
            keyframe_length_index = DeltaVarintCompressor.compress(
                self._keyframe_indexes
            )
            run_length_index = DeltaVarintCompressor.compress(self._run_lengths)
            run_reference_index = DeltaVarintCompressor.compress(
                self._run_references
            )
            first_frame_index = DeltaVarintCompressor.compress(
                self._run_first_frames
            )
            for index in (
                    physical_index,
                    keyframe_length_index,
                    run_length_index,
                    run_reference_index,
                    first_frame_index):
                self.output_stream.write(index)
            footer = struct.pack(
                '<IIIIIIB',
                len(physical_index),
                len(keyframe_length_index),
                len(run_length_index),
                len(run_reference_index),
                len(first_frame_index),
                self.FOOTER_MAGIC_NUMBER,
                self.VERSION,
            )
            self.output_stream.write(footer)
            if hasattr(self.output_stream, 'flush'):
                self.output_stream.flush()
            if hasattr(self.output_stream, 'close'):
                self.output_stream.close()
        finally:
            self._index_spool.close()
            self._closed = True

    def _keyframe_index(self, blob, frame):
        descriptor = frame.keyframe_index_descriptor
        start = self._index_spool.tell()
        try:
            if descriptor is None:
                with blob.new_input_stream() as stream:
                    payload_length = frame.payload_descriptor.length
                    if payload_length < 0:
                        stream.seek(0, 2)
                        payload_length = stream.tell()
                        stream.seek(0)
                    try:
                        mapping = VideoKeyframeIndex.inspect(stream, payload_length)
                    except _video_index_fallback_errors() as error:
                        if isinstance(error, OSError) and error.errno in (errno.ENOSPC, errno.EDQUOT, errno.EIO):
                            raise
                        return 0
                    with mapping:
                        mapping.write_to(self._index_spool, self._index_compression)
            else:
                with tempfile.TemporaryFile() as mapping:
                    with Blob.from_descriptor(blob.uri_reader, descriptor).new_input_stream() as source:
                        shutil.copyfileobj(source, mapping, self.copy_buffer_size)
                    if mapping.tell() != descriptor.length:
                        raise ValueError("Video keyframe index length changed while reading.")
                    try:
                        VideoKeyframeIndex.validate_stream(mapping, descriptor.length, frame.payload_descriptor.length)
                    except UnsupportedVideoIndex as error:
                        logging.getLogger(__name__).warning("Preserving unsupported video index: %s", error)
                    mapping.seek(0)
                    shutil.copyfileobj(mapping, self._index_spool, self.copy_buffer_size)
            return self._index_spool.tell() - start
        except BaseException:
            self._index_spool.seek(start)
            self._index_spool.truncate()
            raise

    def _write_video_payload(self, blob: BlobRef) -> int:
        start = self.position
        stream = blob.new_input_stream()
        try:
            while True:
                chunk = stream.read(self.copy_buffer_size)
                if not chunk:
                    break
                self.output_stream.write(chunk)
                self.position += len(chunk)
        finally:
            stream.close()
        length = self.position - start
        if length <= 0:
            raise ValueError("Encoded video payload must not be empty.")
        return length

    def _append(self, reference: int, frame_index: int) -> None:
        if self._can_extend(reference, frame_index):
            self._current_run_length += 1
            self._current_run_last_frame = frame_index
            return
        self._flush_run()
        self._current_run_reference = reference
        self._current_run_first_frame = frame_index
        self._current_run_last_frame = frame_index
        self._current_run_length = 1

    def _can_extend(self, reference: int, frame_index: int) -> bool:
        if (
            self._current_run_length == 0
            or self._current_run_reference != reference
        ):
            return False
        return (
            reference < 0
            or frame_index == self._current_run_last_frame + 1
        )

    def _flush_run(self) -> None:
        if self._current_run_length == 0:
            return
        self._run_lengths.append(self._current_run_length)
        self._run_references.append(self._current_run_reference)
        self._run_first_frames.append(self._current_run_first_frame)
        self._current_run_length = 0
