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

import struct

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.schema.data_types import is_blob_type
from pypaimon.table.row.blob import (
    Blob,
    BlobRef,
    VideoFrameDescriptor,
)
from pypaimon.write.blob_format_writer import BlobFormatWriter


class VideoFormatWriter(BlobFormatWriter):
    """Pack complete encoded videos and an embedded logical frame-run index."""

    VERSION = 1
    FOOTER_MAGIC_NUMBER = 0x4F454449
    FOOTER_SIZE = 21
    NULL_REFERENCE = -1
    PLACE_HOLDER_REFERENCE = -2

    def __init__(
            self,
            output_stream,
            file_path=None,
            copy_buffer_size=BlobFormatWriter.BUFFER_SIZE):
        super().__init__(
            output_stream,
            blob_consumer=None,
            file_path=file_path,
            copy_buffer_size=copy_buffer_size,
        )
        self._physical_lengths = []
        self._physical_videos = {}
        self._run_lengths = []
        self._run_references = []
        self._run_first_frames = []
        self._current_run_length = 0
        self._current_run_reference = None
        self._current_run_first_frame = 0
        self._current_run_last_frame = 0
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
        ordinal = self._physical_videos.get(payload)
        if ordinal is None:
            length = self._write_video_payload(value)
            ordinal = len(self._physical_lengths)
            self._physical_lengths.append(length)
            self._physical_videos[payload] = ordinal
        self._append(ordinal, frame.frame_index)

    @property
    def physical_video_count(self) -> int:
        return len(self._physical_lengths)

    @property
    def run_count(self) -> int:
        return len(self._run_lengths) + (1 if self._current_run_length else 0)

    def close(self) -> None:
        if self._closed:
            return
        self._flush_run()
        physical_index = DeltaVarintCompressor.compress(self._physical_lengths)
        run_length_index = DeltaVarintCompressor.compress(self._run_lengths)
        run_reference_index = DeltaVarintCompressor.compress(
            self._run_references
        )
        first_frame_index = DeltaVarintCompressor.compress(
            self._run_first_frames
        )
        for index in (
            physical_index,
            run_length_index,
            run_reference_index,
            first_frame_index,
        ):
            self.output_stream.write(index)
        self.output_stream.write(struct.pack(
            '<IIIIIB',
            len(physical_index),
            len(run_length_index),
            len(run_reference_index),
            len(first_frame_index),
            self.FOOTER_MAGIC_NUMBER,
            self.VERSION,
        ))
        if hasattr(self.output_stream, 'flush'):
            self.output_stream.flush()
        if hasattr(self.output_stream, 'close'):
            self.output_stream.close()
        self._closed = True

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
