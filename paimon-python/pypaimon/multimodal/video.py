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

"""PyTorch DataLoader helpers for descriptor-backed video frame rows."""

import os
from collections import OrderedDict
from collections.abc import Mapping

from pypaimon.table.row.blob import Blob, BlobDescriptor


class VideoFrameCollator:
    """Decode frame rows in a DataLoader worker while reusing video sessions.

    ``decoder_factory`` receives a seekable stream containing exactly one
    descriptor-backed video. ``decode_fn`` receives the cached decoder and one
    row dictionary. This keeps Paimon independent of a particular video codec
    library while allowing PyAV, TorchCodec, or an application decoder to be
    plugged in.

    The cache is process-local and keyed by exact ``BlobDescriptor`` identity.
    ``collate_fn`` defaults to PyTorch's ``default_collate`` and may be replaced
    for decoders that already return batched objects.
    """

    def __init__(
            self,
            table,
            *,
            video_column,
            decoder_factory,
            decode_fn,
            output_column="frame",
            max_open_videos=8,
            collate_fn=None):
        if not video_column:
            raise ValueError("video_column is required.")
        if not callable(decoder_factory):
            raise ValueError("decoder_factory must be callable.")
        if not callable(decode_fn):
            raise ValueError("decode_fn must be callable.")
        if (
            isinstance(max_open_videos, bool)
            or not isinstance(max_open_videos, int)
            or max_open_videos <= 0
        ):
            raise ValueError("max_open_videos must be a positive int.")
        if collate_fn is not None and not callable(collate_fn):
            raise ValueError("collate_fn must be callable or None.")

        raw_table = getattr(table, "raw_table", table)
        file_io = getattr(raw_table, "file_io", None)
        if file_io is None:
            raise ValueError("table must provide raw_table.file_io or file_io.")

        self.file_io = file_io
        self.video_column = video_column
        self.decoder_factory = decoder_factory
        self.decode_fn = decode_fn
        self.output_column = output_column
        self.max_open_videos = max_open_videos
        self.collate_fn = collate_fn
        self._decoders = OrderedDict()
        self._owner_pid = os.getpid()

    def __call__(self, rows):
        self._ensure_process_local_cache()
        single_row = isinstance(rows, Mapping)
        input_rows = [rows] if single_row else list(rows)
        decoded_rows = [self._decode_row(row) for row in input_rows]
        if single_row:
            return decoded_rows[0]
        return self._collate(decoded_rows)

    def close(self):
        while self._decoders:
            _, resource = self._decoders.popitem(last=False)
            self._close_resource(resource)

    def __getstate__(self):
        # DataLoader spawn workers must never inherit non-picklable decoder or
        # stream state opened in the parent process.
        state = self.__dict__.copy()
        state["_decoders"] = OrderedDict()
        state["_owner_pid"] = None
        return state

    def _decode_row(self, row):
        if not isinstance(row, Mapping):
            raise ValueError("VideoFrameCollator expects row dictionaries.")
        if self.video_column not in row:
            raise ValueError(
                "Video column %r is missing from the row." % self.video_column
            )

        raw = row[self.video_column]
        output = dict(row)
        if raw is None:
            output[self.output_column] = None
            return output
        if hasattr(raw, "as_py"):
            raw = raw.as_py()
        if not BlobDescriptor.is_blob_descriptor(raw):
            raise ValueError(
                "Video column %r must contain serialized BlobDescriptor bytes. "
                "Read the table with blob-as-descriptor=true."
                % self.video_column
            )

        serialized = bytes(raw)
        descriptor = BlobDescriptor.deserialize(serialized)
        if descriptor.serialize() != serialized:
            raise ValueError(
                "Video column %r must contain one exact serialized "
                "BlobDescriptor without trailing bytes." % self.video_column
            )
        decoder = self._decoder(descriptor)
        output[self.output_column] = self.decode_fn(decoder, output)
        return output

    def _decoder(self, descriptor):
        resource = self._decoders.pop(descriptor, None)
        if resource is not None:
            self._decoders[descriptor] = resource
            return resource[0]

        # Reuse the table's resolved FileIO. Rebuilding a reader from raw URI
        # options can drop merged REST/DLF storage credentials in workers.
        stream = Blob.from_file(
            self.file_io,
            descriptor.uri,
            descriptor.offset,
            descriptor.length,
        ).new_input_stream()
        try:
            decoder = self.decoder_factory(stream)
        except Exception:
            stream.close()
            raise
        resource = (decoder, stream)
        self._decoders[descriptor] = resource
        if len(self._decoders) > self.max_open_videos:
            _, evicted = self._decoders.popitem(last=False)
            self._close_resource(evicted)
        return decoder

    def _collate(self, rows):
        if self.collate_fn is not None:
            return self.collate_fn(rows)
        try:
            from torch.utils.data import default_collate
        except ImportError as error:
            raise ImportError(
                "VideoFrameCollator requires PyTorch for its default collate "
                "function; install pypaimon[torch] or pass collate_fn=."
            ) from error
        return default_collate(rows)

    def _ensure_process_local_cache(self):
        pid = os.getpid()
        if self._owner_pid == pid:
            return
        # A forked worker owns duplicate descriptors for any inherited file
        # handles. Closing them here affects only the worker's copies.
        self.close()
        self._owner_pid = pid

    @staticmethod
    def _close_resource(resource):
        decoder, stream = resource
        close = getattr(decoder, "close", None)
        try:
            if close is not None:
                close()
        finally:
            stream.close()
