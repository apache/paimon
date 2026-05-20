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
from typing import BinaryIO, Optional

import pyarrow as pa

from pypaimon.common.uri_reader import UriReaderFactory
from pypaimon.table.row.blob import Blob, BlobDescriptor


def open_blob_descriptor_stream(raw_value: bytes, catalog_options: Optional[dict] = None) -> BinaryIO:
    if BlobDescriptor.is_blob_descriptor(raw_value):
        descriptor = BlobDescriptor.deserialize(raw_value)
        uri_reader_factory = UriReaderFactory(catalog_options or {})
        uri_reader = uri_reader_factory.create(descriptor.uri)
        return Blob.from_descriptor(uri_reader, descriptor).new_input_stream()
    return io.BytesIO(raw_value)


def _decode_first_frame(stream: BinaryIO, image_format: str) -> Optional[bytes]:
    try:
        import av
    except ImportError as e:
        raise ImportError("PyAV is required to decode video first frames") from e

    with av.open(stream, mode="r") as container:
        if not container.streams.video:
            return None
        for frame in container.decode(video=0):
            image = frame.to_image()
            output = io.BytesIO()
            image.save(output, format=image_format)
            return output.getvalue()
    return None


def make_first_frame(catalog_options: Optional[dict] = None, image_format: str = "JPEG"):
    image_format = image_format.upper()

    def first_frame(values):
        frames = []
        for raw_value in values.to_pylist():
            if raw_value is None:
                frames.append(None)
                continue

            try:
                with open_blob_descriptor_stream(raw_value, catalog_options) as stream:
                    frames.append(_decode_first_frame(stream, image_format))
            except ImportError:
                raise
            except Exception:
                frames.append(None)

        return pa.array(frames, type=pa.binary())

    return first_frame
