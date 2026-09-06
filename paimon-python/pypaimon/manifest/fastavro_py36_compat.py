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

"""
Compatibility patch for fastavro on Python 3.6/3.7 (the python-zstandard
fastavro), for reading zstd-compressed Avro (manifest) files.

Those fastavro versions may fail on frames without a content-size header
(e.g. written by Java or backports.zstd) with:
  "zstd.ZstdError: could not determine content size in frame header"
This patches fastavro's zstd block reader to stream-decompress instead.
Python 3.8+ uses backports.zstd and needs no patch.
"""

import sys

_patch_applied = False


def _apply_zstd_patch():
    global _patch_applied
    if _patch_applied or sys.version_info[:2] >= (3, 8):
        return

    try:
        import fastavro._read as fastavro_read
        import zstandard as zstd
        from io import BytesIO
    except (ImportError, AttributeError):
        return

    def _fixed_zstandard_read_block(decoder):
        from fastavro._read import read_long
        
        length = read_long(decoder)
        
        if hasattr(decoder, 'read_fixed'):
            data = decoder.read_fixed(length)
        elif hasattr(decoder, 'read'):
            data = decoder.read(length)
        else:
            from fastavro._read import read_fixed
            data = read_fixed(decoder, length)

        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(BytesIO(data)) as reader:
            decompressed = reader.read()
            return BytesIO(decompressed)

    if hasattr(fastavro_read, 'BLOCK_READERS'):
        block_readers = fastavro_read.BLOCK_READERS
        block_readers['zstandard'] = _fixed_zstandard_read_block
        block_readers['zstd'] = _fixed_zstandard_read_block
        _patch_applied = True


if sys.version_info[:2] < (3, 8):
    try:
        _apply_zstd_patch()
    except ImportError:
        pass
