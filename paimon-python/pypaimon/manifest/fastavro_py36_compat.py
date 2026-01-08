################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

"""
 Provides compatibility patches for fastavro on Python 3.6,
specifically for handling zstd-compressed Avro files.

The main issue addressed is:
- On Python 3.6, fastavro's zstd decompression may fail with:
  "zstd.ZstdError: could not determine content size in frame header"
  
This module patches fastavro's zstd handling to use a more compatible
decompression method that works on Python 3.6.
"""


import sys

_patch_applied = False


def _apply_zstd_patch():
    global _patch_applied
    if _patch_applied:
        return

    if sys.version_info[:2] != (3, 6):
        return

    try:
        import fastavro
        import zstandard as zstd
        from io import BytesIO

        try:
            import fastavro._read as fastavro_read
        except (ImportError, AttributeError):
            try:
                fastavro_read = fastavro._read
            except (AttributeError, ImportError):
                return

        def _fixed_zstandard_read_block(decoder):
            from fastavro._read import read_long
            length = read_long(decoder)
            
            try:
                if hasattr(decoder, 'read_fixed'):
                    data = decoder.read_fixed(length)
                elif hasattr(decoder, 'read'):
                    data = decoder.read(length)
                else:
                    from fastavro._read import read_fixed
                    data = read_fixed(decoder, length)
            except (TypeError, AttributeError):
                if hasattr(decoder, 'read'):
                    data = decoder.read(length)
                else:
                    raise

            decompressor = zstd.ZstdDecompressor()
            with decompressor.stream_reader(BytesIO(data)) as reader:
                decompressed = reader.read()
                return BytesIO(decompressed)

        try:
            if hasattr(fastavro_read, 'BLOCK_READERS'):
                block_readers = fastavro_read.BLOCK_READERS

                block_readers['zstandard'] = _fixed_zstandard_read_block
                block_readers['zstd'] = _fixed_zstandard_read_block
                
                try:
                    if hasattr(fastavro_read, '__dict__'):
                        fastavro_read.__dict__['zstandard_read_block'] = _fixed_zstandard_read_block
                except (TypeError, AttributeError):
                    pass
                
                if block_readers.get('zstandard') is _fixed_zstandard_read_block:
                    _patch_applied = True
        except (TypeError, AttributeError) as e:
            pass

        if not _patch_applied:
            try:
                setattr(fastavro_read, 'zstandard_read_block', _fixed_zstandard_read_block)
                if hasattr(fastavro, '_read') and hasattr(fastavro._read, 'zstandard_read_block'):
                    setattr(fastavro._read, 'zstandard_read_block', _fixed_zstandard_read_block)
                _patch_applied = True
            except (TypeError, AttributeError):
                pass

        if not _patch_applied:
            try:
                import fastavro._read
                if hasattr(fastavro._read, '__dict__'):
                    fastavro._read.__dict__['zstandard_read_block'] = _fixed_zstandard_read_block
                else:
                    fastavro._read.zstandard_read_block = _fixed_zstandard_read_block
                _patch_applied = True
            except (TypeError, AttributeError):
                pass

    except (ImportError, AttributeError):
        pass


if sys.version_info[:2] == (3, 6):
    try:
        import fastavro
        _apply_zstd_patch()
        try:
            import fastavro._read as fastavro_read
            if hasattr(fastavro_read, 'BLOCK_READERS'):
                block_readers = fastavro_read.BLOCK_READERS
                if 'zstandard' in block_readers or 'zstd' in block_readers:
                    pass
        except (ImportError, AttributeError):
            pass
    except ImportError:
        pass
