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


import sys

if sys.version_info[:2] == (3, 6):
    try:
        import fastavro._read as fastavro_read
        import zstandard as zstd
        from io import BytesIO
        
        _original_zstandard_read_block = None
        if hasattr(fastavro_read, 'zstandard_read_block'):
            _original_zstandard_read_block = fastavro_read.zstandard_read_block
        
        def _fixed_zstandard_read_block(decoder):
            from fastavro._read import read_long
            length = read_long(decoder)
            data = decoder.read_fixed(length)
            
            try:
                decompressor = zstd.ZstdDecompressor()
                
                try:
                    decompressed = decompressor.decompress(data)
                    return BytesIO(decompressed)
                except zstd.ZstdError as e:
                    error_msg = str(e).lower()
                    if "could not determine content size" in error_msg or "content size" in error_msg:
                        dctx = zstd.ZstdDecompressor()
                        with dctx.stream_reader(BytesIO(data)) as reader:
                            decompressed = reader.read()
                            return BytesIO(decompressed)
                    else:
                        raise
            except (zstd.ZstdError, Exception) as e:
                raise zstd.ZstdError(f"Failed to decompress zstd data on Python 3.6: {e}") from e
        
        if hasattr(fastavro_read, 'zstandard_read_block'):
            fastavro_read.zstandard_read_block = _fixed_zstandard_read_block
    except (ImportError, AttributeError):
        pass

