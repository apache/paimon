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

"""Shared test helper that builds a FileIndexFormat blob with pypaimon's own
BloomFilter64.

Used by the pure-Python unit/integration tests to exercise the scanner wiring
and the evaluator without a checked-in fixture. Cross-language byte
compatibility with the Java writer is covered by the e2e test
(JavaPyE2ETest#testBloomFilterIndexWrite), not by this builder.

Keeping the FileIndexFormat framing in one place means a format change touches a
single helper rather than several hand-rolled copies.
"""

import struct

from pypaimon.fileindex.bloom_filter import BloomFilter64
from pypaimon.fileindex.fast_hash import get_hash_function
from pypaimon.fileindex.file_index_format import MAGIC, VERSION_1


def build_bloom_blob(column, type_str, values, items=100, fpp=0.1):
    """Build a complete FileIndexFormat blob carrying a single bloom-filter index.

    Args:
        column: column name to register the index under.
        type_str: the column's atomic type string (e.g. "BIGINT", "VARCHAR(10)").
        values: values to insert into the bloom filter (hashed with the same
            routine the read path uses).
        items, fpp: bloom sizing parameters.

    Returns:
        bytes: magic + header + serialized BloomFilter64 body, in the exact
        layout FileIndexFormatReader expects.
    """
    from pypaimon.schema.data_types import AtomicType

    hash_fn = get_hash_function(AtomicType(type_str))
    bf = BloomFilter64(items, fpp)
    for v in values:
        bf.add_hash(hash_fn(v))
    body = struct.pack(">I", bf.get_num_hash_functions()) + bytes(bf.get_bitset().data)

    def w_utf(s):
        b = s.encode("utf-8")
        return struct.pack(">H", len(b)) + b

    idx = "bloom-filter"
    # head_length = magic(8) + version(4) + headLength(4) + body-info section.
    head_length = 8 + 4 + 4 + (
        4 + len(w_utf(column)) + 4 + len(w_utf(idx)) + 4 + 4 + 4
    )
    head = (
        struct.pack(">I", 1)                  # column count
        + w_utf(column)
        + struct.pack(">I", 1)                # index count for the column
        + w_utf(idx)
        + struct.pack(">i", head_length)      # start pos (relative 0 + head_length)
        + struct.pack(">i", len(body))        # length
        + struct.pack(">I", 0)                # redundant length
    )
    return (
        struct.pack(">Q", MAGIC)
        + struct.pack(">I", VERSION_1)
        + struct.pack(">I", head_length)
        + head
        + body
    )
