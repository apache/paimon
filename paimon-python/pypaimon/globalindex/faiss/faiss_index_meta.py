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
# limitations under the License.
################################################################################

"""Metadata for FAISS vector index."""

import struct
from dataclasses import dataclass
from io import BytesIO


@dataclass
class FaissIndexMeta:
    """
    Metadata for FAISS vector index.
    """
    VERSION = 1
    dim: int
    metric_value: int
    index_type_ordinal: int
    num_vectors: int
    min_id: int
    max_id: int

    def serialize(self) -> bytes:
        """Serialize metadata to byte array."""
        buffer = BytesIO()
        # Write version (4 bytes, big-endian)
        buffer.write(struct.pack('>i', self.VERSION))
        # Write dim (4 bytes)
        buffer.write(struct.pack('>i', self.dim))
        # Write metric_value (4 bytes)
        buffer.write(struct.pack('>i', self.metric_value))
        # Write index_type_ordinal (4 bytes)
        buffer.write(struct.pack('>i', self.index_type_ordinal))
        # Write num_vectors (8 bytes)
        buffer.write(struct.pack('>q', self.num_vectors))
        # Write min_id (8 bytes)
        buffer.write(struct.pack('>q', self.min_id))
        # Write max_id (8 bytes)
        buffer.write(struct.pack('>q', self.max_id))
        return buffer.getvalue()

    @classmethod
    def deserialize(cls, data: bytes) -> 'FaissIndexMeta':
        """Deserialize metadata from byte array."""
        buffer = BytesIO(data)

        # Read version
        version = struct.unpack('>i', buffer.read(4))[0]
        if version != cls.VERSION:
            raise ValueError(f"Unsupported FAISS index meta version: {version}")

        # Read fields
        dim = struct.unpack('>i', buffer.read(4))[0]
        metric_value = struct.unpack('>i', buffer.read(4))[0]
        index_type_ordinal = struct.unpack('>i', buffer.read(4))[0]
        num_vectors = struct.unpack('>q', buffer.read(8))[0]
        min_id = struct.unpack('>q', buffer.read(8))[0]
        max_id = struct.unpack('>q', buffer.read(8))[0]

        return cls(
            dim=dim,
            metric_value=metric_value,
            index_type_ordinal=index_type_ordinal,
            num_vectors=num_vectors,
            min_id=min_id,
            max_id=max_id
        )
