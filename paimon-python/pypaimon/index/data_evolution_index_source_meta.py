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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Java DataEvolutionIndexSourceMeta stored in GlobalIndexMeta.source_meta."""

import struct
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DataEvolutionIndexSourceMeta:
    scan_snapshot_id: int
    source_schema_id: Optional[int] = None

    MAGIC = 0x44454958
    VERSION = 1
    VERSION_WITH_SCHEMA = 2
    _SNAPSHOT = struct.Struct('>iiq')
    _SNAPSHOT_AND_SCHEMA = struct.Struct('>iiqq')

    def __post_init__(self):
        if self.scan_snapshot_id <= 0:
            raise ValueError('Scan snapshot id must be positive.')
        if self.source_schema_id is not None and self.source_schema_id < 0:
            raise ValueError('Source schema id must be non-negative.')

    def serialize(self):
        if self.source_schema_id is None:
            return self._SNAPSHOT.pack(self.MAGIC, self.VERSION, self.scan_snapshot_id)
        return self._SNAPSHOT_AND_SCHEMA.pack(
            self.MAGIC, self.VERSION_WITH_SCHEMA, self.scan_snapshot_id, self.source_schema_id)

    @classmethod
    def is_data_evolution_meta(cls, data):
        return data is not None and len(data) >= 4 and data[:4] == b'DEIX'

    @classmethod
    def deserialize(cls, data):
        if not cls.is_data_evolution_meta(data) or len(data) < 8:
            raise ValueError('Invalid data-evolution index source metadata.')
        _, version = struct.unpack_from('>ii', data)
        if version == cls.VERSION:
            codec = cls._SNAPSHOT
        elif version == cls.VERSION_WITH_SCHEMA:
            codec = cls._SNAPSHOT_AND_SCHEMA
        else:
            raise ValueError('Unsupported data-evolution index source version: {}.'.format(version))
        if len(data) != codec.size:
            raise ValueError('Invalid data-evolution index source metadata.')
        return cls(*codec.unpack(data)[2:])
