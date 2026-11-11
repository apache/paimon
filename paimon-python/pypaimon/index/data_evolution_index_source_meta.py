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


@dataclass(frozen=True)
class DataEvolutionIndexSourceMeta:
    scan_snapshot_id: int

    MAGIC = 0x44454958
    VERSION = 1

    def __post_init__(self):
        if self.scan_snapshot_id <= 0:
            raise ValueError('Scan snapshot id must be positive.')

    def serialize(self):
        return struct.pack('>iiq', self.MAGIC, self.VERSION, self.scan_snapshot_id)

    @classmethod
    def is_data_evolution_meta(cls, data):
        return data is not None and len(data) >= 4 and data[:4] == b'DEIX'

    @classmethod
    def deserialize(cls, data):
        if len(data) != 16 or not cls.is_data_evolution_meta(data):
            raise ValueError('Invalid data-evolution index source metadata.')
        _, version, snapshot_id = struct.unpack('>iiq', data)
        if version != cls.VERSION:
            raise ValueError('Unsupported data-evolution index source version: {}.'.format(version))
        return cls(snapshot_id)
