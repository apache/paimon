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

from typing import Dict, Tuple

import pyarrow as pa


class PostponeBucketPlan:
    """Resolved bucket counts by partition."""

    def __init__(self, num_buckets: Dict[Tuple, int]):
        self._num_buckets = dict(num_buckets)

    def to_arrow(self, table) -> pa.RecordBatch:
        """Encode resolved per-partition bucket counts as Arrow transport."""
        from pypaimon.schema.data_types import PyarrowFieldParser

        fields = [table.field_dict[name] for name in table.partition_keys]
        schema = PyarrowFieldParser.from_paimon_schema(fields)
        schema = schema.append(pa.field('total_buckets', pa.int32(), nullable=False))
        partitions = list(self._num_buckets)
        counts = list(self._num_buckets.values())
        if any(len(partition) != len(fields) for partition in partitions):
            raise ValueError('Bucket plan partition arity does not match the table schema')
        if any(isinstance(count, bool) or not isinstance(count, int)
               or not 0 < count <= 2147483647 for count in counts):
            raise ValueError('Bucket plan total_buckets must be a positive 32-bit integer')
        # Use positional columns: a partition may itself be named total_buckets.
        columns = [pa.array([partition[i] for partition in partitions], type=field.type)
                   for i, field in enumerate(list(schema)[:-1])]
        columns.append(pa.array(counts, type=pa.int32()))
        return pa.RecordBatch.from_arrays(columns, schema=schema)

    def contains(self, partition: Tuple) -> bool:
        return tuple(partition) in self._num_buckets

    def num_buckets(self, partition: Tuple) -> int:
        partition = tuple(partition)
        if partition not in self._num_buckets:
            raise ValueError("Missing bucket plan for partition {}".format(partition))
        return self._num_buckets[partition]

    def as_dict(self) -> Dict[Tuple, int]:
        return dict(self._num_buckets)
