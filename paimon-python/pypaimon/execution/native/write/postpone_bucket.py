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

"""Read existing bucket layouts through Rust before distributed input work."""

from pypaimon.execution.native.read.plan import native_plan
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.write.bucket_planning import PostponeBucketPlanning


def create_native_postpone_bucket_planner(table, known_num_buckets=None):
    if not native_runtime_available():
        return None
    return NativePostponeBucketPlanner(table, known_num_buckets)


class NativePostponeBucketPlanner(PostponeBucketPlanning):
    """Use Rust planning metadata and shared input-only bucket arithmetic."""

    @staticmethod
    def _load_bucket_metadata(table):
        # PartitionStat does not carry total_buckets. Complete file planning
        # preserves that layout in the shared Split v1 metadata without ever
        # creating a Python scanner, reader or data writer.
        known = {}
        for split in native_plan(table, all_files=True).splits():
            if split.bucket < 0 or split.total_buckets is None or split.total_buckets <= 0:
                continue
            partition = tuple(split.partition.values)
            previous = known.get(partition)
            if previous is not None and previous != split.total_buckets:
                raise RuntimeError(
                    "Partition {} has different total buckets {} and {}"
                    .format(partition, previous, split.total_buckets))
            known[partition] = split.total_buckets
        return known
