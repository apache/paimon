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

from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.native.write.native_commit import create_native_write_table
from pypaimon.write.bucket_planning import PostponeBucketPlanning


def create_native_postpone_bucket_planner(table, known_num_buckets=None):
    if not native_runtime_available():
        return None
    return NativePostponeBucketPlanner(table, known_num_buckets)


class NativePostponeBucketPlanner(PostponeBucketPlanning):
    """Use Rust planning metadata and shared input-only bucket arithmetic."""

    @staticmethod
    def _load_bucket_metadata(table):
        # Writer restoration uses the catalog's write identity. Query plans
        # would add SELECT rules and can combine different physical layouts.
        native_table = create_native_write_table(table)
        if native_table is None:
            raise RuntimeError('Native bucket metadata planning is unavailable')
        plan = native_table.new_postpone_fixed_bucket_write_builder().bucket_plan()
        # A partition key may itself be called total_buckets. Keep the
        # positional schema used by the Core plan and with_bucket_plan input.
        columns = [column.to_pylist() for column in plan.columns]
        return {tuple(column[row] for column in columns[:-1]): columns[-1][row]
                for row in range(plan.num_rows)}
