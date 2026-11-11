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

"""Arrow transport for Rust's postpone bucket statistics and planning."""

import pyarrow as pa

from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.native.write.input_routing import input_batches
from pypaimon.execution.native.write.native_commit import create_native_write_table
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.write.bucket_plan import PostponeBucketPlan


def create_native_postpone_bucket_planner(table, known_num_buckets=None):
    if not native_runtime_available():
        return None
    native_table = create_native_write_table(table)
    if native_table is None:
        return None
    return NativePostponeBucketPlanner(table, known_num_buckets, native_table)


def _plan_dict(plan):
    # Both Core and Python use positional columns. Partition keys may be
    # called total_buckets, row_count or data_size.
    columns = [column.to_pylist() for column in plan.columns]
    return {tuple(column[row] for column in columns[:-1]): columns[-1][row]
            for row in range(plan.num_rows)}


class NativePostponeBucketPlanner:
    """Keep Python plans serializable while Rust evaluates all input rows."""

    def __init__(self, table, known_num_buckets=None, native_table=None):
        self.table = table
        self._planner = None
        # Preserve the attributes exposed by the existing public planner.
        options = table.options
        self.default_bucket_num = options.postpone_default_bucket_num()
        self.target_row_num_per_bucket = None
        self.target_size_per_bucket = None
        if self.default_bucket_num is None:
            self.max_num_buckets = options.postpone_batch_write_fixed_bucket_max_parallelism()
            self.target_row_num_per_bucket = options.postpone_target_row_num_per_bucket()
            if self.target_row_num_per_bucket is None:
                self.target_size_per_bucket = options.postpone_target_size_per_bucket()
        if native_table is None:
            native_table = create_native_write_table(table)
        if native_table is None:
            raise RuntimeError('Native postpone bucket planning is unavailable')
        if known_num_buckets is None:
            known_num_buckets = self._load_bucket_metadata(table, native_table)
        self._known_num_buckets = dict(known_num_buckets)
        self._prepare(native_table)

    def _prepare(self, native_table=None):
        from pypaimon_rust.datafusion import PostponeBucketPlanner
        if native_table is None:
            native_table = create_native_write_table(self.table)
        if native_table is None:
            raise RuntimeError('Native postpone bucket planning is unavailable')
        self._planner = PostponeBucketPlanner(
            native_table.schema(), PostponeBucketPlan(self._known_num_buckets).to_arrow(self.table))

    def _ensure_planner(self):
        if self._planner is None:
            self._prepare()
        return self._planner

    def current_plan(self):
        self._known_num_buckets = _plan_dict(self._ensure_planner().current_plan())
        return PostponeBucketPlan(self._known_num_buckets)

    def input_partition_stats(self, data):
        planner = self._ensure_planner()
        partition_stats = {}
        for batch in input_batches(data):
            stats = planner.input_statistics(batch)
            columns = [column.to_pylist() for column in stats.columns]
            for row in range(stats.num_rows):
                partition = tuple(column[row] for column in columns[:-2])
                count, size = partition_stats.get(partition, (0, 0))
                partition_stats[partition] = (count + columns[-2][row], size + columns[-1][row])
        return partition_stats

    def plan(self, partition_stats):
        fields = [self.table.field_dict[name] for name in self.table.partition_keys]
        schema = PyarrowFieldParser.from_paimon_schema(fields)
        schema = schema.append(pa.field('row_count', pa.int64(), nullable=False))
        schema = schema.append(pa.field('data_size', pa.int64(), nullable=False))
        partitions = list(partition_stats)
        if any(len(partition) != len(fields) for partition in partitions):
            raise ValueError('Input statistics partition arity does not match the table schema')
        columns = [pa.array([partition[i] for partition in partitions], type=field.type)
                   for i, field in enumerate(list(schema)[:-2])]
        columns.extend([
            pa.array([partition_stats[partition][i] for partition in partitions], type=pa.int64())
            for i in range(2)])
        plan = self._ensure_planner().plan(pa.RecordBatch.from_arrays(columns, schema=schema))
        self._known_num_buckets = _plan_dict(plan)
        return PostponeBucketPlan(self._known_num_buckets)

    def __getstate__(self):
        state = dict(self.__dict__)
        state['_planner'] = None
        return state

    @staticmethod
    def _load_bucket_metadata(table, native_table=None):
        # Writer restoration uses the catalog's write identity. Query plans
        # would add SELECT rules and can combine different physical layouts.
        if native_table is None:
            native_table = create_native_write_table(table)
        if native_table is None:
            raise RuntimeError('Native bucket metadata planning is unavailable')
        plan = native_table.new_postpone_fixed_bucket_write_builder().resolve_bucket_plan()
        # A partition key may itself be called total_buckets. Keep the
        # positional schema used by the Core plan and with_bucket_plan input.
        return _plan_dict(plan)
