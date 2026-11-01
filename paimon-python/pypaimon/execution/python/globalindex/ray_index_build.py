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

"""Schedule the selected Python vector-index builder on Ray workers."""


def build_vector_index(builder, splits, unindexed_ranges, index_field, table_read,
                       index_path, concurrency, remote_args):
    import ray
    from pypaimon.execution.python.globalindex.build_plan import split_by_global_index_shard
    from pypaimon.execution.python.globalindex.index_file_utils import new_global_index_file_name

    shards = split_by_global_index_shard(
        splits, builder._core_options.global_index_row_count_per_shard(), unindexed_ranges)
    # Allocate names before dispatch, so failed/lost result delivery cannot hide
    # files from cleanup. Workers never publish index manifests or snapshots.
    names = [new_global_index_file_name("vindex-" + builder._index_type) for _ in shards]
    context = ray.put((builder, index_field, table_read, index_path))
    remote = ray.remote(_build_shard).options(**remote_args)
    pending = {}
    results = {}
    next_shard = 0
    try:
        while pending or next_shard < len(shards):
            while len(pending) < concurrency and next_shard < len(shards):
                ref = remote.remote(context, shards[next_shard], names[next_shard])
                pending[ref] = next_shard
                next_shard += 1
            ready, _ = ray.wait(list(pending), num_returns=1)
            ref = ready[0]
            results[pending.pop(ref)] = ray.get(ref)
        return [results[i] for i in range(len(shards)) if results[i] is not None]
    except BaseException:
        # Do not delete a file while another worker may still be writing it.
        # Stop submissions and drain every dispatched task before cleanup.
        for ref in pending:
            try:
                ray.get(ref)
            except BaseException:
                pass
        for name in names:
            builder._table.file_io.delete_quietly(index_path.rstrip("/") + "/" + name)
        raise


def _build_shard(context, shard, file_name):
    builder, index_field, table_read, index_path = context
    index_split, index_range = shard
    return builder._build_generic_shard(
        index_split, index_range, index_field, table_read, index_path, file_name=file_name)
