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

"""Snapshot-pinned distributed lookup of global-index search candidates."""

from copy import copy
import uuid

import pyarrow as pa

from pypaimon.table.special_fields import SpecialFields


def read_search_result(query, result, concurrency, remote_args, override_num_blocks):
    import ray

    query._require_row_id_scores()
    lookup = copy(query)
    lookup._table = query._table.copy({"blob-as-descriptor": "true"})
    projection = query._effective_projection()
    lookup._projection = (list(projection) if projection is not None
                          else [field.name for field in query._table.fields])
    if not lookup._projection or len(set(lookup._projection)) != len(lookup._projection):
        raise ValueError("Ray search requires a nonempty projection with unique column names.")
    row_id = SpecialFields.ROW_ID.name
    added_row_id = row_id not in lookup._projection
    if added_row_id:
        lookup._projection.append(row_id)
    lookup._limit = None  # Candidates already contain the global top-k.
    builder = lookup._configured_read_builder()
    reader = builder.new_read()
    if query._metadata_only_result():
        dataset = ray.data.from_arrow(pa.table({row_id: pa.array(list(result.results()), type=pa.int64())}))
    else:
        splits = builder.new_scan().with_global_index_result(result).plan().splits()
        dataset = reader.to_ray(
            splits, concurrency=concurrency, ray_remote_args=remote_args,
            override_num_blocks=override_num_blocks)

    # Only compact candidate metadata crosses the driver. Payloads remain in
    # worker blocks, including during score attachment and global ordering.
    rank_column = "_paimon_search_rank_" + uuid.uuid4().hex
    finish = copy(query)
    finish._sort_by_score = False
    sort_by_score = query._sort_by_score
    score_getter = (result.score_getter()
                    if sort_by_score and not result.results().is_empty() else None)

    def finish_batch(batch):
        output = finish._finish_search_result(batch, result, False)
        if sort_by_score:
            output = output.append_column(rank_column, pa.array(
                [-score_getter(value) for value in batch[row_id].to_pylist()], type=pa.float64()))
        return output

    empty = reader._output_arrow_schema().empty_table()
    dataset = dataset.map_batches(finish_batch, batch_format="pyarrow")
    empty = finish_batch(empty)
    if query._sort_by_score:
        dataset = dataset.sort([rank_column, row_id]).drop_columns([rank_column])
        empty = empty.drop_columns([rank_column])
    if added_row_id:
        dataset = dataset.drop_columns([row_id])
        empty = empty.drop_columns([row_id])
    # Sort/map may discard every empty block. Restore the final typed schema.
    dataset = dataset.union(ray.data.from_arrow(empty))
    if sort_by_score:
        # Projection tasks after Sort can finish out of order. Preserve the
        # sorted block order through the whole returned Dataset, without
        # changing the process-wide DataContext or unrelated Datasets.
        dataset.context.execution_options.preserve_order = True
    setattr(dataset, "_paimon_blob_file_io", lookup._table.file_io)
    setattr(dataset, "_paimon_blob_columns", query._readable_blob_columns())
    maps, arrays = query._nested_blob_columns()
    setattr(dataset, "_paimon_map_blob_columns", maps)
    setattr(dataset, "_paimon_array_blob_columns", arrays)
    return dataset
