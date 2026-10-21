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

"""Evaluate MERGE conditions with the Python DataFusion engine."""

import pyarrow as pa


def filter_batch(
    batch: pa.Table, condition: str, _pre_rewritten: bool = False,
) -> pa.Table:
    if batch.num_rows == 0:
        return batch
    from pypaimon.write.merge_condition import _load_datafusion, rewrite_condition

    datafusion = _load_datafusion()
    rewritten = condition if _pre_rewritten else rewrite_condition(condition)
    config = datafusion.SessionConfig().set(
        "datafusion.optimizer.enable_round_robin_repartition", "false"
    )
    ctx = datafusion.SessionContext(config)
    input_batches = batch.to_batches()
    # Use one batch per partition and rebuild from partitioned batches so
    # DataFusion neither concatenates 32-bit offsets nor reorders the input.
    ctx.register_record_batches(
        "_batch", [[record_batch] for record_batch in input_batches]
    )
    result = ctx.sql(
        f'SELECT * FROM _batch WHERE {rewritten}'
    )
    output_batches = [
        record_batch
        for partition in result.collect_partitioned()
        for record_batch in partition
    ]
    if not output_batches:
        return batch.schema.empty_table()
    return pa.Table.from_batches(output_batches)


def apply_condition(
    batch: pa.Table, rewritten: str, empty_schema: pa.Schema,
) -> pa.Table:
    batch = filter_batch(batch, rewritten, _pre_rewritten=True)
    if batch.num_rows == 0:
        return empty_schema.empty_table()
    return batch
