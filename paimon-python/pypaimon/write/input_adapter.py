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

import pyarrow as pa


def _pandas_write_schema(table, schema):
    """Keep ignored events available for the normal row-kind NULL checks."""
    if table is None or not table.is_primary_key_table:
        return schema
    from pypaimon.write.row_kind import write_value_schema
    schema = write_value_schema(table, schema)
    options = table.options
    raw = options.options.to_map()
    if (raw.get('rowkind.field') is None
            or not (options.ignore_delete() or str(raw.get('ignore-update-before', 'false')).lower() == 'true')):
        return schema
    # Pandas checks top-level NOT NULL before Arrow reaches the writer. Only
    # relax the transport; the declared schema and nested fields stay intact.
    return pa.schema([field.with_nullable(True) for field in schema], metadata=schema.metadata)


def _pandas_write_batch(table, dataframe, schema, table_schema):
    """Retain event/routing inputs omitted from the physical write type."""
    # Convert the selected fields before building a wider transport. PyArrow
    # validates missing columns and can resolve selected values from a named
    # DataFrame index; filling a full DataFrame layout first would erase both.
    batch = pa.RecordBatch.from_pandas(dataframe, schema=_pandas_write_schema(table, schema))
    if table is not None and schema.names != table_schema.names:
        from pypaimon.table.bucket_mode import BucketMode
        required = list(table.partition_keys)
        if table.bucket_mode() == BucketMode.HASH_FIXED and table.options.bucket() > 1:
            required.extend(table.table_schema.bucket_keys)
        row_kind_field = table.options.options.to_map().get('rowkind.field')
        if row_kind_field is not None:
            required.append(row_kind_field)
        if not set(required).issubset(schema.names):
            extra_schema = pa.schema([field.with_nullable(True) for field in table_schema
                                      if field.name in required and field.name not in schema.names])
            extra = pa.RecordBatch.from_pandas(dataframe, schema=extra_schema)
            columns = dict(zip(batch.schema.names, batch.columns))
            columns.update(zip(extra.schema.names, extra.columns))
            # Only written/event/routing fields were converted. Do not touch
            # unwritten payloads, including live BLOB objects.
            fields = [field if field.name in schema.names else field.with_nullable(True) for field in table_schema]
            arrays = [columns[field.name] if field.name in columns else pa.nulls(batch.num_rows, field.type)
                      for field in fields]
            batch = pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields, metadata=table_schema.metadata))
    return batch


def _write_ray(writer, dataset, overwrite=False, concurrency=None, ray_remote_args=None,
               hash_fixed_precluster='auto', static_partition=None):
    """Share Ray coordination without constructing another local data writer."""
    from pypaimon.write.ray_datasink import write_paimon_dataset

    overwrite_partition = writer.static_partition if static_partition is None else static_partition
    return write_paimon_dataset(
        dataset, writer.table, overwrite=overwrite, static_partition=overwrite_partition,
        concurrency=concurrency, ray_remote_args=ray_remote_args,
        hash_fixed_precluster=hash_fixed_precluster,
        **writer._distributed_write_options(overwrite=overwrite or overwrite_partition is not None))


def postpone_distributed_write_options(table, bucket_plan, overwrite):
    """Build a distributed bucket plan before worker writers receive data."""
    from pypaimon.execution.dispatch import create_postpone_bucket_planner
    return {'postpone_bucket_planner': create_postpone_bucket_planner(
        table, known_num_buckets=(bucket_plan.as_dict() if bucket_plan is not None
                                  else {} if overwrite else None))}
