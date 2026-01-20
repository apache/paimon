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
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.types as pa_types

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_write import FileStoreWrite

if TYPE_CHECKING:
    from ray.data import Dataset


def _is_binary_type_compatible(input_type: pa.DataType, table_type: pa.DataType) -> bool:
    if pa_types.is_binary(input_type) and pa_types.is_large_binary(table_type):
        return True
    if pa_types.is_large_binary(input_type) and pa_types.is_binary(table_type):
        return True
    return False


class TableWrite:
    def __init__(self, table, commit_user):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.table_pyarrow_schema = PyarrowFieldParser.from_paimon_schema(self.table.table_schema.fields)
        self.file_store_write = FileStoreWrite(self.table, commit_user)
        self.row_key_extractor = self.table.create_row_key_extractor()
        self.commit_user = commit_user

    def write_arrow(self, table: pa.Table):
        batches_iterator = table.to_batches()
        for batch in batches_iterator:
            self.write_arrow_batch(batch)

    def _convert_binary_types(self, data: pa.RecordBatch) -> pa.RecordBatch:
        write_cols = self.file_store_write.write_cols
        table_schema = self.table_pyarrow_schema
        
        converted_arrays = []
        needs_conversion = False
        
        for i, field in enumerate(data.schema):
            array = data.column(i)
            expected_type = None
            
            if write_cols is None or field.name in write_cols:
                try:
                    expected_type = table_schema.field(field.name).type
                except KeyError:
                    pass
            
            if expected_type and field.type != expected_type and _is_binary_type_compatible(field.type, expected_type):
                try:
                    array = pa.compute.cast(array, expected_type)
                    needs_conversion = True
                except (pa.ArrowInvalid, pa.ArrowCapacityError, ValueError) as e:
                    direction = f"{field.type} to {expected_type}"
                    raise ValueError(
                        f"Failed to convert field '{field.name}' from {direction}. "
                        f"If converting to binary(), ensure no value exceeds 2GB limit: {e}"
                    ) from e
            
            converted_arrays.append(array)
        
        if needs_conversion:
            new_fields = [pa.field(field.name, arr.type, nullable=field.nullable)
                          for field, arr in zip(data.schema, converted_arrays)]
            return pa.RecordBatch.from_arrays(converted_arrays, schema=pa.schema(new_fields))
        
        return data

    def write_arrow_batch(self, data: pa.RecordBatch):
        self._validate_pyarrow_schema(data.schema)
        data = self._convert_binary_types(data)
        partitions, buckets = self.row_key_extractor.extract_partition_bucket_batch(data)

        partition_bucket_groups = defaultdict(list)
        for i in range(data.num_rows):
            partition_bucket_groups[(tuple(partitions[i]), buckets[i])].append(i)

        for (partition, bucket), row_indices in partition_bucket_groups.items():
            indices_array = pa.array(row_indices, type=pa.int64())
            sub_table = pa.compute.take(data, indices_array)
            self.file_store_write.write(partition, bucket, sub_table)

    def write_pandas(self, dataframe):
        pa_schema = PyarrowFieldParser.from_paimon_schema(self.table.table_schema.fields)
        record_batch = pa.RecordBatch.from_pandas(dataframe, schema=pa_schema, preserve_index=False)
        return self.write_arrow_batch(record_batch)

    def with_write_type(self, write_cols: List[str]):
        for col in write_cols:
            if col not in self.table_pyarrow_schema.names:
                raise ValueError(f"Column {col} is not in table schema.")
        if len(write_cols) == len(self.table_pyarrow_schema.names):
            write_cols = None
        self.file_store_write.write_cols = write_cols
        return self

    def write_ray(
        self,
        dataset: "Dataset",
        overwrite: bool = False,
        concurrency: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Write a Ray Dataset to Paimon table.
        
        .. note::
            Ray Data converts ``large_binary()`` to ``binary()`` when reading.
            This method automatically converts ``binary()`` back to ``large_binary()``
            to match the table schema.
        
        Args:
            dataset: Ray Dataset to write. This is a distributed data collection
                from Ray Data (ray.data.Dataset).
            overwrite: Whether to overwrite existing data. Defaults to False.
            concurrency: Optional max number of Ray tasks to run concurrently.
                By default, dynamically decided based on available resources.
            ray_remote_args: Optional kwargs passed to :func:`ray.remote` in write tasks.
                For example, ``{"num_cpus": 2, "max_retries": 3}``.
        """
        from pypaimon.write.ray_datasink import PaimonDatasink
        datasink = PaimonDatasink(self.table, overwrite=overwrite)
        dataset.write_datasink(
            datasink,
            concurrency=concurrency,
            ray_remote_args=ray_remote_args,
        )

    def close(self):
        self.file_store_write.close()

    def _validate_pyarrow_schema(self, data_schema: pa.Schema):
        write_cols = self.file_store_write.write_cols
        
        if write_cols is None:
            if data_schema.names != self.table_pyarrow_schema.names:
                raise ValueError(
                    f"Input schema doesn't match table schema. "
                    f"Field names and order must exactly match.\n"
                    f"Input schema: {data_schema}\n"
                    f"Table schema: {self.table_pyarrow_schema}"
                )
            for input_field, table_field in zip(data_schema, self.table_pyarrow_schema):
                if input_field.type != table_field.type:
                    if not _is_binary_type_compatible(input_field.type, table_field.type):
                        raise ValueError(
                            f"Input schema doesn't match table schema. "
                            f"Field '{input_field.name}' type mismatch.\n"
                            f"Input type: {input_field.type}\n"
                            f"Table type: {table_field.type}\n"
                            f"Input schema: {data_schema}\n"
                            f"Table schema: {self.table_pyarrow_schema}"
                        )
        else:
            if list(data_schema.names) != write_cols:
                raise ValueError(
                    f"Input schema field names don't match write_cols. "
                    f"Field names and order must match write_cols.\n"
                    f"Input schema names: {list(data_schema.names)}\n"
                    f"Write cols: {write_cols}"
                )
            table_field_map = {field.name: field for field in self.table_pyarrow_schema}
            for field_name in write_cols:
                if field_name not in table_field_map:
                    raise ValueError(
                        f"Field '{field_name}' in write_cols is not in table schema."
                    )
                input_field = data_schema.field(field_name)
                table_field = table_field_map[field_name]
                if input_field.type != table_field.type:
                    if not _is_binary_type_compatible(input_field.type, table_field.type):
                        raise ValueError(
                            f"Field '{field_name}' type mismatch.\n"
                            f"Input type: {input_field.type}\n"
                            f"Table type: {table_field.type}"
                        )


class BatchTableWrite(TableWrite):
    def __init__(self, table, commit_user):
        super().__init__(table, commit_user)
        self.batch_committed = False

    def prepare_commit(self) -> List[CommitMessage]:
        if self.batch_committed:
            raise RuntimeError("BatchTableWrite only supports one-time committing.")
        self.batch_committed = True
        return self.file_store_write.prepare_commit(BATCH_COMMIT_IDENTIFIER)


class StreamTableWrite(TableWrite):

    def prepare_commit(self, commit_identifier) -> List[CommitMessage]:
        return self.file_store_write.prepare_commit(commit_identifier)
