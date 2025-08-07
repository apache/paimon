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
from typing import List

import pyarrow as pa

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_write import FileStoreWrite


class BatchTableWrite:
    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.table_pyarrow_schema = PyarrowFieldParser.from_paimon_schema(self.table.table_schema.fields)
        self.file_store_write = FileStoreWrite(self.table)
        self.row_key_extractor = self.table.create_row_key_extractor()
        self.batch_committed = False

    def write_arrow(self, table: pa.Table, row_kind: List[int] = None):
        # TODO: support row_kind
        batches_iterator = table.to_batches()
        for batch in batches_iterator:
            self.write_arrow_batch(batch)

    def write_arrow_batch(self, data: pa.RecordBatch, row_kind: List[int] = None):
        # TODO: support row_kind
        self._validate_pyarrow_schema(data.schema)
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
        record_batch = pa.RecordBatch.from_pandas(dataframe, schema=pa_schema)
        return self.write_arrow_batch(record_batch)

    def prepare_commit(self) -> List[CommitMessage]:
        if self.batch_committed:
            raise RuntimeError("BatchTableWrite only supports one-time committing.")
        self.batch_committed = True
        return self.file_store_write.prepare_commit()

    def close(self):
        self.file_store_write.close()

    def _validate_pyarrow_schema(self, data_schema):
        if data_schema != self.table_pyarrow_schema:
            raise ValueError(f"Input schema isn't consistent with table schema. "
                             f"Table schema is: {data_schema} Input schema is: {self.table_pyarrow_schema}")
