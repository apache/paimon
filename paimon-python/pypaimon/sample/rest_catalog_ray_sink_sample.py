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
#  limitations under the License.
################################################################################
"""
Example: REST Catalog + Ray Sink with Local File + Mock Server

Demonstrates reading JSON data from local file using Ray Data and writing to Paimon table
using Ray Sink (write_ray) with a mock REST catalog server.
"""

import os
import tempfile
import uuid

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.tests.rest.rest_server import RESTCatalogServer
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider


def _get_sample_data_path(filename: str) -> str:
    sample_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(sample_dir, 'data', filename)


def main():
    ray.init(ignore_reinit_error=True, num_cpus=2)
    
    temp_dir = tempfile.mkdtemp()
    token = str(uuid.uuid4())
    server = RESTCatalogServer(
        data_path=temp_dir,
        auth_provider=BearTokenAuthProvider(token),
        config=ConfigResponse(defaults={"prefix": "mock-test"}),
        warehouse="warehouse"
    )
    server.start()
    print(f"REST server started at: {server.get_url()}")
    
    json_file_path = _get_sample_data_path('data.jsonl')
    
    table_write = None
    try:
        catalog = CatalogFactory.create({
            'metastore': 'rest',
            'uri': f"http://localhost:{server.port}",
            'warehouse': "warehouse",
            'token.provider': 'bear',
            'token': token,
        })
        catalog.create_database("default", ignore_if_exists=True)
        
        schema = Schema.from_pyarrow_schema(pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('category', pa.string()),
            ('value', pa.float64()),
            ('score', pa.int64()),
            ('timestamp', pa.timestamp('s')),
        ]), primary_keys=['id'], options={
            CoreOptions.BUCKET.key(): '4'
        })
        
        table_name = 'default.oss_json_import_table'
        catalog.create_table(table_name, schema, ignore_if_exists=True)
        table = catalog.get_table(table_name)
        
        print(f"Reading JSON from local file: {json_file_path}")
        ray_dataset = ray.data.read_json(
            json_file_path,
            concurrency=2,
        )
        
        print(f"Ray Dataset: {ray_dataset.count()} rows")
        print(f"Schema: {ray_dataset.schema()}")
        
        table_pa_schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
        
        def cast_batch_to_table_schema(batch: pa.RecordBatch) -> pa.Table:
            arrays = []
            for field in table_pa_schema:
                col_name = field.name
                col_array = batch.column(col_name)
                if isinstance(col_array, pa.ChunkedArray):
                    col_array = col_array.combine_chunks()
                if col_array.type != field.type:
                    col_array = col_array.cast(field.type)
                arrays.append(col_array)
            record_batch = pa.RecordBatch.from_arrays(arrays, schema=table_pa_schema)
            return pa.Table.from_batches([record_batch])
        
        ray_dataset = ray_dataset.map_batches(
            cast_batch_to_table_schema,
            batch_format="pyarrow",
        )
        
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        
        table_write.write_ray(
            ray_dataset,
            overwrite=False,
            concurrency=2,
            ray_remote_args={"num_cpus": 1}
        )
        
        # write_ray() has already committed the data, just close the writer
        table_write.close()
        table_write = None
        
        print(f"Successfully wrote {ray_dataset.count()} rows to table")
        
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()
        
        result_df = table_read.to_pandas(splits)
        print(f"Read back {len(result_df)} rows from table")
        print(result_df.head())
        
    finally:
        if table_write is not None:
            try:
                table_write.close()
            except Exception:
                pass
        server.shutdown()
        if ray.is_initialized():
            ray.shutdown()


if __name__ == '__main__':
    main()
