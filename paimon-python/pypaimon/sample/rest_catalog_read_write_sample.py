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
Example: REST Catalog Read and Write

Demonstrates:
1. REST catalog basic read/write operations
2. Table paths include URI schemes (file://, oss://, s3://)
3. Paths with schemes can be used directly with FileIO
"""

import tempfile
import uuid

import pandas as pd
import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.tests.rest.rest_server import RESTCatalogServer
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider


def main():
    """REST catalog read/write example with path scheme demonstration."""

    # Setup mock REST server
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

    try:
        # Note: warehouse must match server's warehouse parameter for config endpoint
        catalog = CatalogFactory.create({
            'metastore': 'rest',
            'uri': f"http://localhost:{server.port}",
            'warehouse': "warehouse",  # Must match server's warehouse parameter
            'token.provider': 'bear',
            'token': token,
        })
        catalog.create_database("default", False)

        schema = Schema.from_pyarrow_schema(pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('value', pa.float64()),
        ]))
        table_name = 'default.example_table'
        catalog.create_table(table_name, schema, False)
        table = catalog.get_table(table_name)

        table_path = table.table_path
        print(f"\nTable path: {table_path}")

        print("\nWriting data...")
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.5, 20.3, 30.7]
        })
        table_write.write_pandas(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        print("Data written successfully!")

        # Read data
        print("\nReading data...")
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        result = table_read.to_pandas(splits)
        print(result)

    finally:
        server.shutdown()
        print("\nServer stopped")


if __name__ == '__main__':
    main()
