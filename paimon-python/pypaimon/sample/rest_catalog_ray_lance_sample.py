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
Example: REST Catalog + Ray Data + Lance Format Integration

Demonstrates:
1. REST catalog with Lance file format
2. Writing data to Paimon table using Lance format
3. Reading data using Ray Data for distributed processing
4. Integration of REST catalog, Ray Data, and Lance format
"""

import tempfile
import uuid

import pandas as pd
import pyarrow as pa
import ray


from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions

from pypaimon.tests.rest.rest_server import RESTCatalogServer
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider


def main():
    """REST catalog + Ray Data + Lance format integration example."""

    # Initialize Ray
    ray.init(ignore_reinit_error=True)
    print("Ray initialized successfully")

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
        # Create REST catalog
        catalog = CatalogFactory.create({
            'metastore': 'rest',
            'uri': f"http://localhost:{server.port}",
            'warehouse': "warehouse",  # Must match server's warehouse parameter
            'token.provider': 'bear',
            'token': token,
        })
        catalog.create_database("default", True)

        # Create table schema with Lance format
        pa_schema = pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('value', pa.float64()),
            ('category', pa.string()),
        ])

        # Create schema with Lance file format option
        schema = Schema.from_pyarrow_schema(
            pa_schema=pa_schema,
            primary_keys=['id'],
            options={
                'bucket': '2',
                CoreOptions.FILE_FORMAT.key(): CoreOptions.FILE_FORMAT_LANCE,
            }
        )

        table_name = 'default.ray_lance_example'
        catalog.create_table(table_name, schema, False)
        table = catalog.get_table(table_name)

        table_path = table.table_path
        print(f"\nTable path: {table_path}")
        print(f"File format: {CoreOptions.FILE_FORMAT_LANCE}")

        # Write data using Lance format
        print("\nWriting data with Lance format...")
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        # Write multiple batches to demonstrate Lance format
        data1 = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [10.5, 20.3, 30.7, 40.1, 50.9],
            'category': ['A', 'B', 'A', 'B', 'A'],
        })
        table_write.write_pandas(data1)

        data2 = pd.DataFrame({
            'id': [6, 7, 8],
            'name': ['Frank', 'Grace', 'Henry'],
            'value': [60.2, 70.4, 80.6],
            'category': ['B', 'A', 'B'],
        })
        table_write.write_pandas(data2)

        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        print("Data written successfully with Lance format!")

        # Read data using standard methods
        print("\nReading data using standard methods...")
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        # Read to Pandas
        result_pandas = table_read.to_pandas(splits)
        print("\nPandas DataFrame result:")
        print(result_pandas)

        # Read to Arrow
        result_arrow = table_read.to_arrow(splits)
        print(f"\nArrow Table result: {result_arrow.num_rows} rows")

        # Read using Ray Data
        print("\nReading data using Ray Data for distributed processing...")
        ray_dataset = table_read.to_ray(splits)

        print(f"Ray Dataset: {ray_dataset}")
        print(f"Number of rows: {ray_dataset.count()}")

        # Sample Ray Data operations
        print("\nRay Data operations:")

        # Take first few rows
        sample_data = ray_dataset.take(3)
        print(f"First 3 rows: {sample_data}")

        # Filter data
        filtered_dataset = ray_dataset.filter(lambda row: row['value'] > 30.0)
        print(f"Filtered rows (value > 30): {filtered_dataset.count()}")

        # Map operation
        def double_value(row):
            row['value'] = row['value'] * 2
            return row

        mapped_dataset = ray_dataset.map(double_value)
        print(f"Mapped dataset (doubled values): {mapped_dataset.count()} rows")

        # Convert to Pandas
        ray_pandas = ray_dataset.to_pandas()
        print("\nRay Dataset converted to Pandas:")
        print(ray_pandas)

        # Group by category
        print("\nGrouping by category using Ray Data:")
        grouped = ray_dataset.groupby("category").sum("value")
        print(grouped)

        # Demonstrate predicate pushdown with Lance format
        print("\nDemonstrating predicate pushdown with Lance format...")
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.greater_than('value', 30.0)
        read_builder_filtered = read_builder.with_filter(predicate)

        table_scan_filtered = read_builder_filtered.new_scan()
        table_read_filtered = read_builder_filtered.new_read()
        splits_filtered = table_scan_filtered.plan().splits()

        result_filtered = table_read_filtered.to_pandas(splits_filtered)
        print("\nFiltered result (value > 30):")
        print(result_filtered)

        # Demonstrate projection with Lance format
        print("\nDemonstrating column projection with Lance format...")
        read_builder_projected = read_builder.with_projection(['id', 'name', 'value'])
        table_scan_projected = read_builder_projected.new_scan()
        table_read_projected = read_builder_projected.new_read()
        splits_projected = table_scan_projected.plan().splits()

        result_projected = table_read_projected.to_pandas(splits_projected)
        print("\nProjected result (id, name, value only):")
        print(result_projected)

    finally:
        server.shutdown()
        ray.shutdown()
        print("\nServer stopped and Ray shutdown")


if __name__ == '__main__':
    main()
