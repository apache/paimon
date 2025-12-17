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
Example: REST Catalog with Ray Data Integration

Demonstrates:
1. REST catalog basic read/write operations
2. Reading Paimon tables into Ray Dataset for distributed processing
3. Using Ray Data operations (map, filter, etc.) on Paimon data
4. Performance comparison between simple and distributed reads

Prerequisites:
    pip install pypaimon ray pandas pyarrow
"""

import tempfile
import uuid

import pandas as pd
import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.tests.rest.rest_server import RESTCatalogServer
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider


def main():
    """REST catalog with Ray Data integration example."""

    # Initialize Ray
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True, num_cpus=2)
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

        # Create table schema
        schema = Schema.from_pyarrow_schema(pa.schema([
            ('id', pa.int64()),
            ('name', pa.string()),
            ('category', pa.string()),
            ('value', pa.float64()),
            ('score', pa.int32()),
        ]))
        table_name = 'default.ray_example_table'
        catalog.create_table(table_name, schema, True)
        table = catalog.get_table(table_name)

        print(f"\nTable created: {table_name}")
        print(f"Table path: {table.table_path}")

        # Write data
        print("\n" + "="*60)
        print("Step 1: Writing data to Paimon table")
        print("="*60)
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        # Generate sample data
        data = pd.DataFrame({
            'id': list(range(1, 21)),  # 20 rows
            'name': [f'Item_{i}' for i in range(1, 21)],
            'category': ['A', 'B', 'C'] * 6 + ['A', 'B'],
            'value': [10.5 + i * 2.3 for i in range(20)],
            'score': [50 + i * 5 for i in range(20)],
        })
        table_write.write_pandas(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()
        print(f"✓ Successfully wrote {len(data)} rows to table")

        # Read with Ray Data (distributed mode)
        print("\n" + "="*60)
        print("Step 2: Reading data with Ray Data (distributed mode)")
        print("="*60)
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()

        print(f"Number of splits: {len(splits)}")

        # Convert to Ray Dataset
        ray_dataset = table_read.to_ray(splits, parallelism=2)
        print("✓ Ray Dataset created successfully")
        print(f"  - Total rows: {ray_dataset.count()}")
        # Note: num_blocks() requires materialized dataset, so we skip it for simplicity

        # Convert to pandas for display
        df_ray = ray_dataset.to_pandas()
        print("\nFirst 5 rows from Ray Dataset:")
        print(df_ray.head().to_string())

        # Compare with simple read
        print("\n" + "="*60)
        print("Step 3: Comparison with simple read mode")
        print("="*60)
        ray_dataset_simple = table_read.to_ray(splits, parallelism=1)
        df_simple = ray_dataset_simple.to_pandas()

        print(f"Distributed mode rows: {ray_dataset.count()}")
        print(f"Simple mode rows: {ray_dataset_simple.count()}")
        print("✓ Both modes return the same number of rows")

        # Verify data consistency (sort by id for comparison)
        df_ray_sorted = df_ray.sort_values(by='id').reset_index(drop=True)
        df_simple_sorted = df_simple.sort_values(by='id').reset_index(drop=True)
        pd.testing.assert_frame_equal(df_ray_sorted, df_simple_sorted)
        print("✓ Data content matches between distributed and simple modes")

        # Ray Data operations
        print("\n" + "="*60)
        print("Step 4: Ray Data operations on Paimon data")
        print("="*60)

        # Filter operation
        print("\n4.1 Filter: Get items with score >= 80")
        filtered_dataset = ray_dataset.filter(lambda row: row['score'] >= 80)
        df_filtered = filtered_dataset.to_pandas()
        print(f"  - Filtered rows: {len(df_filtered)}")
        print(f"  - IDs: {sorted(df_filtered['id'].tolist())}")

        # Map operation
        print("\n4.2 Map: Double the value column")

        def double_value(row):
            row['value'] = row['value'] * 2
            return row

        mapped_dataset = ray_dataset.map(double_value)
        df_mapped = mapped_dataset.to_pandas()
        print(f"  - Original first value: {df_ray.iloc[0]['value']:.2f}")
        print(f"  - Mapped first value: {df_mapped.iloc[0]['value']:.2f}")

        # Group by operation (using pandas after conversion)
        print("\n4.3 Group by: Aggregate by category")
        df_grouped = df_ray.groupby('category').agg({
            'value': 'sum',
            'score': 'mean',
            'id': 'count'
        }).round(2)
        print(df_grouped.to_string())

        # Predicate filtering at read time
        print("\n" + "="*60)
        print("Step 5: Predicate filtering at read time")
        print("="*60)
        predicate_builder = read_builder.new_predicate_builder()
        predicate = predicate_builder.equal('category', 'A')
        read_builder_filtered = read_builder.with_filter(predicate)

        table_read_filtered = read_builder_filtered.new_read()
        table_scan_filtered = read_builder_filtered.new_scan()
        splits_filtered = table_scan_filtered.plan().splits()

        ray_dataset_filtered = table_read_filtered.to_ray(splits_filtered, parallelism=2)
        df_filtered_at_read = ray_dataset_filtered.to_pandas()
        print(f"✓ Filtered at read time: {ray_dataset_filtered.count()} rows")
        print(f"  - All categories are 'A': {all(df_filtered_at_read['category'] == 'A')}")

        # Projection (select specific columns)
        print("\n" + "="*60)
        print("Step 6: Column projection")
        print("="*60)
        read_builder_projected = read_builder.with_projection(['id', 'name', 'value'])
        table_read_projected = read_builder_projected.new_read()
        table_scan_projected = read_builder_projected.new_scan()
        splits_projected = table_scan_projected.plan().splits()

        ray_dataset_projected = table_read_projected.to_ray(splits_projected, parallelism=2)
        df_projected = ray_dataset_projected.to_pandas()
        print(f"✓ Projected columns: {list(df_projected.columns)}")
        print("  - Expected: ['id', 'name', 'value']")
        print(f"  - Match: {set(df_projected.columns) == {'id', 'name', 'value'}}")

        # Optional: Blob data example (uncomment to demonstrate blob data handling)
        # print("\n" + "="*60)
        # print("Step 7: Blob data handling (Optional)")
        # print("="*60)
        # print("Paimon supports storing blob data (images, audio, etc.)")
        # print("For blob data examples, see: pypaimon/sample/oss_blob_as_descriptor.py")

        print("\n" + "="*60)
        print("Summary")
        print("="*60)
        print("✓ Successfully demonstrated Ray Data integration with REST catalog")
        print("✓ Distributed reading works correctly")
        print("✓ Ray Data operations (filter, map, group by) work on Paimon data")
        print("✓ Predicate filtering and projection work at read time")
        print("\nRay Data enables:")
        print("  - Distributed parallel reading from Paimon tables")
        print("  - Scalable data processing with Ray operations")
        print("  - Integration with Ray ecosystem (Ray Train, Ray Serve, etc.)")
        print("\nFor blob data (images, audio) examples, see:")
        print("  - pypaimon/sample/oss_blob_as_descriptor.py")

    finally:
        server.shutdown()
        if ray.is_initialized():
            ray.shutdown()
        print("\n✓ Server stopped and Ray shutdown")


if __name__ == '__main__':
    main()
