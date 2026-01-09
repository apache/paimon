#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import os
import tempfile
import unittest

import pyarrow as pa
from parameterized import parameterized
from torch.utils.data import DataLoader

from pypaimon import CatalogFactory, Schema

from pypaimon.table.file_store_table import FileStoreTable


class TorchReadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('user_id', pa.int32()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        cls.expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
            'behavior': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p2'],
        }, schema=cls.pa_schema)

    @parameterized.expand([True, False])
    def test_torch_read(self, is_streaming: bool = False):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'])
        self.catalog.create_table(f'default.test_torch_read_{str(is_streaming)}', schema, False)
        table = self.catalog.get_table(f'default.test_torch_read_{str(is_streaming)}')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['user_id', 'behavior'])
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        dataset = table_read.to_torch(splits, streaming=is_streaming)
        dataloader = DataLoader(
            dataset,
            batch_size=2,
            num_workers=2,
            shuffle=False
        )

        # Collect all data from dataloader
        all_user_ids = []
        all_behaviors = []
        for batch_idx, batch_data in enumerate(dataloader):
            user_ids = batch_data['user_id'].tolist()
            behaviors = batch_data['behavior']
            all_user_ids.extend(user_ids)
            all_behaviors.extend(behaviors)

        # Sort by user_id for comparison
        sorted_data = sorted(zip(all_user_ids, all_behaviors), key=lambda x: x[0])
        sorted_user_ids = [x[0] for x in sorted_data]
        sorted_behaviors = [x[1] for x in sorted_data]

        # Expected data (sorted by user_id)
        expected_user_ids = [1, 2, 3, 4, 5, 6, 7, 8]
        expected_behaviors = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']

        # Verify results
        self.assertEqual(sorted_user_ids, expected_user_ids,
                         f"User IDs mismatch. Expected {expected_user_ids}, got {sorted_user_ids}")
        self.assertEqual(sorted_behaviors, expected_behaviors,
                         f"Behaviors mismatch. Expected {expected_behaviors}, got {sorted_behaviors}")

        print(f"✓ Test passed: Successfully read {len(all_user_ids)} rows with correct data")

    def test_blob_torch_read(self):
        """Test end-to-end blob functionality using blob descriptors."""
        import random
        from pypaimon import Schema
        from pypaimon.table.row.blob import BlobDescriptor

        # Create schema with blob column
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('picture', pa.large_binary()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={
                'row-tracking.enabled': 'true',
                'data-evolution.enabled': 'true',
                'blob-as-descriptor': 'true'
            }
        )

        # Create table
        self.catalog.create_table('default.test_blob_torch_read', schema, False)
        table: FileStoreTable = self.catalog.get_table('default.test_blob_torch_read')

        # Create test blob data (1MB)
        blob_data = bytearray(1024 * 1024)
        random.seed(42)  # For reproducible tests
        for i in range(len(blob_data)):
            blob_data[i] = random.randint(0, 255)
        blob_data = bytes(blob_data)

        # Create external blob file
        external_blob_path = os.path.join(self.tempdir, 'external_blob')
        with open(external_blob_path, 'wb') as f:
            f.write(blob_data)

        # Create blob descriptor pointing to external file
        blob_descriptor = BlobDescriptor(external_blob_path, 0, len(blob_data))

        # Create test data with blob descriptor
        test_data = pa.Table.from_pydict({
            'id': [1],
            'picture': [blob_descriptor.serialize()]
        }, schema=pa_schema)

        # Write data using table API
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)

        # Commit the data
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)

        # Read data back
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        result = table_read.to_torch(table_scan.plan().splits())

        dataloader = DataLoader(
            result,
            batch_size=1,
            num_workers=0,
            shuffle=False
        )

        # Collect and verify data
        all_ids = []
        all_pictures = []
        for batch_idx, batch_data in enumerate(dataloader):
            ids = batch_data['id'].tolist()
            pictures = batch_data['picture']
            all_ids.extend(ids)
            all_pictures.extend(pictures)

        # Verify results
        self.assertEqual(len(all_ids), 1, "Should have exactly 1 row")
        self.assertEqual(all_ids[0], 1, "ID should be 1")

        # Verify blob descriptor
        picture_bytes = all_pictures[0]
        self.assertIsInstance(picture_bytes, bytes, "Picture should be bytes")

        # Deserialize and verify blob descriptor
        from pypaimon.table.row.blob import BlobDescriptor
        read_blob_descriptor = BlobDescriptor.deserialize(picture_bytes)
        self.assertEqual(read_blob_descriptor.length, len(blob_data),
                         f"Blob length mismatch. Expected {len(blob_data)}, got {read_blob_descriptor.length}")
        self.assertGreaterEqual(read_blob_descriptor.offset, 0, "Offset should be non-negative")

        # Read and verify blob content
        from pypaimon.common.uri_reader import UriReaderFactory
        from pypaimon.common.options.config import CatalogOptions
        from pypaimon.table.row.blob import Blob

        catalog_options = {CatalogOptions.WAREHOUSE.key(): self.warehouse}
        uri_reader_factory = UriReaderFactory(catalog_options)
        uri_reader = uri_reader_factory.create(read_blob_descriptor.uri)
        blob = Blob.from_descriptor(uri_reader, read_blob_descriptor)

        # Verify blob data matches original
        read_blob_data = blob.to_data()
        self.assertEqual(len(read_blob_data), len(blob_data),
                         f"Blob data length mismatch. Expected {len(blob_data)}, got {len(read_blob_data)}")
        self.assertEqual(read_blob_data, blob_data, "Blob data content should match original")

        print(f"✓ Blob torch read test passed: Successfully read and verified {len(blob_data)} bytes of blob data")

    def test_torch_read_pk_table(self):
        """Test torch read with primary key table."""
        # Create PK table with user_id as primary key and behavior as partition key
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['user_id', 'behavior'],
            partition_keys=['behavior'],
            options={'bucket': 2}
        )
        self.catalog.create_table('default.test_pk_table', schema, False)
        table = self.catalog.get_table('default.test_pk_table')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['user_id', 'behavior'])
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        dataset = table_read.to_torch(splits, streaming=True)
        dataloader = DataLoader(
            dataset,
            batch_size=2,
            num_workers=0,
            shuffle=False
        )

        # Collect all data from dataloader
        all_user_ids = []
        all_behaviors = []
        for batch_idx, batch_data in enumerate(dataloader):
            user_ids = batch_data['user_id'].tolist()
            behaviors = batch_data['behavior']
            all_user_ids.extend(user_ids)
            all_behaviors.extend(behaviors)

        # Sort by user_id for comparison
        sorted_data = sorted(zip(all_user_ids, all_behaviors), key=lambda x: x[0])
        sorted_user_ids = [x[0] for x in sorted_data]
        sorted_behaviors = [x[1] for x in sorted_data]

        # Expected data (sorted by user_id)
        expected_user_ids = [1, 2, 3, 4, 5, 6, 7, 8]
        expected_behaviors = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']

        # Verify results
        self.assertEqual(sorted_user_ids, expected_user_ids,
                         f"User IDs mismatch. Expected {expected_user_ids}, got {sorted_user_ids}")
        self.assertEqual(sorted_behaviors, expected_behaviors,
                         f"Behaviors mismatch. Expected {expected_behaviors}, got {sorted_behaviors}")

        print(f"✓ PK table test passed: Successfully read {len(all_user_ids)} rows with correct data")

    def test_torch_read_large_append_table(self):
        """Test torch read with large data volume on append-only table."""
        # Create append-only table
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['dt'])
        self.catalog.create_table('default.test_large_append', schema, False)
        table = self.catalog.get_table('default.test_large_append')

        # Write large amount of data
        write_builder = table.new_batch_write_builder()
        total_rows = 100000  # 10万行数据
        batch_size = 10000
        num_batches = total_rows // batch_size

        print(f"\n{'=' * 60}")
        print(f"Writing {total_rows} rows to append-only table...")
        print(f"{'=' * 60}")

        for batch_idx in range(num_batches):
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()

            start_id = batch_idx * batch_size + 1
            end_id = start_id + batch_size

            data = {
                'user_id': list(range(start_id, end_id)),
                'item_id': [1000 + i for i in range(start_id, end_id)],
                'behavior': [chr(ord('a') + (i % 26)) for i in range(batch_size)],
                'dt': [f'p{i % 4}' for i in range(batch_size)],
            }
            pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
            table_write.write_arrow(pa_table)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

            if (batch_idx + 1) % 2 == 0:
                print(f"  Written {(batch_idx + 1) * batch_size} rows...")

        # Read data using torch
        print(f"\nReading {total_rows} rows using Torch DataLoader...")

        read_builder = table.new_read_builder().with_projection(['user_id', 'behavior'])
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        print(f"Total splits: {len(splits)}")

        dataset = table_read.to_torch(splits, streaming=True)
        dataloader = DataLoader(
            dataset,
            batch_size=1000,
            num_workers=4,
            shuffle=False
        )

        # Collect all data
        all_user_ids = []
        batch_count = 0
        for batch_idx, batch_data in enumerate(dataloader):
            batch_count += 1
            user_ids = batch_data['user_id'].tolist()
            all_user_ids.extend(user_ids)

            if (batch_idx + 1) % 20 == 0:
                print(f"  Read {len(all_user_ids)} rows...")

        all_user_ids.sort()
        # Verify data
        self.assertEqual(len(all_user_ids), total_rows,
                         f"Row count mismatch. Expected {total_rows}, got {len(all_user_ids)}")
        self.assertEqual(all_user_ids, list(range(1, total_rows + 1)),
                         f"Row count mismatch. Expected {total_rows}, got {len(all_user_ids)}")
        print(f"\n{'=' * 60}")
        print("✓ Large append table test passed!")
        print(f"  Total rows: {total_rows}")
        print(f"  Total batches: {batch_count}")
        print(f"{'=' * 60}\n")

    def test_torch_read_large_pk_table(self):
        """Test torch read with large data volume on primary key table."""

        # Create PK table
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['user_id'],
            partition_keys=['dt'],
            options={'bucket': '4'}
        )
        self.catalog.create_table('default.test_large_pk', schema, False)
        table = self.catalog.get_table('default.test_large_pk')

        # Write large amount of data
        write_builder = table.new_batch_write_builder()
        total_rows = 100000  # 10万行数据
        batch_size = 10000
        num_batches = total_rows // batch_size

        print(f"\n{'=' * 60}")
        print(f"Writing {total_rows} rows to PK table...")
        print(f"{'=' * 60}")

        for batch_idx in range(num_batches):
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()

            start_id = batch_idx * batch_size + 1
            end_id = start_id + batch_size

            data = {
                'user_id': list(range(start_id, end_id)),
                'item_id': [1000 + i for i in range(start_id, end_id)],
                'behavior': [chr(ord('a') + (i % 26)) for i in range(batch_size)],
                'dt': [f'p{i % 4}' for i in range(batch_size)],
            }
            pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
            table_write.write_arrow(pa_table)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

            if (batch_idx + 1) % 2 == 0:
                print(f"  Written {(batch_idx + 1) * batch_size} rows...")

        # Read data using torch
        print(f"\nReading {total_rows} rows using Torch DataLoader...")

        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        print(f"Total splits: {len(splits)}")

        dataset = table_read.to_torch(splits, streaming=True)
        dataloader = DataLoader(
            dataset,
            batch_size=1000,
            num_workers=8,
            shuffle=False
        )

        # Collect all data
        all_user_ids = []
        batch_count = 0
        for batch_idx, batch_data in enumerate(dataloader):
            batch_count += 1
            user_ids = batch_data['user_id'].tolist()
            all_user_ids.extend(user_ids)

            if (batch_idx + 1) % 20 == 0:
                print(f"  Read {len(all_user_ids)} rows...")

        all_user_ids.sort()
        # Verify data
        self.assertEqual(len(all_user_ids), total_rows,
                         f"Row count mismatch. Expected {total_rows}, got {len(all_user_ids)}")

        self.assertEqual(all_user_ids, list(range(1, total_rows + 1)),
                         f"Row count mismatch. Expected {total_rows}, got {len(all_user_ids)}")

        print(f"\n{'=' * 60}")
        print("✓ Large PK table test passed!")
        print(f"  Total rows: {total_rows}")
        print(f"  Total batches: {batch_count}")
        print("  Primary key uniqueness: ✓")
        print(f"{'=' * 60}\n")

    def test_torch_read_with_predicate(self):
        """Test torch read with predicate filtering."""

        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'])
        self.catalog.create_table('default.test_predicate', schema, False)
        table = self.catalog.get_table('default.test_predicate')
        self._write_test_table(table)

        # Test case 1: Filter by user_id > 4
        print(f"\n{'=' * 60}")
        print("Test Case 1: user_id > 4")
        print(f"{'=' * 60}")
        predicate_builder = table.new_read_builder().new_predicate_builder()

        predicate = predicate_builder.greater_than('user_id', 4)
        read_builder = table.new_read_builder().with_filter(predicate)
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        dataset = table_read.to_torch(splits, streaming=True)
        dataloader = DataLoader(
            dataset,
            batch_size=2,
            num_workers=0,
            shuffle=False
        )

        all_user_ids = []
        for batch_idx, batch_data in enumerate(dataloader):
            user_ids = batch_data['user_id'].tolist()
            all_user_ids.extend(user_ids)

        all_user_ids.sort()
        expected_user_ids = [5, 6, 7, 8]
        self.assertEqual(all_user_ids, expected_user_ids,
                         f"User IDs mismatch. Expected {expected_user_ids}, got {all_user_ids}")
        print(f"✓ Filtered {len(all_user_ids)} rows: {all_user_ids}")

        # Test case 2: Filter by user_id <= 3
        print(f"\n{'=' * 60}")
        print("Test Case 2: user_id <= 3")
        print(f"{'=' * 60}")

        predicate = predicate_builder.less_or_equal('user_id', 3)
        read_builder = table.new_read_builder().with_filter(predicate)
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        dataset = table_read.to_torch(splits, streaming=True)
        dataloader = DataLoader(
            dataset,
            batch_size=2,
            num_workers=0,
            shuffle=False
        )

        all_user_ids = []
        for batch_idx, batch_data in enumerate(dataloader):
            user_ids = batch_data['user_id'].tolist()
            all_user_ids.extend(user_ids)

        all_user_ids.sort()
        expected_user_ids = [1, 2, 3]
        self.assertEqual(all_user_ids, expected_user_ids,
                         f"User IDs mismatch. Expected {expected_user_ids}, got {all_user_ids}")
        print(f"✓ Filtered {len(all_user_ids)} rows: {all_user_ids}")

        # Test case 3: Filter by behavior = 'a'
        print(f"\n{'=' * 60}")
        print("Test Case 3: behavior = 'a'")
        print(f"{'=' * 60}")

        predicate = predicate_builder.equal('behavior', 'a')
        read_builder = table.new_read_builder().with_filter(predicate)
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        dataset = table_read.to_torch(splits, streaming=True)
        dataloader = DataLoader(
            dataset,
            batch_size=2,
            num_workers=0,
            shuffle=False
        )

        all_user_ids = []
        all_behaviors = []
        for batch_idx, batch_data in enumerate(dataloader):
            user_ids = batch_data['user_id'].tolist()
            behaviors = batch_data['behavior']
            all_user_ids.extend(user_ids)
            all_behaviors.extend(behaviors)

        expected_user_ids = [1]
        expected_behaviors = ['a']
        self.assertEqual(all_user_ids, expected_user_ids,
                         f"User IDs mismatch. Expected {expected_user_ids}, got {all_user_ids}")
        self.assertEqual(all_behaviors, expected_behaviors,
                         f"Behaviors mismatch. Expected {expected_behaviors}, got {all_behaviors}")
        print(f"✓ Filtered {len(all_user_ids)} rows: user_ids={all_user_ids}, behaviors={all_behaviors}")

        # Test case 4: Filter by user_id IN (2, 4, 6)
        print(f"\n{'=' * 60}")
        print("Test Case 4: user_id IN (2, 4, 6)")
        print(f"{'=' * 60}")

        predicate = predicate_builder.is_in('user_id', [2, 4, 6])
        read_builder = table.new_read_builder().with_filter(predicate)
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        dataset = table_read.to_torch(splits, streaming=True)
        dataloader = DataLoader(
            dataset,
            batch_size=2,
            num_workers=0,
            shuffle=False
        )

        all_user_ids = []
        for batch_idx, batch_data in enumerate(dataloader):
            user_ids = batch_data['user_id'].tolist()
            all_user_ids.extend(user_ids)

        all_user_ids.sort()
        expected_user_ids = [2, 4, 6]
        self.assertEqual(all_user_ids, expected_user_ids,
                         f"User IDs mismatch. Expected {expected_user_ids}, got {all_user_ids}")
        print(f"✓ Filtered {len(all_user_ids)} rows: {all_user_ids}")

        # Test case 5: Combined filter (user_id > 2 AND user_id < 7)
        print(f"\n{'=' * 60}")
        print("Test Case 5: user_id > 2 AND user_id < 7")
        print(f"{'=' * 60}")

        predicate1 = predicate_builder.greater_than('user_id', 2)
        predicate2 = predicate_builder.less_than('user_id', 7)
        combined_predicate = predicate_builder.and_predicates([predicate1, predicate2])
        read_builder = table.new_read_builder().with_filter(combined_predicate)
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        dataset = table_read.to_torch(splits, streaming=True)
        dataloader = DataLoader(
            dataset,
            batch_size=2,
            num_workers=0,
            shuffle=False
        )

        all_user_ids = []
        for batch_idx, batch_data in enumerate(dataloader):
            user_ids = batch_data['user_id'].tolist()
            all_user_ids.extend(user_ids)

        all_user_ids.sort()
        expected_user_ids = [3, 4, 5, 6]
        self.assertEqual(all_user_ids, expected_user_ids,
                         f"User IDs mismatch. Expected {expected_user_ids}, got {all_user_ids}")
        print(f"✓ Filtered {len(all_user_ids)} rows: {all_user_ids}")

        print(f"\n{'=' * 60}")
        print("✓ All predicate test cases passed!")
        print(f"{'=' * 60}\n")

    def _write_test_table(self, table):
        write_builder = table.new_batch_write_builder()

        # first write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', 'd'],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # second write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = {
            'user_id': [5, 6, 7, 8],
            'item_id': [1005, 1006, 1007, 1008],
            'behavior': ['e', 'f', 'g', 'h'],
            'dt': ['p2', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data2, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _read_test_table(self, read_builder):
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        return table_read.to_arrow(splits)
