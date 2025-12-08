#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for PyTorch Dataset integration.
"""

import os
import shutil
import tempfile
import unittest

try:
    import torch
    from torch.utils.data import DataLoader
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

import pyarrow as pa

from pypaimon.catalog import CatalogFactory
from pypaimon.schema import Schema

if TORCH_AVAILABLE:
    from pypaimon.ml.pytorch import (
        PaimonIterableDataset,
        PaimonMapDataset,
        PaimonDatasetConfig
    )


@unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
class TestPaimonDatasetConfig(unittest.TestCase):
    """Test PaimonDatasetConfig."""

    def test_default_config(self):
        """Test default configuration."""
        config = PaimonDatasetConfig()
        self.assertEqual(config.batch_size, 32)
        self.assertEqual(config.prefetch_batches, 2)
        self.assertEqual(config.shuffle_buffer_size, 0)
        self.assertIsNone(config.target_column)
        self.assertEqual(config.feature_columns, [])

    def test_custom_config(self):
        """Test custom configuration."""
        config = PaimonDatasetConfig(
            batch_size=64,
            target_column='label',
            feature_columns=['feature1', 'feature2']
        )
        self.assertEqual(config.batch_size, 64)
        self.assertEqual(config.target_column, 'label')
        self.assertEqual(config.feature_columns, ['feature1', 'feature2'])


@unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
class TestPaimonIterableDataset(unittest.TestCase):
    """Test PaimonIterableDataset."""

    @classmethod
    def setUpClass(cls):
        """Set up test catalog and table."""
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

        # Create test table
        cls.pa_schema = pa.schema([
            ('feature1', pa.int64()),
            ('feature2', pa.float64()),
            ('label', pa.int32())
        ])

        cls.raw_data = {
            'feature1': [1, 2, 3, 4, 5],
            'feature2': [1.0, 2.0, 3.0, 4.0, 5.0],
            'label': [0, 1, 0, 1, 1]
        }
        cls.expected = pa.Table.from_pydict(cls.raw_data, schema=cls.pa_schema)

        schema = Schema.from_pyarrow_schema(cls.pa_schema)
        cls.catalog.create_table('default.test_dataset', schema, False)
        cls.table = cls.catalog.get_table('default.test_dataset')

        # Write data
        write_builder = cls.table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(cls.expected)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    @classmethod
    def tearDownClass(cls):
        """Clean up."""
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_iterable_dataset_creation(self):
        """Test creating IterableDataset."""
        read_builder = self.table.new_read_builder()
        dataset = PaimonIterableDataset(read_builder=read_builder)
        self.assertIsNotNone(dataset)
        self.assertIsNotNone(dataset.read_builder)
        self.assertIsNotNone(dataset.config)

    def test_iterable_dataset_with_config(self):
        """Test IterableDataset with configuration."""
        config = PaimonDatasetConfig(
            target_column='label',
            feature_columns=['feature1', 'feature2']
        )
        read_builder = self.table.new_read_builder()
        dataset = PaimonIterableDataset(read_builder=read_builder, config=config)
        self.assertEqual(dataset.config.target_column, 'label')
        self.assertEqual(dataset.config.feature_columns, ['feature1', 'feature2'])

    def test_iterable_dataset_iteration(self):
        """Test iterating over IterableDataset."""
        read_builder = self.table.new_read_builder()
        config = PaimonDatasetConfig(target_column='label')
        dataset = PaimonIterableDataset(read_builder=read_builder, config=config)

        batch_count = 0
        row_count = 0
        for features, labels in dataset:
            batch_count += 1
            row_count += features.shape[0]
            self.assertIsInstance(features, torch.Tensor)
            self.assertIsInstance(labels, torch.Tensor)
            self.assertEqual(features.dtype, torch.float32)

        self.assertGreater(batch_count, 0)
        self.assertEqual(row_count, 5)

    def test_iterable_dataset_with_dataloader(self):
        """Test IterableDataset with DataLoader."""
        read_builder = self.table.new_read_builder()
        config = PaimonDatasetConfig(target_column='label')
        dataset = PaimonIterableDataset(read_builder=read_builder, config=config)

        dataloader = DataLoader(dataset, batch_size=2)
        batch_count = 0
        for features, labels in dataloader:
            batch_count += 1
            self.assertIsInstance(features, torch.Tensor)
            self.assertIsInstance(labels, torch.Tensor)

        self.assertGreater(batch_count, 0)

    def test_iterable_dataset_invalid_read_builder(self):
        """Test IterableDataset with invalid read builder."""
        with self.assertRaises(ValueError):
            PaimonIterableDataset(read_builder=None)


@unittest.skipIf(not TORCH_AVAILABLE, "PyTorch not available")
class TestPaimonMapDataset(unittest.TestCase):
    """Test PaimonMapDataset."""

    @classmethod
    def setUpClass(cls):
        """Set up test catalog and table."""
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

        # Create test table
        cls.pa_schema = pa.schema([
            ('feature1', pa.int64()),
            ('feature2', pa.float64()),
            ('label', pa.int32())
        ])

        cls.raw_data = {
            'feature1': [1, 2, 3, 4, 5],
            'feature2': [1.0, 2.0, 3.0, 4.0, 5.0],
            'label': [0, 1, 0, 1, 1]
        }
        cls.expected = pa.Table.from_pydict(cls.raw_data, schema=cls.pa_schema)

        schema = Schema.from_pyarrow_schema(cls.pa_schema)
        cls.catalog.create_table('default.test_map_dataset', schema, False)
        cls.table = cls.catalog.get_table('default.test_map_dataset')

        # Write data
        write_builder = cls.table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(cls.expected)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    @classmethod
    def tearDownClass(cls):
        """Clean up."""
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_map_dataset_creation(self):
        """Test creating MapDataset."""
        read_builder = self.table.new_read_builder()
        dataset = PaimonMapDataset(read_builder=read_builder)
        self.assertIsNotNone(dataset)
        self.assertEqual(dataset.num_rows, 5)

    def test_map_dataset_len(self):
        """Test MapDataset length."""
        read_builder = self.table.new_read_builder()
        dataset = PaimonMapDataset(read_builder=read_builder)
        self.assertEqual(len(dataset), 5)

    def test_map_dataset_getitem(self):
        """Test MapDataset __getitem__."""
        read_builder = self.table.new_read_builder()
        config = PaimonDatasetConfig(target_column='label')
        dataset = PaimonMapDataset(read_builder=read_builder, config=config)

        features, labels = dataset[0]
        self.assertIsInstance(features, torch.Tensor)
        self.assertIsInstance(labels, torch.Tensor)
        self.assertEqual(features.shape[0], 2)  # 2 features

    def test_map_dataset_with_dataloader(self):
        """Test MapDataset with DataLoader."""
        read_builder = self.table.new_read_builder()
        config = PaimonDatasetConfig(target_column='label')
        dataset = PaimonMapDataset(read_builder=read_builder, config=config)

        dataloader = DataLoader(dataset, batch_size=2)
        batch_count = 0
        for features, labels in dataloader:
            batch_count += 1
            self.assertIsInstance(features, torch.Tensor)
            self.assertIsInstance(labels, torch.Tensor)

        self.assertEqual(batch_count, 3)  # 5 samples / 2 batch size = 3 batches

    def test_map_dataset_index_out_of_bounds(self):
        """Test MapDataset with out of bounds index."""
        read_builder = self.table.new_read_builder()
        dataset = PaimonMapDataset(read_builder=read_builder)

        with self.assertRaises(IndexError):
            dataset[10]

    def test_map_dataset_negative_index(self):
        """Test MapDataset with negative index."""
        read_builder = self.table.new_read_builder()
        dataset = PaimonMapDataset(read_builder=read_builder)

        with self.assertRaises(IndexError):
            dataset[-1]

    def test_map_dataset_invalid_read_builder(self):
        """Test MapDataset with invalid read builder."""
        with self.assertRaises(ValueError):
            PaimonMapDataset(read_builder=None)


if __name__ == '__main__':
    unittest.main()
