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

import json
import os
import pickle
import shutil
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
from parameterized import parameterized
import torch
from torch.utils.data import DataLoader

from pypaimon import CatalogFactory, Schema

from pypaimon.read.datasource.torch_dataset import (
    TorchIterDataset,
    TorchShuffledIterDataset,
    _resolve_distributed_context,
)
from pypaimon.table.file_store_table import FileStoreTable


class TorchDistributedShardingTest(unittest.TestCase):
    @staticmethod
    def _table_read(limit=None):
        return SimpleNamespace(limit=limit, read_type=[])

    @staticmethod
    def _worker(worker_id, num_workers):
        return SimpleNamespace(id=worker_id, num_workers=num_workers)

    def _dataset(
        self,
        splits,
        rank=0,
        world_size=1,
        limit=None,
        dataset_type=TorchIterDataset,
        **kwargs
    ):
        with patch(
            "pypaimon.read.datasource.torch_dataset."
            "_resolve_distributed_context",
            return_value=(rank, world_size),
        ):
            return dataset_type(
                self._table_read(limit),
                splits,
                auto_detect_rank=True,
                **kwargs
            )

    def _assignments(self, split_count, world_size, num_workers):
        splits = list(range(split_count))
        assignments = {}
        for rank in range(world_size):
            dataset = self._dataset(splits, rank, world_size)
            for worker_id in range(num_workers):
                assignments[(rank, worker_id)] = dataset._worker_splits(
                    self._worker(worker_id, num_workers)
                )
        return assignments

    def assertCompleteNonOverlapping(self, assignments, expected):
        assigned = [
            split for splits in assignments.values() for split in splits
        ]
        self.assertCountEqual(assigned, expected)
        self.assertEqual(len(assigned), len(set(assigned)))

    @parameterized.expand([
        ("single", 7, 1, 1, [7]),
        ("workers", 10, 1, 3, [4, 3, 3]),
        ("ranks", 10, 3, 1, [4, 3, 3]),
        ("rank_workers", 17, 3, 2, [3, 3, 3, 3, 3, 2]),
        ("uneven", 11, 2, 2, [3, 3, 3, 2]),
        ("sparse", 3, 2, 3, [1, 1, 0, 1, 0, 0]),
    ])
    def test_balanced_assignments(
        self, _, split_count, world_size, num_workers, expected_sizes
    ):
        assignments = self._assignments(
            split_count, world_size, num_workers
        )
        self.assertCompleteNonOverlapping(
            assignments, list(range(split_count))
        )
        self.assertEqual(
            [len(splits) for splits in assignments.values()], expected_sizes
        )

    def test_binding_limit_uses_one_distributed_consumer(self):
        splits = [SimpleNamespace(row_count=10) for _ in range(4)]
        assignments = {}
        for rank in range(2):
            dataset = self._dataset(splits, rank, 2, limit=5)
            for worker_id in range(2):
                assignments[(rank, worker_id)] = dataset._worker_splits(
                    self._worker(worker_id, 2)
                )

        self.assertEqual(assignments[(0, 0)], splits)
        self.assertTrue(
            all(
                not value for key, value in assignments.items()
                if key != (0, 0)
            )
        )

    def test_initialized_distributed_context_precedes_environment(self):
        with patch.dict(
            os.environ, {"RANK": "4", "WORLD_SIZE": "5"}, clear=True
        ), patch.object(
            torch.distributed, "is_available", return_value=True
        ), patch.object(
            torch.distributed, "is_initialized", return_value=True
        ), patch.object(
            torch.distributed, "get_rank", return_value=1
        ), patch.object(
            torch.distributed, "get_world_size", return_value=3
        ):
            context = _resolve_distributed_context(True)

        self.assertEqual(context, (1, 3))

    def test_worker_process_can_resolve_torchrun_environment(self):
        with patch.dict(
            os.environ, {"RANK": "2", "WORLD_SIZE": "4"}, clear=True
        ), patch.object(
            torch.distributed, "is_available", return_value=True
        ), patch.object(
            torch.distributed, "is_initialized", return_value=False
        ):
            context = _resolve_distributed_context(True)

        self.assertEqual(context, (2, 4))
        dataset = self._dataset(list(range(8)), *context)
        self.assertEqual(dataset._worker_splits(None), [4, 5])

    def test_auto_falls_back_to_single_process(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(
            torch.distributed, "is_available", return_value=False
        ):
            context = _resolve_distributed_context(True)

        self.assertEqual(context, (0, 1))

    def test_disabled_preserves_worker_sharding(self):
        splits = list(range(8))
        with patch.dict(
            os.environ, {"RANK": "1", "WORLD_SIZE": "2"}, clear=True
        ), patch.object(
            torch.distributed, "is_available", return_value=True
        ), patch.object(
            torch.distributed, "is_initialized", return_value=True
        ):
            dataset = TorchIterDataset(
                self._table_read(), splits, auto_detect_rank=False
            )

        self.assertEqual((dataset.rank, dataset.world_size), (0, 1))
        self.assertEqual(
            dataset._worker_splits(self._worker(1, 2)),
            list(range(4, 8)),
        )

    def test_shuffled_dataset_is_reproducible_and_rank_local(self):
        splits = list(range(20))
        datasets = [
            self._dataset(
                splits,
                rank,
                2,
                dataset_type=TorchShuffledIterDataset,
                seed=17,
                buffer_size=20,
            )
            for rank in range(2)
        ]
        local_splits = [dataset._worker_splits(None) for dataset in datasets]
        self.assertTrue(set(local_splits[0]).isdisjoint(local_splits[1]))
        self.assertCountEqual(local_splits[0] + local_splits[1], splits)
        restored = pickle.loads(pickle.dumps(datasets[1]))
        self.assertEqual((restored.rank, restored.world_size), (1, 2))

        rows = [{"id": value} for value in range(20)]
        first = list(datasets[0]._iter_buffer_shuffled_rows(iter(rows), 0))
        repeat = list(datasets[0]._iter_buffer_shuffled_rows(iter(rows), 0))
        other_rank = list(
            datasets[1]._iter_buffer_shuffled_rows(iter(rows), 0)
        )
        other_worker = list(
            datasets[0]._iter_buffer_shuffled_rows(iter(rows), 1)
        )
        self.assertEqual(first, repeat)
        self.assertNotEqual(first, other_rank)
        self.assertNotEqual(first, other_worker)

        datasets[0].set_epoch(1)
        next_epoch = list(
            datasets[0]._iter_buffer_shuffled_rows(iter(rows), 0)
        )
        self.assertNotEqual(first, next_epoch)

    def test_invalid_distributed_context(self):
        with self.assertRaisesRegex(ValueError, "auto_detect_rank"):
            _resolve_distributed_context("auto")

        with patch.dict(os.environ, {"RANK": "one"}, clear=True), patch.object(
            torch.distributed, "is_available", return_value=False
        ), self.assertRaisesRegex(ValueError, "must be set together"):
            _resolve_distributed_context(True)

        with patch.dict(
            os.environ, {"RANK": "one", "WORLD_SIZE": "2"}, clear=True
        ), patch.object(
            torch.distributed, "is_available", return_value=False
        ), self.assertRaisesRegex(ValueError, "must be integers"):
            _resolve_distributed_context(True)

        for environment, message in [
            ({"RANK": "0", "WORLD_SIZE": "0"}, "greater than 0"),
            ({"RANK": "2", "WORLD_SIZE": "2"}, "0 <= rank"),
        ]:
            with self.subTest(environment=environment), patch.dict(
                os.environ, environment, clear=True
            ), patch.object(
                torch.distributed, "is_available", return_value=False
            ), self.assertRaisesRegex(ValueError, message):
                _resolve_distributed_context(True)

    @unittest.skipUnless(
        torch.distributed.is_available(), "torch.distributed is unavailable"
    )
    def test_torchrun_rank_and_worker_sharding(self):
        script = os.path.join(
            os.path.dirname(__file__), "torch_distributed_sharding_worker.py"
        )
        python_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..")
        )
        with tempfile.TemporaryDirectory() as output_dir:
            env = os.environ.copy()
            env["PYTHONPATH"] = os.pathsep.join(
                filter(None, [python_root, env.get("PYTHONPATH")])
            )
            process = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "torch.distributed.run",
                    "--standalone",
                    "--nproc-per-node=2",
                    script,
                    output_dir,
                ],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=180,
            )
            self.assertEqual(
                process.returncode,
                0,
                "torchrun failed:\n%s\n%s" % (
                    process.stdout, process.stderr
                ),
            )
            rows = []
            for rank in range(2):
                with open(
                    os.path.join(output_dir, "rank-%d.json" % rank),
                    encoding="utf-8",
                ) as result_file:
                    rows.extend(json.load(result_file))

        split_ids = [row["split_id"] for row in rows]
        self.assertCountEqual(split_ids, list(range(11)))
        self.assertEqual(len(split_ids), len(set(split_ids)))
        assignments = {}
        for row in rows:
            assignments.setdefault(
                (row["rank"], row["worker"]), []
            ).append(row["split_id"])
        self.assertEqual(
            {key: sorted(values) for key, values in assignments.items()},
            {
                (0, 0): [0, 1, 2],
                (0, 1): [3, 4, 5],
                (1, 0): [6, 7, 8],
                (1, 1): [9, 10],
            },
        )


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
        cls.pk_pa_schema = pa.schema([
            pa.field('user_id', pa.int32(), nullable=False),
            ('item_id', pa.int64()),
            pa.field('behavior', pa.string(), nullable=False),
            ('dt', pa.string())
        ])
        cls.expected = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8],
            'item_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
            'behavior': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'],
            'dt': ['p1', 'p1', 'p2', 'p1', 'p2', 'p1', 'p2', 'p2'],
        }, schema=cls.pa_schema)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

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

    def test_torch_streaming_prefetch_concurrency(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema, partition_keys=['user_id'])
        self.catalog.create_table('default.test_torch_prefetch_concurrency', schema, False)
        table = self.catalog.get_table('default.test_torch_prefetch_concurrency')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['user_id', 'behavior'])
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        self.assertGreater(len(splits), 0, "Need at least one split to test prefetch")

        dataset = table_read.to_torch(splits, streaming=True, prefetch_concurrency=4)
        dataloader = DataLoader(
            dataset,
            batch_size=2,
            num_workers=0,
            shuffle=False
        )

        all_user_ids = []
        all_behaviors = []
        for batch_data in dataloader:
            all_user_ids.extend(batch_data['user_id'].tolist())
            all_behaviors.extend(batch_data['behavior'])

        sorted_data = sorted(zip(all_user_ids, all_behaviors), key=lambda x: x[0])
        sorted_user_ids = [x[0] for x in sorted_data]
        sorted_behaviors = [x[1] for x in sorted_data]

        expected_user_ids = [1, 2, 3, 4, 5, 6, 7, 8]
        expected_behaviors = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
        self.assertEqual(len(all_user_ids), 8, "Should read 8 rows with prefetch_concurrency")
        self.assertEqual(sorted_user_ids, expected_user_ids)
        self.assertEqual(sorted_behaviors, expected_behaviors)

    def test_torch_streaming_pyarrow_batches(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['user_id']
        )
        self.catalog.create_table(
            'default.test_torch_pyarrow_batches', schema, False
        )
        table = self.catalog.get_table(
            'default.test_torch_pyarrow_batches'
        )
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(
            ['user_id', 'behavior']
        )
        splits = read_builder.new_scan().plan().splits()
        dataset = read_builder.new_read().to_torch(
            splits,
            streaming=True,
            batch_format='pyarrow',
            batch_size=3,
        )
        dataloader = DataLoader(
            dataset,
            batch_size=None,
            num_workers=2,
            shuffle=False,
        )

        batches = list(dataloader)
        self.assertTrue(batches)
        self.assertTrue(
            all(isinstance(batch, pa.RecordBatch) for batch in batches)
        )
        self.assertTrue(all(0 < batch.num_rows <= 3 for batch in batches))
        result = pa.Table.from_batches(batches).sort_by('user_id').to_pydict()
        self.assertEqual(result['user_id'], list(range(1, 9)))
        self.assertEqual(result['behavior'], list('abcdefgh'))

    def test_torch_streaming_tensor_batches(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['user_id']
        )
        self.catalog.create_table(
            'default.test_torch_tensor_batches', schema, False
        )
        table = self.catalog.get_table(
            'default.test_torch_tensor_batches'
        )
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(
            ['user_id', 'item_id']
        )
        splits = read_builder.new_scan().plan().splits()
        dataset = read_builder.new_read().to_torch(
            splits,
            streaming=True,
            batch_format='torch',
            batch_size=3,
        )

        batches = list(dataset)
        self.assertEqual([len(batch['user_id']) for batch in batches], [3, 3, 2])
        self.assertTrue(
            all(batch['user_id'].dtype == torch.int32 for batch in batches)
        )
        self.assertTrue(
            all(batch['item_id'].dtype == torch.int64 for batch in batches)
        )
        user_ids = torch.cat(
            [batch['user_id'] for batch in batches]
        ).sort().values.tolist()
        self.assertEqual(user_ids, list(range(1, 9)))

    def test_torch_streaming_batches_respect_limit(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['user_id']
        )
        self.catalog.create_table(
            'default.test_torch_batch_limit', schema, False
        )
        table = self.catalog.get_table('default.test_torch_batch_limit')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(
            ['user_id']
        ).with_limit(5)
        splits = read_builder.new_scan().plan().splits()
        dataset = read_builder.new_read().to_torch(
            splits,
            streaming=True,
            batch_format='pyarrow',
            batch_size=3,
        )
        batches = list(dataset)
        self.assertEqual([batch.num_rows for batch in batches], [3, 2])

    def test_torch_streaming_batches_respect_limit_with_workers(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['user_id']
        )
        self.catalog.create_table(
            'default.test_torch_batch_worker_limit', schema, False
        )
        table = self.catalog.get_table(
            'default.test_torch_batch_worker_limit'
        )
        self._write_test_table(table)

        predicate = (
            table.new_read_builder().new_predicate_builder()
            .greater_than('item_id', 0)
        )
        read_builder = (
            table.new_read_builder()
            .with_filter(predicate)
            .with_projection(['user_id'])
            .with_limit(5)
        )
        splits = read_builder.new_scan().plan().splits()
        self.assertGreater(len(splits), 1)
        dataset = read_builder.new_read().to_torch(
            splits,
            streaming=True,
            batch_format='pyarrow',
            batch_size=3,
        )
        self.assertEqual(
            dataset._worker_splits(SimpleNamespace(id=1, num_workers=2)),
            [],
        )
        batches = list(DataLoader(
            dataset, batch_size=None, num_workers=2
        ))
        user_ids = [
            value
            for batch in batches
            for value in batch.column('user_id').to_pylist()
        ]
        self.assertEqual(len(user_ids), 5)
        self.assertEqual(len(set(user_ids)), 5)

    def test_non_binding_limit_preserves_worker_splits(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['user_id']
        )
        self.catalog.create_table(
            'default.test_torch_non_binding_limit', schema, False
        )
        table = self.catalog.get_table(
            'default.test_torch_non_binding_limit'
        )
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_limit(1000)
        splits = read_builder.new_scan().plan().splits()
        self.assertGreater(len(splits), 1)
        table_read = read_builder.new_read()

        for batch_format in ['row', 'pyarrow']:
            dataset = table_read.to_torch(
                splits,
                streaming=True,
                batch_format=batch_format,
            )
            assigned = [
                dataset._worker_splits(
                    SimpleNamespace(id=worker_id, num_workers=2)
                )
                for worker_id in range(2)
            ]
            self.assertTrue(all(assigned))
            self.assertCountEqual(
                [id(split) for group in assigned for split in group],
                [id(split) for split in splits],
            )

    def test_non_binding_limit_uses_merged_row_counts(self):
        from pypaimon.read.datasource.torch_dataset import TorchIterDataset

        table_read = SimpleNamespace(limit=8, read_type=[])
        splits = [
            SimpleNamespace(row_count=10, merged_row_count=lambda: 4),
            SimpleNamespace(row_count=10, merged_row_count=lambda: 4),
        ]
        dataset = TorchIterDataset(table_read, splits)

        assigned = [
            dataset._worker_splits(
                SimpleNamespace(id=worker_id, num_workers=2)
            )
            for worker_id in range(2)
        ]
        self.assertTrue(all(assigned))
        self.assertCountEqual(
            [id(split) for group in assigned for split in group],
            [id(split) for split in splits],
        )

    def test_torch_batch_sizing_respects_arrow_offset_limit(self):
        from pypaimon.read.datasource.torch_dataset import (
            _sized_record_batches)

        batches = iter([
            pa.record_batch([pa.array(['aaaa'])], names=['value']),
            pa.record_batch([pa.array(['bbbb'])], names=['value']),
        ])
        with patch(
            'pypaimon.read.datasource.torch_dataset._MAX_ARROW_OFFSET', 4
        ):
            actual = list(_sized_record_batches(batches, batch_size=2))

        self.assertEqual(
            [batch.column('value').to_pylist() for batch in actual],
            [['aaaa'], ['bbbb']],
        )

    def test_default_tensor_converter_supports_fixed_size_list(self):
        from pypaimon.read.datasource.torch_dataset import _default_to_tensor

        values = pa.array([1, 2, 3, 4, 5, 6], type=pa.int32())
        features = pa.FixedSizeListArray.from_arrays(values, 3)
        batch = pa.RecordBatch.from_arrays([features], ['features'])

        result = _default_to_tensor(batch)

        self.assertEqual(result['features'].dtype, torch.int32)
        self.assertEqual(result['features'].tolist(), [[1, 2, 3], [4, 5, 6]])

    def test_torch_streaming_custom_tensor_conversion(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.catalog.create_table(
            'default.test_torch_custom_tensor_batch', schema, False
        )
        table = self.catalog.get_table(
            'default.test_torch_custom_tensor_batch'
        )
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(
            ['user_id', 'behavior']
        )
        splits = read_builder.new_scan().plan().splits()

        def to_tensor(batch):
            return {
                'user_id': torch.from_numpy(
                    batch.column('user_id').to_numpy(zero_copy_only=False)
                ),
                'behavior': batch.column('behavior').to_pylist(),
            }

        dataset = read_builder.new_read().to_torch(
            splits,
            streaming=True,
            batch_format='torch',
            batch_size=5,
            to_tensor_fn=to_tensor,
        )
        batches = list(dataset)
        self.assertEqual([len(batch['user_id']) for batch in batches], [5, 3])
        self.assertEqual(
            sorted(value for batch in batches for value in batch['behavior']),
            list('abcdefgh'),
        )

        default_dataset = read_builder.new_read().to_torch(
            splits,
            streaming=True,
            batch_format='torch',
        )
        with self.assertRaisesRegex(ValueError, "batch_format='pyarrow'"):
            next(iter(default_dataset))

    def test_torch_batch_options_validation(self):
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.catalog.create_table(
            'default.test_torch_batch_validation', schema, False
        )
        table = self.catalog.get_table(
            'default.test_torch_batch_validation'
        )
        self._write_test_table(table)
        read_builder = table.new_read_builder().with_projection(['user_id'])
        splits = read_builder.new_scan().plan().splits()
        table_read = read_builder.new_read()

        with self.assertRaisesRegex(ValueError, 'batch_format must be one of'):
            table_read.to_torch(
                splits, streaming=True, batch_format='numpy'
            )
        with self.assertRaisesRegex(ValueError, 'requires streaming=True'):
            table_read.to_torch(splits, batch_format='pyarrow')
        with self.assertRaisesRegex(ValueError, 'batch_size must be'):
            table_read.to_torch(
                splits,
                streaming=True,
                batch_format='torch',
                batch_size=0,
            )
        with self.assertRaisesRegex(ValueError, 'batch_size requires'):
            table_read.to_torch(splits, streaming=True, batch_size=2)
        with self.assertRaisesRegex(ValueError, 'only supports batch_format'):
            table_read.to_torch(
                splits,
                streaming=True,
                batch_format='torch',
                shuffle=True,
            )
        for invalid in [0, -1, 1.9, True, 2]:
            with self.subTest(prefetch_concurrency=invalid):
                with self.assertRaisesRegex(
                    ValueError, 'prefetch_concurrency'
                ):
                    table_read.to_torch(
                        splits,
                        streaming=True,
                        batch_format='pyarrow',
                        prefetch_concurrency=invalid,
                    )

    def test_torch_distributed_sharding_public_api(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['user_id']
        )
        self.catalog.create_table(
            'default.test_torch_distributed_api', schema, False
        )
        table = self.catalog.get_table(
            'default.test_torch_distributed_api'
        )
        self._write_test_table(table)
        read_builder = table.new_read_builder().with_projection(['user_id'])
        splits = read_builder.new_scan().plan().splits()
        table_read = read_builder.new_read()

        with patch(
            "pypaimon.read.datasource.torch_dataset."
            "_resolve_distributed_context",
            return_value=(1, 2),
        ):
            datasets = [
                table_read.to_torch(
                    splits,
                    streaming=True,
                    batch_format=batch_format,
                    shuffle=batch_format == 'row' and shuffle,
                )
                for batch_format, shuffle in [
                    ('row', False),
                    ('row', True),
                    ('pyarrow', False),
                ]
            ]
        for dataset in datasets:
            self.assertEqual((dataset.rank, dataset.world_size), (1, 2))

        self.assertIsNotNone(table_read.to_torch(splits))

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
            num_workers=3,
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
        total_rows = 100000
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
        large_pk_pa_schema = pa.schema([
            pa.field('user_id', pa.int32(), nullable=False),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
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
        total_rows = 100000
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
            pa_table = pa.Table.from_pydict(data, schema=large_pk_pa_schema)
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

    def test_torch_streaming_shuffle_single_worker(self):
        table = self._create_shuffle_append_table('default.test_torch_shuffle_single')
        read_builder = table.new_read_builder().with_projection(['user_id'])
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        expected = list(range(80))
        for max_buffer_input_splits in [1, 3]:
            with self.subTest(max_buffer_input_splits=max_buffer_input_splits):
                dataset = table_read.to_torch(
                    splits,
                    streaming=True,
                    shuffle=True,
                    seed=17,
                    buffer_size=7,
                    max_buffer_input_splits=max_buffer_input_splits,
                )
                ids = self._collect_torch_user_ids(dataset, num_workers=0)
                self.assertEqual(sorted(ids), expected)
                self.assertNotEqual(ids, expected)

    def test_torch_streaming_shuffle_seed_and_epoch(self):
        table = self._create_shuffle_append_table('default.test_torch_shuffle_epoch')
        read_builder = table.new_read_builder().with_projection(['user_id'])
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        dataset = table_read.to_torch(
            splits,
            streaming=True,
            shuffle=True,
            seed=23,
            buffer_size=11,
            max_buffer_input_splits=4,
        )
        epoch0 = self._collect_torch_user_ids(dataset, num_workers=0)
        epoch0_again = self._collect_torch_user_ids(dataset, num_workers=0)
        self.assertEqual(epoch0, epoch0_again)

        dataset.set_epoch(1)
        epoch1 = self._collect_torch_user_ids(dataset, num_workers=0)
        self.assertEqual(sorted(epoch1), list(range(80)))
        self.assertNotEqual(epoch0, epoch1)

        dataset.set_epoch(0)
        self.assertEqual(epoch0, self._collect_torch_user_ids(dataset, num_workers=0))

        other_seed_dataset = table_read.to_torch(
            splits,
            streaming=True,
            shuffle=True,
            seed=24,
            buffer_size=11,
            max_buffer_input_splits=4,
        )
        self.assertNotEqual(
            epoch0,
            self._collect_torch_user_ids(other_seed_dataset, num_workers=0),
        )

    def test_torch_streaming_shuffle_epoch_with_persistent_workers(self):
        table = self._create_shuffle_append_table('default.test_torch_shuffle_persistent_epoch')
        read_builder = table.new_read_builder().with_projection(['user_id'])
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        dataset = table_read.to_torch(
            splits,
            streaming=True,
            shuffle=True,
            seed=23,
            buffer_size=11,
            max_buffer_input_splits=4,
        )
        dataloader = DataLoader(
            dataset,
            batch_size=8,
            num_workers=2,
            persistent_workers=True,
            shuffle=False,
        )

        epoch0 = self._collect_torch_user_ids_from_dataloader(dataloader)
        self.assertEqual(epoch0, self._collect_torch_user_ids_from_dataloader(dataloader))

        dataset.set_epoch(1)
        epoch1 = self._collect_torch_user_ids_from_dataloader(dataloader)
        self.assertEqual(sorted(epoch1), list(range(80)))
        self.assertNotEqual(epoch0, epoch1)

    def test_torch_streaming_shuffle_multi_worker(self):
        table = self._create_shuffle_append_table('default.test_torch_shuffle_multi')
        read_builder = table.new_read_builder().with_projection(['user_id'])
        table_read = read_builder.new_read()
        splits = read_builder.new_scan() \
            .with_chunk_shuffle(seed=31, chunk_size=5) \
            .plan() \
            .splits()

        dataset = table_read.to_torch(
            splits,
            streaming=True,
            shuffle=True,
            seed=31,
            buffer_size=13,
            max_buffer_input_splits=4,
        )
        ids = self._collect_torch_user_ids(dataset, num_workers=2)

        expected = list(range(80))
        self.assertEqual(len(ids), len(expected))
        self.assertEqual(sorted(ids), expected)

    def test_torch_streaming_shuffle_rejects_non_streaming(self):
        table = self._create_shuffle_append_table('default.test_torch_shuffle_non_streaming')
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        with self.assertRaisesRegex(ValueError, "streaming=True"):
            table_read.to_torch(splits, streaming=False, shuffle=True)

    def test_torch_streaming_shuffle_accepts_pk_table_splits(self):
        pa_schema = pa.schema([
            pa.field('user_id', pa.int32(), nullable=False),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['user_id'],
            options={'bucket': '1'},
        )
        self.catalog.create_table('default.test_torch_shuffle_pk', schema, False)
        table = self.catalog.get_table('default.test_torch_shuffle_pk')
        self._write_test_table(table)

        read_builder = table.new_read_builder().with_projection(['user_id'])
        splits = read_builder.new_scan().plan().splits()
        dataset = read_builder.new_read().to_torch(
            splits,
            streaming=True,
            shuffle=True,
            seed=7,
            buffer_size=3,
        )
        ids = self._collect_torch_user_ids(dataset, num_workers=0)

        self.assertEqual(sorted(ids), [1, 2, 3, 4, 5, 6, 7, 8])

    def test_torch_streaming_shuffle_rejects_invalid_dataset_options(self):
        table = self._create_shuffle_append_table('default.test_torch_shuffle_invalid_options')
        read_builder = table.new_read_builder().with_projection(['user_id'])
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        with self.assertRaisesRegex(ValueError, "prefetch_concurrency"):
            table_read.to_torch(
                splits,
                streaming=True,
                shuffle=True,
                prefetch_concurrency=2,
            )
        with self.assertRaisesRegex(ValueError, "buffer_size"):
            table_read.to_torch(
                splits,
                streaming=True,
                shuffle=True,
                buffer_size=0,
            )
        with self.assertRaisesRegex(ValueError, "max_buffer_input_splits"):
            table_read.to_torch(
                splits,
                streaming=True,
                shuffle=True,
                max_buffer_input_splits=0,
            )

    def _create_shuffle_append_table(
        self,
        identifier,
        total_rows=80,
        rows_per_commit=10,
        partition_keys=None,
    ):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=partition_keys or [],
        )
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)

        write_builder = table.new_batch_write_builder()
        for start in range(0, total_rows, rows_per_commit):
            end = min(start + rows_per_commit, total_rows)
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            pa_table = pa.Table.from_pydict({
                'user_id': list(range(start, end)),
                'item_id': [1000 + i for i in range(start, end)],
                'behavior': [chr(ord('a') + (i % 26)) for i in range(start, end)],
                'dt': [f'p{i % 4}' for i in range(start, end)],
            }, schema=self.pa_schema)
            table_write.write_arrow(pa_table)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()
        return table

    @staticmethod
    def _collect_torch_user_ids(dataset, num_workers=0):
        dataloader = DataLoader(
            dataset,
            batch_size=8,
            num_workers=num_workers,
            shuffle=False,
        )
        all_user_ids = []
        for batch_data in dataloader:
            all_user_ids.extend(batch_data['user_id'].tolist())
        return all_user_ids

    @staticmethod
    def _collect_torch_user_ids_from_dataloader(dataloader):
        all_user_ids = []
        for batch_data in dataloader:
            all_user_ids.extend(batch_data['user_id'].tolist())
        return all_user_ids

    def _write_test_table(self, table):
        write_builder = table.new_batch_write_builder()
        table_pa_schema = self.pk_pa_schema if table.primary_keys else self.pa_schema

        # first write
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'user_id': [1, 2, 3, 4],
            'item_id': [1001, 1002, 1003, 1004],
            'behavior': ['a', 'b', 'c', 'd'],
            'dt': ['p1', 'p1', 'p2', 'p1'],
        }
        pa_table = pa.Table.from_pydict(data1, schema=table_pa_schema)
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
        pa_table = pa.Table.from_pydict(data2, schema=table_pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _read_test_table(self, read_builder):
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        return table_read.to_arrow(splits)
