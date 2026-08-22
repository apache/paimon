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

import types
import unittest
from unittest import mock

import pyarrow as pa
import pytest

pytest.importorskip("ray")

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.ray.data_evolution_merge_into import (
    _estimate_merge_input_size_bytes,
)
from pypaimon.ray.partitioning import (
    _estimate_dataset_num_rows,
    _estimate_dataset_size_bytes,
    _estimate_table_scan_size_bytes,
    _resolve_num_partitions,
    _resolve_row_id_num_partitions,
)


class RayPartitioningTest(unittest.TestCase):

    def test_explicit_num_partitions_is_unchanged(self):
        with mock.patch(
            "ray.cluster_resources",
            side_effect=AssertionError("must not inspect the cluster"),
        ):
            self.assertEqual(_resolve_num_partitions(37, 1), 37)

    def test_known_size_uses_target_block_size_and_cpu_cap(self):
        from ray.data.context import DataContext

        context = DataContext.get_current()
        previous_target = context.target_max_block_size
        context.target_max_block_size = 128 * 1024 * 1024
        try:
            with mock.patch(
                "ray.cluster_resources", return_value={"CPU": 320}
            ):
                self.assertEqual(_resolve_num_partitions(None, 0), 1)
                self.assertEqual(
                    _resolve_num_partitions(None, 5 * 128 * 1024 * 1024),
                    5,
                )
                self.assertEqual(
                    _resolve_num_partitions(None, 641 * 128 * 1024 * 1024),
                    640,
                )
                # At the same average row width, a 10x larger input gets
                # proportionally more partitions instead of both using 640.
                self.assertEqual(
                    _resolve_num_partitions(None, 5_000_000 * 1024),
                    39,
                )
                self.assertEqual(
                    _resolve_num_partitions(None, 50_000_000 * 1024),
                    382,
                )
                self.assertEqual(
                    _resolve_num_partitions(None, 1, min_partitions=25),
                    25,
                )
        finally:
            context.target_max_block_size = previous_target

    def test_unknown_size_keeps_cpu_default(self):
        with mock.patch(
            "ray.cluster_resources", return_value={"CPU": 320}
        ):
            self.assertEqual(_resolve_num_partitions(None, None), 640)

    def test_dataset_estimate_does_not_call_size_bytes(self):
        metadata = types.SimpleNamespace(size_bytes=1234)
        dataset = types.SimpleNamespace(
            _logical_plan=types.SimpleNamespace(
                dag=types.SimpleNamespace(
                    infer_metadata=mock.Mock(return_value=metadata)
                )
            ),
            size_bytes=mock.Mock(
                side_effect=AssertionError("must not execute Dataset")
            ),
        )

        self.assertEqual(_estimate_dataset_size_bytes(dataset), 1234)
        dataset.size_bytes.assert_not_called()

    def test_unknown_dataset_estimate_falls_back(self):
        metadata = types.SimpleNamespace(size_bytes=None)
        dataset = types.SimpleNamespace(
            _logical_plan=types.SimpleNamespace(
                dag=types.SimpleNamespace(
                    infer_metadata=mock.Mock(return_value=metadata)
                )
            )
        )

        self.assertIsNone(_estimate_dataset_size_bytes(dataset))

    def test_ray_metadata_falls_back_to_unary_input(self):
        import ray

        dataset = ray.data.from_arrow(pa.table({"id": [1, 2]}))
        self.assertEqual(_estimate_dataset_size_bytes(dataset), 16)
        self.assertEqual(_estimate_dataset_num_rows(dataset), 2)

        mapped = dataset.map_batches(lambda batch: batch)
        self.assertEqual(_estimate_dataset_size_bytes(mapped), 16)
        self.assertEqual(_estimate_dataset_num_rows(mapped), 2)

    def test_sparse_row_ids_keep_shuffle_parallelism(self):
        with mock.patch(
            "ray.cluster_resources", return_value={"CPU": 320}
        ):
            self.assertEqual(
                _resolve_row_id_num_partitions(None, 4096, 500, 500),
                200,
            )
            self.assertEqual(
                _resolve_row_id_num_partitions(None, 32, 1, 500),
                1,
            )
            self.assertEqual(
                _resolve_row_id_num_partitions(37, 1, 500, 500),
                37,
            )

    def test_table_estimate_uses_pinned_scan(self):
        captured = {}

        class _Plan:
            @staticmethod
            def splits():
                return [
                    types.SimpleNamespace(file_size=10),
                    types.SimpleNamespace(file_size=20),
                ]

        class _Scan:
            @staticmethod
            def plan():
                return _Plan()

        class _ReadBuilder:
            @staticmethod
            def new_scan():
                return _Scan()

        class _Table:
            def copy(self, options):
                captured["options"] = options
                return self

            @staticmethod
            def new_read_builder():
                return _ReadBuilder()

        self.assertEqual(
            _estimate_table_scan_size_bytes(_Table(), 7),
            30,
        )
        self.assertEqual(
            captured["options"][CoreOptions.SCAN_SNAPSHOT_ID.key()], "7"
        )

    def test_merge_estimate_uses_source_and_target_scan(self):
        update_clause = types.SimpleNamespace(
            spec={"age": object()}, delete=False, condition=None,
        )
        delete_clause = types.SimpleNamespace(
            spec={}, delete=True, condition=None,
        )
        ctx = types.SimpleNamespace(
            is_self_merge=False,
            target_on_cols=["id"],
            settable_field_names=["id", "name", "age"],
        )
        snapshot = types.SimpleNamespace(id=9)

        with mock.patch(
            "pypaimon.ray.data_evolution_merge_into."
            "_estimate_dataset_size_bytes",
            return_value=100,
        ), mock.patch(
            "pypaimon.ray.data_evolution_merge_into."
            "_estimate_table_scan_size_bytes",
            return_value=20,
        ) as target_estimate:
            result = _estimate_merge_input_size_bytes(
                object(), object(),
                [update_clause, delete_clause], [object()], ctx, snapshot,
            )

        self.assertEqual(result, 120)
        target_estimate.assert_called_once_with(mock.ANY, 9)

    def test_merge_estimate_falls_back_when_source_size_is_unknown(self):
        ctx = types.SimpleNamespace(is_self_merge=False)
        with mock.patch(
            "pypaimon.ray.data_evolution_merge_into."
            "_estimate_dataset_size_bytes",
            return_value=None,
        ), mock.patch(
            "pypaimon.ray.data_evolution_merge_into."
            "_estimate_table_scan_size_bytes",
        ) as target_estimate:
            result = _estimate_merge_input_size_bytes(
                object(), object(), [], [], ctx, None,
            )

        self.assertIsNone(result)
        target_estimate.assert_not_called()


if __name__ == "__main__":
    unittest.main()
