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

from pypaimon.ray.data_evolution_merge_into import (
    _estimate_merge_input_size_bytes,
)
from pypaimon.ray.partitioning import (
    _estimate_dataset_num_rows,
    _estimate_dataset_size_bytes,
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
                self.assertEqual(
                    _resolve_num_partitions(None, 1, min_partitions=200),
                    200,
                )
        finally:
            context.target_max_block_size = previous_target

    def test_unknown_size_keeps_cpu_default(self):
        with mock.patch(
            "ray.cluster_resources", return_value={"CPU": 320}
        ):
            self.assertEqual(_resolve_num_partitions(None, None), 640)
            self.assertEqual(
                _resolve_num_partitions(
                    None,
                    None,
                    unknown_num_partitions=200,
                ),
                200,
            )

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

    def test_ray_metadata_respects_transform_cardinality(self):
        import inspect

        import ray

        dataset = ray.data.from_arrow(pa.table({"id": [1, 2]}))
        self.assertEqual(_estimate_dataset_size_bytes(dataset), 16)
        self.assertEqual(_estimate_dataset_num_rows(dataset), 2)

        mapped = dataset.map_batches(lambda batch: batch)
        self.assertIsNone(_estimate_dataset_size_bytes(mapped))
        self.assertIsNone(_estimate_dataset_num_rows(mapped))

        if "udf_modifying_row_count" in inspect.signature(
            ray.data.Dataset.map_batches
        ).parameters:
            one_to_one = dataset.map_batches(
                lambda batch: batch,
                udf_modifying_row_count=False,
            )
            self.assertIsNone(_estimate_dataset_size_bytes(one_to_one))
            self.assertEqual(_estimate_dataset_num_rows(one_to_one), 2)

            widened = dataset.map_batches(
                lambda batch: pa.table({
                    "payload": ["x" * 1_000_000] * batch.num_rows,
                }),
                batch_format="pyarrow",
                udf_modifying_row_count=False,
            )
            self.assertIsNone(_estimate_dataset_size_bytes(widened))
            self.assertEqual(_estimate_dataset_num_rows(widened), 2)
            self.assertGreater(widened.materialize().size_bytes(), 1_000_000)

    def test_sparse_row_ids_keep_shuffle_parallelism(self):
        with mock.patch(
            "ray.cluster_resources", return_value={"CPU": 320}
        ):
            self.assertEqual(
                _resolve_row_id_num_partitions(None, 4096, 500, 500),
                200,
            )
            self.assertEqual(
                _resolve_row_id_num_partitions(None, None, None, 500),
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

    def test_merge_estimate_uses_only_reliable_source_size(self):
        ctx = types.SimpleNamespace(
            is_self_merge=False,
        )

        with mock.patch(
            "pypaimon.ray.data_evolution_merge_into."
            "_estimate_dataset_size_bytes",
            return_value=100,
        ):
            result = _estimate_merge_input_size_bytes(
                object(), ctx,
            )

        self.assertEqual(result, 100)

    def test_merge_estimate_falls_back_when_source_size_is_unknown(self):
        ctx = types.SimpleNamespace(is_self_merge=False)
        with mock.patch(
            "pypaimon.ray.data_evolution_merge_into."
            "_estimate_dataset_size_bytes",
            return_value=None,
        ):
            result = _estimate_merge_input_size_bytes(
                object(), ctx,
            )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
