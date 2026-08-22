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

import unittest

import pyarrow as pa

try:
    import ray
except ImportError:
    ray = None

from pypaimon.ray.ray_paimon import (
    _append_blob_affinity_keys,
    _blob_prefetch_windows,
)
from pypaimon.table.row.blob import BlobDescriptor


def _descriptor(uri, offset, length):
    return BlobDescriptor(uri, offset, length).serialize()


class _ReadCounter:
    def __init__(self):
        self.reads = 0

    def add(self, count):
        self.reads += count

    def get(self):
        return self.reads

    def reset(self):
        self.reads = 0


class _CountingFileIO:
    def __init__(self, counter):
        self.counter = counter

    def read_ranges_coalesced(self, ranges, parallelism):
        paths = {value[0] for value in ranges if value is not None}
        ray.get(self.counter.add.remote(len(paths)))
        return [
            None if value is None else bytes([value[1] + 1]) * value[2]
            for value in ranges
        ]


class BlobAffinityHelperTest(unittest.TestCase):
    def test_appends_uri_and_offset(self):
        batch = pa.table({
            "id": [1, 2, 3],
            "thumbnail": [None, None, None],
            "image": [
                _descriptor("oss://bucket/a", 20, 2),
                None,
                b"inline",
            ],
        })

        result = _append_blob_affinity_keys(
            batch, ["thumbnail", "image"], "key", "offset")

        keys = result.column("key").to_pylist()
        self.assertEqual(len(keys[0]), 16)
        self.assertEqual(keys[1], b"\0" * 16)
        self.assertEqual(keys[2], b"\0" * 16)
        self.assertEqual(result.column("offset").to_pylist(), [20, -1, -1])

    def test_prefetch_windows_end_on_function_batch_boundaries(self):
        batch = pa.table({
            "image": [
                _descriptor("oss://bucket/a", i * 4, 4)
                for i in range(5)
            ],
        })

        windows = list(_blob_prefetch_windows(
            batch, ["image"], fn_batch_size=2, max_bytes=8))

        self.assertEqual(windows, [(0, 2), (2, 4), (4, 5)])


@unittest.skipIf(ray is None, "ray is not installed")
class BlobAffinityRayTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.started_ray = not ray.is_initialized()
        if cls.started_ray:
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        if cls.started_ray:
            ray.shutdown()

    def test_uri_affinity_coalesces_across_function_batches(self):
        from pypaimon.ray import map_with_blobs

        counter = ray.remote(num_cpus=0)(_ReadCounter).remote()
        file_io = _CountingFileIO(counter)
        source = pa.table({
            "id": [1, 2, 3, 4],
            "image": [
                _descriptor("oss://bucket/a", 0, 1),
                _descriptor("oss://bucket/b", 0, 1),
                _descriptor("oss://bucket/a", 1, 1),
                _descriptor("oss://bucket/b", 1, 1),
            ],
        })

        def consume(scalar, blobs):
            return pa.table({
                "id": scalar.column("id"),
                "image_size": [len(value) for value in blobs["image"]],
                "fn_batch_size": [scalar.num_rows] * scalar.num_rows,
            })

        baseline = map_with_blobs(
            ray.data.from_arrow(source),
            ["image"],
            consume,
            file_io=file_io,
            all_blob_columns=["image"],
            batch_size=1,
        )
        self.assertEqual(len(baseline.take_all()), 4)
        self.assertEqual(ray.get(counter.get.remote()), 4)

        ray.get(counter.reset.remote())
        clustered = map_with_blobs(
            ray.data.from_arrow(source),
            ["image"],
            consume,
            file_io=file_io,
            all_blob_columns=["image"],
            batch_size=1,
            blob_uri_affinity=True,
            prefetch_bytes=16,
        )
        rows = sorted(clustered.take_all(), key=lambda row: row["id"])

        self.assertEqual([row["id"] for row in rows], [1, 2, 3, 4])
        self.assertEqual([row["fn_batch_size"] for row in rows], [1, 1, 1, 1])
        self.assertEqual(ray.get(counter.get.remote()), 2)


if __name__ == "__main__":
    unittest.main()
