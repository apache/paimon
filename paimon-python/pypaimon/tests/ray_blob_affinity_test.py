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
import weakref
from unittest.mock import patch

import pyarrow as pa

try:
    import ray
except ImportError:
    ray = None

from pypaimon.ray.ray_paimon import (
    _append_blob_affinity_keys,
    _blob_prefetch_windows,
    _map_blob_affinity_block,
)
from pypaimon.table.row.blob import BlobDescriptor, VideoFrameDescriptor


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

    def test_video_frames_share_payload_uri_key_and_preserve_offsets(self):
        batch = pa.table({"image": [
            _descriptor("oss://bucket/a", 10, 100),
            VideoFrameDescriptor("oss://bucket/a", 20, 100, 0).serialize(),
            VideoFrameDescriptor("oss://bucket/a", 20, 100, 1).serialize(),
            VideoFrameDescriptor("oss://bucket/b", 30, 100, 0).serialize(),
        ]})
        result = _append_blob_affinity_keys(batch, ["image"], "key", "offset")
        keys = result.column("key").to_pylist()
        self.assertEqual(keys[:3], [keys[0]] * 3)
        self.assertNotEqual(keys[3], keys[0])
        self.assertTrue(all(key != b"\0" * 16 for key in keys))
        self.assertEqual(result.column("offset").to_pylist(), [10, 20, 20, 30])

    def test_video_prefetch_windows_use_payload_length(self):
        batch = pa.table({"image": [
            VideoFrameDescriptor("oss://bucket/video-{}".format(i),
                                 0, 10 * 1024 * 1024, 0).serialize()
            for i in range(3)
        ]})
        windows = list(_blob_prefetch_windows(
            batch, ["image"], fn_batch_size=1, max_bytes=1024 * 1024))
        self.assertEqual(windows, [(0, 1), (1, 2), (2, 3)])

    def test_previous_payload_window_is_released_before_next_read(self):
        class Payload(bytearray):
            pass

        refs = []
        reads = []
        test = self

        class FileIO:
            def read_ranges_coalesced(self, ranges, parallelism):
                test.assertTrue(all(ref() is None for ref in refs))
                payloads = [Payload(b"x") for _ in ranges]
                refs.extend(weakref.ref(value) for value in payloads)
                reads.append(len(ranges))
                return payloads

        batch = pa.table({
            "id": list(range(4)),
            "image": [_descriptor("file:///video", i, 1) for i in range(4)],
        })
        outputs = list(_map_blob_affinity_block(
            batch, FileIO(), ["image"], ["image"], 1,
            lambda scalar, blobs: scalar, {},
            fn_batch_size=1, prefetch_bytes=2, affinity_cols=[]))
        self.assertEqual(reads, [2, 2])
        self.assertEqual([result.num_rows for result in outputs], [1] * 4)
        self.assertTrue(all(ref() is None for ref in refs))

    def test_affinity_rejects_nested_columns(self):
        for data_type, value in [
                (pa.list_(pa.binary()), [b"image"]),
                (pa.map_(pa.string(), pa.binary()), [("key", b"image")])]:
            batch = pa.table({"image": pa.array([value], type=data_type)})
            with self.assertRaisesRegex(ValueError, "scalar BLOB"):
                _append_blob_affinity_keys(batch, ["image"], "key", "offset")

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

    def test_gpu_arguments_keep_explicit_outer_and_inner_batch_sizes(self):
        from pypaimon.ray import map_with_blobs

        source = ray.data.from_arrow(pa.table({"image": [b"inline"]}))
        original = ray.data.Dataset.map_batches
        calls = []

        def capture(dataset, fn, **kwargs):
            calls.append(kwargs)
            return original(dataset, fn, **kwargs)

        for resources in ({"num_gpus": 1}, {"ray_remote_args": {"num_gpus": 1}}):
            with self.subTest(resources=resources), \
                    patch.object(ray.data.Dataset, "map_batches", capture):
                # Construct a real Ray plan without scheduling GPU work.
                map_with_blobs(
                    source, ["image"], lambda scalar, blobs: scalar,
                    file_io=object(), all_blob_columns=["image"],
                    batch_size=32, blob_uri_affinity=True, **resources)
                self.assertGreater(calls[-1]["batch_size"], 32)
                self.assertEqual(calls[-1]["fn_kwargs"]["fn_batch_size"], 32)
                self.assertEqual(calls[-1]["num_gpus"], 1)

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
