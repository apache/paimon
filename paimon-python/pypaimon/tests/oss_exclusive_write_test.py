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

"""Exclusive OSS creation must not inherit metadata-write rename fallbacks."""

import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest import mock

import pytest

from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.filesystem.caching_file_io import CachingFileIO
from pypaimon.filesystem.oss_file_io import OssFileIO
from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
from pypaimon.multimodal.lerobot.export import _upload_to_object_table


def _oss_io(bucket):
    file_io = OssFileIO.__new__(OssFileIO)
    file_io._oss_bucket = "test-bucket"
    file_io._create_oss_bucket = lambda session: bucket
    file_io._sse_headers = lambda: {}
    return file_io


@pytest.mark.parametrize("status", ["Enabled", "Suspended"])
def test_exclusive_oss_claim_rejects_versioning_before_upload(tmp_path, status):
    oss2 = pytest.importorskip("oss2")
    bucket = mock.Mock()
    bucket.get_bucket_versioning.return_value = SimpleNamespace(status=status)
    file_io = _oss_io(bucket)
    file_io._atomic_write_supported = True  # A stale metadata-write cache is unsafe.
    file_io.exists = lambda path: False
    file_io.list_status = lambda path: []
    file_io.new_output_stream = mock.Mock(side_effect=AssertionError("uploaded"))
    stage = tmp_path / "stage"
    (stage / "meta").mkdir(parents=True)
    (stage / "meta/info.json").write_text("{}")
    (stage / "data.parquet").write_bytes(b"data")

    with mock.patch.object(oss2, "Session"):
        with pytest.raises(NotImplementedError, match="versioning"):
            _upload_to_object_table(stage, "oss://test-bucket/export", file_io)
    bucket.put_object.assert_not_called()
    file_io.new_output_stream.assert_not_called()


def test_exclusive_oss_claim_rejects_unknown_versioning():
    oss2 = pytest.importorskip("oss2")
    bucket = mock.Mock()
    bucket.get_bucket_versioning.side_effect = oss2.exceptions.ServerError(
        403, {}, b"", {"Code": "AccessDenied"})
    file_io = _oss_io(bucket)

    with mock.patch.object(oss2, "Session"):
        with pytest.raises(NotImplementedError, match="permission"):
            file_io.try_to_write_exclusive(
                "oss://test-bucket/export/claim", "claimed")
    bucket.put_object.assert_not_called()


def test_two_independent_oss_clients_have_one_exclusive_winner():
    oss2 = pytest.importorskip("oss2")
    objects = {}
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    class Bucket:
        def get_bucket_versioning(self):
            barrier.wait(timeout=5)
            return SimpleNamespace(status=None)

        def put_object(self, key, content, headers):
            assert headers["x-oss-forbid-overwrite"] == "true"
            with lock:
                if key in objects:
                    raise oss2.exceptions.ServerError(
                        409, {}, b"", {"Code": "FileAlreadyExists"})
                objects[key] = content

    file_ios = [_oss_io(Bucket()), _oss_io(Bucket())]
    path = "oss://test-bucket/export/meta/.pypaimon-export-claim"
    with mock.patch.object(oss2, "Session"):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(
                lambda file_io: file_io.try_to_write_exclusive(path, "claimed"),
                file_ios))
    assert sorted(results) == [False, True]
    assert objects == {"export/meta/.pypaimon-export-claim": b"claimed"}


def test_exclusive_claim_passes_through_file_io_wrappers():
    target = mock.Mock()
    target.try_to_write_exclusive.return_value = True
    cache = CachingFileIO.__new__(CachingFileIO)
    cache._delegate = target
    resolving = ResolvingFileIO.__new__(ResolvingFileIO)
    resolving._get_fileio = lambda path: cache
    rest = RESTTokenFileIO.__new__(RESTTokenFileIO)
    rest.file_io = lambda: resolving

    assert rest.try_to_write_exclusive("oss://test-bucket/claim", "claimed")
    target.try_to_write_exclusive.assert_called_once_with(
        "oss://test-bucket/claim", "claimed")
