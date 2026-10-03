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

"""Reader-side coverage for the remote (object-store) branch of
``FormatVortexReader``.

The pinned Vortex 0.70.0 object stores (``S3Store`` et al.) have no
``.open()`` method; the supported entry point is ``vortex.open(path,
store=...)``. This test drives the reader with the OSS store kwargs produced
by the real ``to_vortex_specified`` and a stand-in ``vortex`` module, and
asserts the reader opens the file through ``vortex.open(path, store=...)`` and
never calls ``store.open()`` -- the pre-existing mismatch this change fixes.

It fakes the SDK boundary so it runs on the normal Python lane without the
native ``vortex`` package; the real end-to-end OSS read is validated against
the pinned SDK in the native CI lane.
"""

import sys
import unittest
from unittest import mock

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import OssOptions
from pypaimon.schema.data_types import AtomicType, DataField


class _FakeArrowSchema:
    def __init__(self, names):
        self.names = names


class _FakeDType:
    def __init__(self, names):
        self._names = names

    def to_arrow_schema(self):
        return _FakeArrowSchema(self._names)


class _FakeScan:
    def to_arrow(self):
        return iter(())


class _FakeVortexFile:
    def __init__(self, names):
        self.dtype = _FakeDType(names)

    def scan(self, *args, **kwargs):
        return _FakeScan()


class _FakeStore:
    """A vortex object store stand-in. ``.open()`` must never be called: 0.70.0
    stores do not expose it, so hitting it means the reader regressed."""

    def open(self, *args, **kwargs):
        raise AssertionError(
            "store.open() must not be called; use vortex.open(path, store=...)")


class FormatVortexReaderStoreBranchTest(unittest.TestCase):

    def test_remote_store_opens_through_vortex_open_with_store(self):
        captured = {}
        fake_store_obj = _FakeStore()

        fake_store_module = mock.Mock()
        fake_store_module.from_url = mock.Mock(return_value=fake_store_obj)

        def fake_open(path, store=None):
            captured['path'] = path
            captured['store'] = store
            return _FakeVortexFile(['a'])

        fake_vortex = mock.Mock()
        fake_vortex.open = mock.Mock(side_effect=fake_open)
        fake_vortex.store = fake_store_module

        file_path = "oss://test-bucket/db.db/t/bucket-0/data.vortex"
        file_io = FileIO.get(file_path, Options({
            OssOptions.OSS_ENDPOINT.key(): "oss-region.example.com",
            OssOptions.OSS_ACCESS_KEY_ID.key(): "k",
            OssOptions.OSS_ACCESS_KEY_SECRET.key(): "s",
        }))
        read_fields = [DataField(0, 'a', AtomicType('INT'))]

        from pypaimon.read.reader.format_vortex_reader import FormatVortexReader
        with mock.patch.dict(
                sys.modules,
                {'vortex': fake_vortex, 'vortex.store': fake_store_module}):
            FormatVortexReader(file_io, file_path, read_fields, None)

        # Built the store from the generated OSS kwargs...
        fake_store_module.from_url.assert_called_once()
        _, from_url_kwargs = fake_store_module.from_url.call_args
        self.assertEqual(
            from_url_kwargs.get('endpoint'),
            "https://test-bucket.oss-region.example.com")
        # ...and opened via vortex.open(path, store=...), not store.open().
        fake_vortex.open.assert_called_once()
        self.assertIs(captured['store'], fake_store_obj)
        self.assertTrue(str(captured['path']).startswith('s3://test-bucket/'))


if __name__ == '__main__':
    unittest.main()
