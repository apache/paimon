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
store=...)``. ``store.from_url(full_uri)`` prefixes the store with the full
object key, and ``vortex.open`` then resolves ``path`` *relative* to that
prefix -- so the reader must pass an empty ``path``; passing the key (or the
full URL) again doubles the prefix and 404s.

This test drives the reader with the OSS store kwargs produced by the real
``to_vortex_specified`` and a stand-in ``vortex`` module whose fake store
*models that prefix contract* (an empty path resolves to the single real
object key; anything else resolves to a doubled, non-existent key and raises).
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
    """A vortex 0.70.0 virtual-hosted object store stand-in.

    Models the prefix contract the maintainer verified against the real
    pinned SDK: ``store.from_url(full_uri)`` prefixes the store with the full
    object key, and ``vortex.open(path, store=...)`` resolves ``path``
    *relative* to that prefix (appending it). Only the exact stored object
    key resolves; any other resolved key is a 404 -- so a non-empty ``path``
    (the object key, or the full URL) doubles the prefix and fails, exactly
    as the real SDK does. ``.open()`` is absent on 0.70.0 stores, so calling
    it means the reader regressed.
    """

    def __init__(self, prefix, existing_keys):
        self._prefix = prefix
        self._existing = existing_keys

    def resolve(self, path):
        # ``path`` is relative to the store prefix; empty path == the prefix.
        if path == '':
            return self._prefix
        return self._prefix + '/' + path.lstrip('/')

    def open(self, *args, **kwargs):
        raise AssertionError(
            "store.open() must not be called; use vortex.open(path, store=...)")


class FormatVortexReaderStoreBranchTest(unittest.TestCase):

    def test_remote_store_opens_relative_to_the_file_prefixed_store(self):
        from urllib.parse import urlparse

        captured = {}

        def fake_from_url(url, **kwargs):
            captured['from_url_url'] = url
            captured['from_url_kwargs'] = kwargs
            key = urlparse(url).path.lstrip('/')
            # The store is prefixed at the full object key, and that key is the
            # only object that exists behind it.
            store_obj = _FakeStore(prefix=key, existing_keys={key})
            captured['store_obj'] = store_obj
            return store_obj

        fake_store_module = mock.Mock()
        fake_store_module.from_url = mock.Mock(side_effect=fake_from_url)

        def fake_open(path, store=None):
            captured['path'] = path
            captured['store'] = store
            resolved = store.resolve(path)
            captured['resolved'] = resolved
            if resolved not in store._existing:
                # Mirrors the real signed HEAD 404 on a doubled key.
                raise FileNotFoundError("404: no object at {!r}".format(resolved))
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
            # Reader construction performs the open; a doubled key would raise
            # FileNotFoundError here and fail the test.
            FormatVortexReader(file_io, file_path, read_fields, None)

        # Built the store from the generated OSS kwargs (virtual-hosted endpoint)...
        fake_store_module.from_url.assert_called_once()
        self.assertEqual(
            captured['from_url_kwargs'].get('endpoint'),
            "https://test-bucket.oss-region.example.com")
        # ...and opened via vortex.open(path, store=...), not store.open().
        fake_vortex.open.assert_called_once()
        self.assertIs(captured['store'], captured['store_obj'])
        # The path must be empty: the store is already prefixed with the full
        # object key, so any non-empty path doubles it and 404s. The resolved
        # key is the single real object key, with no scheme leaked.
        self.assertEqual(captured['path'], '')
        self.assertEqual(captured['resolved'], 'db.db/t/bucket-0/data.vortex')
        self.assertNotIn('s3://', str(captured['resolved']))


if __name__ == '__main__':
    unittest.main()
