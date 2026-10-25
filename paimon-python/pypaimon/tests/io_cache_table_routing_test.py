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

"""io-cache routing of real tables whose target keeps stale copies of objects replaced on origin."""

import os
import shutil
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock
from urllib.parse import urlparse

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.filesystem.io_cache_file_io import IoCacheRoutingFileIO
from pypaimon.filesystem.io_cache_routing import IoCacheRouting
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.row.blob import BlobDescriptor

WAREHOUSE = "oss://bkt/warehouse"


class _Endpoint(LocalFileIO):
    """One OSS endpoint over a local directory; a cache also serves the copies it retained."""

    def __init__(self, root, retained=None):
        super().__init__()
        self.root = root
        self.retained = retained
        self.requests = []

    def _to_file(self, path):
        parsed = urlparse(path)
        if parsed.scheme != "oss":
            return super()._to_file(path)
        key = Path(parsed.netloc, parsed.path.lstrip("/"))
        if self.retained is not None and Path(self.retained, key).exists():
            return Path(self.retained, key)
        return Path(self.root, key)

    def new_input_stream(self, path):
        self.requests.append(path)
        return super().new_input_stream(path)

    def to_filesystem_path(self, path):
        # The routing filesystem calls this right before it opens or stats the file.
        self.requests.append(path)
        return super().to_filesystem_path(path)


class IoCacheTableRoutingTest(unittest.TestCase):
    """Origin holds the current objects; the target serves what it retained, as a stale cache would."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="io_cache_table_routing_test_")
        storage = os.path.join(self.temp_dir, "storage")
        self.origin = _Endpoint(storage)
        self.target = _Endpoint(storage, retained=os.path.join(self.temp_dir, "retained"))
        self.routing_enabled = True
        patcher = mock.patch("pypaimon.catalog.filesystem_catalog.FileIO.get", side_effect=self._file_io)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.catalog = CatalogFactory.create({"warehouse": WAREHOUSE})
        self.catalog.create_database("db", False)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _file_io(self, path, options=None):
        if not self.routing_enabled:
            return self.origin
        routing = IoCacheRouting.create({
            "fs.oss.endpoint": "http://cache",
            "io-cache.enabled": "true",
            "io-cache.endpoint": "http://cache",
            "io-cache.origin.endpoint": "http://origin",
            "io-cache.policy": "meta,read",
        })
        return IoCacheRoutingFileIO(routing, self.origin, lambda name: self.target)

    def _put(self, path, content, retained=False):
        key = urlparse(path)
        root = self.target.retained if retained else self.origin.root
        file = Path(root, key.netloc, key.path.lstrip("/"))
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(content)

    def _target_reads(self, needle):
        return [path for path in self.target.requests if needle in path]

    @staticmethod
    def _write(table, data):
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(data)
        write_builder.new_commit().commit(writer.prepare_commit())
        writer.close()

    @staticmethod
    def _read(table):
        read_builder = table.new_read_builder()
        return read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())

    def _external_descriptor(self):
        # Named like a Paimon data file, outside every table, and replaced on origin after the cache kept it.
        uri = "oss://bkt/import/data-" + str(uuid.uuid4()) + "-1.png"
        self._put(uri, b"NEW")
        self._put(uri, b"OLD", retained=True)
        return uri, BlobDescriptor(uri, 0, 3).serialize()

    def test_descriptor_fields_are_read_from_origin(self):
        pa_schema = pa.schema([("id", pa.int32()), ("payload", pa.large_binary())])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            "row-tracking.enabled": "true", "data-evolution.enabled": "true",
            "blob-descriptor-field": "payload"})
        self.catalog.create_table("db.descriptors", schema, False)
        table = self.catalog.get_table("db.descriptors")
        uri, descriptor = self._external_descriptor()
        self._write(table, pa.Table.from_pydict({"id": [1], "payload": [descriptor]}, schema=pa_schema))

        self.assertEqual([b"NEW"], self._read(table).column("payload").to_pylist())
        self.assertEqual([], self._target_reads("/import/"))

    def test_data_files_of_older_schema_prefixes_use_the_target(self):
        pa_schema = pa.schema([("id", pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema, options={"data-file.prefix": "older-"})
        self.catalog.create_table("db.prefixes", schema, False)
        self._write(self.catalog.get_table("db.prefixes"), pa.Table.from_pydict({"id": [1]}, schema=pa_schema))
        self.catalog.alter_table("db.prefixes", [SchemaChange.set_option("data-file.prefix", "newer-")], False)
        self._write(self.catalog.get_table("db.prefixes"), pa.Table.from_pydict({"id": [2]}, schema=pa_schema))
        self.target.requests.clear()

        # A fresh load only knows newer- from its schema; older- comes from the schema history.
        table = self.catalog.get_table("db.prefixes")
        self.assertEqual([1, 2], sorted(self._read(table).column("id").to_pylist()))
        self.assertTrue(self._target_reads("/older-"))
        self.assertTrue(self._target_reads("/newer-"))

    def test_branch_copies_route_the_data_files_of_the_branch(self):
        pa_schema = pa.schema([("id", pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema, options={"data-file.prefix": "main-"})
        self.catalog.create_table("db.branched", schema, False)
        main = self.catalog.get_table("db.branched")
        main.branch_manager().create_branch("dev")
        dev = self.catalog.get_table("db.branched$branch_dev")
        dev.schema_manager.commit_changes([SchemaChange.set_option("data-file.prefix", "dev-")])
        self._write(self.catalog.get_table("db.branched$branch_dev"),
                    pa.Table.from_pydict({"id": [1]}, schema=pa_schema))
        self.target.requests.clear()

        copy = main.copy({"branch": "dev"})
        self.assertEqual([1], self._read(copy).column("id").to_pylist())
        self.assertTrue(self._target_reads("/dev-"))

    def test_table_files_named_like_external_objects_still_use_the_target(self):
        pa_schema = pa.schema([("id", pa.int32())])
        self.catalog.create_table("db.plain", Schema.from_pyarrow_schema(pa_schema), False)
        table = self.catalog.get_table("db.plain")
        self._write(table, pa.Table.from_pydict({"id": [7]}, schema=pa_schema))
        self.target.requests.clear()

        self.assertEqual([7], self._read(table).column("id").to_pylist())
        self.assertTrue(self._target_reads("/db.db/plain/bucket-0/data-"))


if __name__ == "__main__":
    unittest.main()
