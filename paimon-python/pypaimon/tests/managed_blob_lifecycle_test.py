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

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, call, patch

from pypaimon.blob.managed_blob_reference_file import ManagedBlobReferenceFile, Reference
from pypaimon.blob.managed_blob_cleanup import managed_blob_deletion_paths
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.write.commit_message import CommitMessage
from pypaimon.execution.python.write.file_store_commit import FileStoreCommit


def _file(path, extra_files):
    return DataFileMeta(
        file_name=path.rsplit("/", 1)[-1],
        file_size=1,
        row_count=0,
        min_key=None,
        max_key=None,
        key_stats=None,
        value_stats=None,
        min_sequence_number=0,
        max_sequence_number=0,
        schema_id=0,
        level=0,
        extra_files=extra_files,
        file_path=path,
    )


class ManagedBlobLifecycleTest(unittest.TestCase):

    def test_cleanup_compares_qualified_local_references(self):
        parent = os.path.abspath('warehouse %?#/bucket-0')
        pack = parent + '/pack.managed.blob'
        sidecar = 'warehouse %?#/bucket-0/data.blobref'
        file_io = Mock()
        file_io.to_filesystem_path.side_effect = LocalFileIO.create().to_filesystem_path
        file_io.list_status.return_value = [Mock(base_name='data.blobref')]
        table = Mock(file_io=file_io, table_path=os.path.dirname(parent))
        reference = Reference(Path(parent).as_uri(), 'pack.managed.blob')
        with patch.object(ManagedBlobReferenceFile, 'read', return_value=[reference]):
            paths = managed_blob_deletion_paths(table, [sidecar])
        self.assertIn(pack, paths)
        file_io.list_status.assert_called_once_with(parent)

    def test_local_uri_cleanup_preserves_surviving_references(self):
        file_io = LocalFileIO.create()
        with tempfile.TemporaryDirectory(prefix='pack cleanup ') as directory:
            root = Path(directory)
            for name in ('space dir', 'literal%20', 'literal%?#'):
                parent = root / name
                parent.mkdir()
                pack = parent / 'pack.managed.blob'
                pack.write_bytes(b'payload')
                aborted, live = parent / 'aborted.blobref', parent / 'live.blobref'
                table = Mock(file_io=file_io, table_path=str(root))
                roots = (str(parent), parent.as_uri(), 'file:' + str(parent), os.path.relpath(str(parent)))
                for aborted_root in roots:
                    for live_root in roots:
                        with self.subTest(name=name, aborted_root=aborted_root, live_root=live_root):
                            reference = Reference(aborted_root, pack.name)
                            live_reference = Reference(live_root, pack.name)
                            ManagedBlobReferenceFile.write(file_io, str(aborted), [reference])
                            ManagedBlobReferenceFile.write(file_io, str(live), [live_reference])
                            paths = managed_blob_deletion_paths(table, [aborted.as_uri()])
                            self.assertEqual(paths, [aborted.as_uri()])
                            for path in paths:
                                file_io.delete_quietly(path)
                            self.assertTrue(pack.exists())
                            paths = managed_blob_deletion_paths(table, [str(live)])
                            self.assertEqual(set(paths), {str(live), str(pack)})

    def test_remote_cleanup_uses_full_reference_identity(self):
        root = 's3://bucket/table %?#/bucket-0'
        pack = root + '/pack.managed.blob'
        sidecar = root + '/aborted.blobref'
        for surviving_root in (root, 's3://other/table %?#/bucket-0'):
            with self.subTest(surviving_root=surviving_root):
                file_io = Mock()
                file_io.list_status.return_value = [Mock(base_name='live.blobref')]
                table = Mock(file_io=file_io, table_path='s3://bucket/table %?#')

                def references(_file_io, path):
                    return [Reference(root if path == sidecar else surviving_root, 'pack.managed.blob')]

                with patch.object(ManagedBlobReferenceFile, 'read', side_effect=references):
                    paths = managed_blob_deletion_paths(table, [sidecar])
                self.assertEqual(pack in paths, surviving_root != root)
                file_io.list_status.assert_called_once_with(root)

    def test_commit_abort_deletes_sidecars_and_unpreserved_packs(self):
        file_io = Mock()
        file_io.list_status.return_value = []
        commit = FileStoreCommit.__new__(FileStoreCommit)
        commit.table = Mock(file_io=file_io)

        data_file = _file(
            "/warehouse/table/bucket-0/data.avro",
            ["data.avro.blobref", "data.avro.row", "pack.managed.blob"],
        )
        message = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[data_file],
        )

        commit.abort([message])

        file_io.delete_quietly.assert_has_calls([
            call("/warehouse/table/bucket-0/data.avro"),
            call("/warehouse/table/bucket-0/data.avro.blobref"),
            call("/warehouse/table/bucket-0/data.avro.row"),
            call("/warehouse/table/bucket-0/pack.managed.blob"),
        ], any_order=True)

    def test_commit_abort_deletes_blob_packs(self):
        file_io = Mock()
        file_io.list_status.return_value = []
        commit = FileStoreCommit.__new__(FileStoreCommit)
        commit.table = Mock(file_io=file_io)

        data_file = _file(
            "/warehouse/table/bucket-0/data.avro",
            ["data.avro.blobref", "pack.managed.blob"],
        )
        blob_file = _file("/warehouse/table/bucket-0/payload.blob", [])
        message = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[data_file, blob_file],
        )

        commit.abort([message])

        file_io.delete_quietly.assert_has_calls([
            call("/warehouse/table/bucket-0/data.avro"),
            call("/warehouse/table/bucket-0/data.avro.blobref"),
            call("/warehouse/table/bucket-0/pack.managed.blob"),
            call("/warehouse/table/bucket-0/payload.blob"),
        ], any_order=True)

    def test_commit_abort_discovers_packs_from_sidecars(self):
        for listing_fails in (False, True):
            with self.subTest(listing_fails=listing_fails):
                file_io = Mock()
                file_io.list_status.return_value = []
                if listing_fails:
                    file_io.list_status.side_effect = OSError('injected')
                commit = FileStoreCommit.__new__(FileStoreCommit)
                commit.table = Mock(file_io=file_io, table_path='/warehouse/table')
                data_file = _file(
                    '/warehouse/table/bucket-0/data.avro', ['data.avro.blobref'])
                reference = Reference('bucket-0', 'pack.managed.blob')
                with patch.object(ManagedBlobReferenceFile, 'read', return_value=[reference]):
                    commit.abort([CommitMessage((), 0, [data_file])])
                deleted = {args[0] for args, _kwargs in file_io.delete_quietly.call_args_list}
                self.assertIn('/warehouse/table/bucket-0/data.avro', deleted)
                self.assertIn('/warehouse/table/bucket-0/data.avro.blobref', deleted)
                self.assertEqual(
                    '/warehouse/table/bucket-0/pack.managed.blob' in deleted, not listing_fails)

    def test_commit_abort_keeps_pack_when_surviving_sidecar_is_unreadable(self):
        file_io = Mock()
        file_io.list_status.return_value = [Mock(base_name='committed.avro.blobref')]
        commit = FileStoreCommit.__new__(FileStoreCommit)
        commit.table = Mock(file_io=file_io, table_path='/warehouse/table')
        data_file = _file('/warehouse/table/bucket-0/data.avro', ['data.avro.blobref'])
        with patch.object(ManagedBlobReferenceFile, 'read', side_effect=[
                [Reference('bucket-0', 'pack.managed.blob')], OSError('injected')]):
            commit.abort([CommitMessage((), 0, [data_file])])
        deleted = {args[0] for args, _kwargs in file_io.delete_quietly.call_args_list}
        self.assertNotIn('/warehouse/table/bucket-0/pack.managed.blob', deleted)

    def test_commit_abort_deletes_resolved_data_file_when_extras_fail(self):
        file_io = Mock()
        commit = FileStoreCommit.__new__(FileStoreCommit)
        commit.table = Mock(file_io=file_io)

        class _BoomExtras(object):
            def physical_path(self):
                return "/warehouse/table/bucket-0/data.avro"

            def collect_files(self, bucket_path=None):
                raise RuntimeError("boom resolving extras")

        message = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[_BoomExtras()],
        )

        commit.abort([message])

        file_io.delete_quietly.assert_called_once_with(
            "/warehouse/table/bucket-0/data.avro")

    def test_commit_abort_keeps_packs_when_listing_fails(self):
        file_io = Mock()
        file_io.list_status.side_effect = OSError("injected")
        commit = FileStoreCommit.__new__(FileStoreCommit)
        commit.table = Mock(file_io=file_io)
        data_file = _file(
            "/warehouse/table/bucket-0/data.avro",
            ["data.avro.blobref", "pack.managed.blob"],
        )
        message = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[data_file],
        )

        commit.abort([message])

        deleted = [args[0] for args, _kwargs in file_io.delete_quietly.call_args_list]
        self.assertIn("/warehouse/table/bucket-0/data.avro", deleted)
        self.assertIn("/warehouse/table/bucket-0/data.avro.blobref", deleted)
        self.assertNotIn("/warehouse/table/bucket-0/pack.managed.blob", deleted)


if __name__ == "__main__":
    unittest.main()
