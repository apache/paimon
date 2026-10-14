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

import unittest
from unittest.mock import Mock, call, patch

from pypaimon.blob.managed_blob_reference_file import ManagedBlobReferenceFile, Reference
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import FileStoreCommit


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
