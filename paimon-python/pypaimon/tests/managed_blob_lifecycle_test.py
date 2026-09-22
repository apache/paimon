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
from unittest.mock import Mock, call

from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_commit import FileStoreCommit


class ManagedBlobLifecycleTest(unittest.TestCase):

    def test_commit_abort_deletes_sidecars_and_unpreserved_packs(self):
        file_io = Mock()
        commit = FileStoreCommit.__new__(FileStoreCommit)
        commit.table = Mock(file_io=file_io)

        data_file = Mock(
            external_path=None,
            file_path="/warehouse/table/bucket-0/data.avro",
            extra_files=[
                "data.avro.blobref",
                "data.avro.row",
                "pack.managed.blob",
            ],
        )
        message = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[data_file],
            preserve_blob_files_on_abort=False,
        )

        commit.abort([message])

        file_io.delete_quietly.assert_has_calls([
            call("/warehouse/table/bucket-0/data.avro"),
            call("/warehouse/table/bucket-0/data.avro.blobref"),
            call("/warehouse/table/bucket-0/data.avro.row"),
            call("/warehouse/table/bucket-0/pack.managed.blob"),
        ], any_order=True)

    def test_commit_abort_preserves_consumer_owned_blob_packs(self):
        file_io = Mock()
        commit = FileStoreCommit.__new__(FileStoreCommit)
        commit.table = Mock(file_io=file_io)

        data_file = Mock(
            external_path=None,
            file_path="/warehouse/table/bucket-0/data.avro",
            extra_files=["data.avro.blobref", "pack.managed.blob"],
        )
        blob_file = Mock(
            external_path=None,
            file_path="/warehouse/table/bucket-0/payload.blob",
            extra_files=[],
        )
        message = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[data_file, blob_file],
            preserve_blob_files_on_abort=True,
        )

        commit.abort([message])

        file_io.delete_quietly.assert_has_calls([
            call("/warehouse/table/bucket-0/data.avro"),
            call("/warehouse/table/bucket-0/data.avro.blobref"),
        ])
        deleted = [args[0] for args, _ in file_io.delete_quietly.call_args_list]
        self.assertNotIn("/warehouse/table/bucket-0/payload.blob", deleted)
        self.assertNotIn("/warehouse/table/bucket-0/pack.managed.blob", deleted)

    def test_commit_abort_deletes_resolved_data_file_when_extras_fail(self):
        file_io = Mock()
        commit = FileStoreCommit.__new__(FileStoreCommit)
        commit.table = Mock(file_io=file_io)

        class _BoomExtras(object):
            external_path = None
            file_path = "/warehouse/table/bucket-0/data.avro"

            @property
            def extra_files(self):
                raise RuntimeError("boom resolving extras")

        message = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[_BoomExtras()],
            preserve_blob_files_on_abort=False,
        )

        commit.abort([message])

        file_io.delete_quietly.assert_called_once_with(
            "/warehouse/table/bucket-0/data.avro")


if __name__ == "__main__":
    unittest.main()
