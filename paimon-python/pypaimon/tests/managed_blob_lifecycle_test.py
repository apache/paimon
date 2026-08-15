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

from pypaimon.write.file_store_commit import FileStoreCommit


class ManagedBlobLifecycleTest(unittest.TestCase):

    def test_commit_abort_deletes_files_but_not_shared_managed_packs(self):
        file_io = Mock()
        commit = FileStoreCommit.__new__(FileStoreCommit)
        commit.table = Mock(file_io=file_io)

        data_file = Mock(
            external_path=None,
            file_path="/warehouse/table/bucket-0/data.avro",
            extra_files=["data.avro.blobref", "data.avro.row"],
        )
        message = Mock(
            new_files=[data_file],
            changelog_files=[],
            index_adds=[],
        )

        commit.abort([message])

        file_io.delete_quietly.assert_has_calls([
            call("/warehouse/table/bucket-0/data.avro"),
            call("/warehouse/table/bucket-0/data.avro.blobref"),
            call("/warehouse/table/bucket-0/data.avro.row"),
        ])
        self.assertNotIn(
            call("/warehouse/table/bucket-0/data.managed.blob"),
            file_io.delete_quietly.call_args_list,
        )


if __name__ == "__main__":
    unittest.main()
