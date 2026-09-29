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

from pypaimon.write.file_store_write import FileStoreWrite


class _KeepingWriter:
    """Returns file lists without giving up the writer's own copies."""

    def __init__(self, files):
        self.committed_files = list(files)
        self.committed_changelog_files = []

    def prepare_commit(self):
        return list(self.committed_files)

    def prepare_changelog_commit(self):
        return list(self.committed_changelog_files)


def _file_store(writer, blob_consumer=None):
    file_store = object.__new__(FileStoreWrite)
    file_store.data_writers = {((), 0): writer}
    file_store.commit_identifier = 0
    file_store._runtime_total_buckets = {}
    file_store.blob_consumer = blob_consumer
    return file_store


class ManagedBlobWriteOwnershipTest(unittest.TestCase):

    def test_prepare_commit_keeps_files_for_writer_abort(self):
        writer = _KeepingWriter(["data"])
        messages = _file_store(writer).prepare_commit(1)

        self.assertEqual(messages[0].new_files, ["data"])
        self.assertFalse(messages[0].preserve_blob_files_on_abort)
        self.assertEqual(writer.committed_files, ["data"])

    def test_blob_consumer_marks_messages_to_preserve_packs(self):
        writer = _KeepingWriter(["pack.blob"])
        messages = _file_store(writer, blob_consumer=object()).prepare_commit(1)

        self.assertTrue(messages[0].preserve_blob_files_on_abort)
        self.assertEqual(writer.committed_files, ["pack.blob"])


if __name__ == "__main__":
    unittest.main()
