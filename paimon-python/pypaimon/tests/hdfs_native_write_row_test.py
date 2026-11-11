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

import pyarrow as pa

from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
from pypaimon.execution.python.read.reader.format_row_reader import (FormatRowReader)
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.execution.python.write.format_file_writer import write_row_file


class _LocalReadIO:
    def get_file_size(self, path):
        return os.path.getsize(path)

    def new_input_stream(self, path):
        return open(path, 'rb')


class HdfsNativeWriteRowTest(unittest.TestCase):
    """The Python row codec requires only the HDFS FileIO stream contract."""

    def test_write_row_roundtrips(self):
        fields = [DataField(0, "id", AtomicType("INT")),
                  DataField(1, "name", AtomicType("STRING"))]
        table = pa.table({"id": pa.array([1, 2, 3], pa.int32()),
                          "name": ["a", "b", "c"]})

        # Bypass HDFS initialization and exercise the output stream contract
        # against local storage, without starting an HDFS service.
        fio = object.__new__(HdfsNativeFileIO)
        fio.new_output_stream = lambda p: open(p, 'wb')
        fio.delete_quietly = lambda p: None

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "data.row")
            write_row_file(fio, path, table, fields=fields)

            reader = FormatRowReader(_LocalReadIO(), path,
                                     [f.name for f in fields], fields, None)
            batches = []
            while True:
                batch = reader.read_arrow_batch()
                if batch is None:
                    break
                batches.append(batch)
            reader.close()
            result = pa.Table.from_batches(batches)

        self.assertEqual(result.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(result.column("name").to_pylist(), ["a", "b", "c"])


if __name__ == '__main__':
    unittest.main()
