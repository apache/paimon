################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import shutil

import pyarrow as pa

import os
import tempfile

from pypaimon.api import Schema
from pypaimon.api.catalog_factory import CatalogFactory
from pypaimon.pynative.tests import PypaimonTestBase


class NativeFullTest(PypaimonTestBase):

    def testWriteAndRead(self):
        tempdir = tempfile.mkdtemp()
        warehouse = os.path.join(tempdir, 'warehouse')
        catalog = CatalogFactory.create({
            "warehouse": warehouse
        })
        simple_pa_schema = pa.schema([
            ('f0', pa.int32()),
            ('f1', pa.string()),
            ('f2', pa.string())
        ])
        data = {
            'f0': [1, 2, 3],
            'f1': ['a', 'b', 'c'],
            'f2': ['X', 'Y', 'Z']
        }
        expect = pa.Table.from_pydict(data, schema=simple_pa_schema)
        catalog.create_database("test_db", False)
        catalog.create_table("test_db.native_full", Schema(simple_pa_schema, options={}), False)
        table = catalog.get_table("test_db.native_full")

        # write
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(expect)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        # read
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()
        actual = table_read.to_arrow(splits)
        self.assertEqual(actual, expect)

        shutil.rmtree(tempdir, ignore_errors=True)
