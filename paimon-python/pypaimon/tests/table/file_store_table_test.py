#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions

from pypaimon.table.file_store_table import FileStoreTable


class FileStoreTableTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('user_id', pa.int32()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string())
        ])
        schema = Schema.from_pyarrow_schema(cls.pa_schema, partition_keys=['dt'],
                                            options={CoreOptions.BUCKET.key(): "2"})
        cls.catalog.create_table('default.test_copy_with_new_options', schema, False)
        cls.table = cls.catalog.get_table('default.test_copy_with_new_options')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_copy_with_new_options(self):
        """Test copy method with new options."""
        new_options = {"new.option": "new_value", "bucket": "2"}

        # Call copy method
        copied_table = self.table.copy(new_options)

        # Verify the copied table is a new instance
        self.assertIsNot(copied_table, self.table)
        self.assertIsInstance(copied_table, FileStoreTable)

        # Verify the new option is added
        self.assertIn("new.option", copied_table.table_schema.options)
        self.assertEqual(copied_table.table_schema.options["new.option"], "new_value")

    def test_copy_raises_error_when_changing_bucket(self):
        """Test copy method raises ValueError when trying to change bucket number."""
        # Get current bucket value
        current_bucket = self.table.options.bucket()

        # Try to change bucket number to a different value
        new_bucket_value = current_bucket + 1
        new_options = {CoreOptions.BUCKET.key(): new_bucket_value}

        # Verify ValueError is raised
        with self.assertRaises(ValueError) as context:
            self.table.copy(new_options)

        self.assertIn("Cannot change bucket number", str(context.exception))
