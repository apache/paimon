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
#  limitations under the License.
################################################################################

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.table.source.batch_vector_search_builder import (
    BatchVectorSearchBuilderImpl,
)
from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
from pypaimon.table.source.hybrid_search_builder import HybridSearchBuilderImpl
from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl

REJECTED = "Search is not supported on a query-auth table"


def _entry_points(table):
    return {
        "vector scan": VectorSearchBuilderImpl(table).new_vector_search_scan,
        "vector read": VectorSearchBuilderImpl(table).new_vector_search_read,
        "batch vector scan": BatchVectorSearchBuilderImpl(
            table).new_vector_search_scan,
        "batch vector read": BatchVectorSearchBuilderImpl(
            table).new_batch_vector_search_read,
        "full-text scan": FullTextSearchBuilderImpl(table).new_full_text_scan,
        "full-text read": FullTextSearchBuilderImpl(table).new_full_text_read,
        "hybrid routes": HybridSearchBuilderImpl(table).route_builders,
    }


class TestSearchRejectedUnderQueryAuth(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = CatalogFactory.create(
            {'warehouse': os.path.join(cls.tempdir, 'warehouse')})
        cls.catalog.create_database('db', False)
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('embedding', pa.list_(pa.float32())),
            ('text', pa.string()),
        ])
        cls.catalog.create_table(
            'db.plain', Schema.from_pyarrow_schema(pa_schema), False)
        cls.catalog.create_table(
            'db.authed',
            Schema.from_pyarrow_schema(
                pa_schema, options={'query-auth.enabled': 'true'}),
            False)
        cls.plain = cls.catalog.get_table('db.plain')
        cls.authed = cls.catalog.get_table('db.authed')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_every_scan_and_read_entry_point_is_rejected(self):
        self.assertTrue(self.authed.options.query_auth_enabled)
        for name, entry in _entry_points(self.authed).items():
            with self.subTest(entry=name):
                with self.assertRaises(ValueError) as ctx:
                    entry()
                self.assertIn(REJECTED, str(ctx.exception))

    def test_without_query_auth_the_builders_reach_their_own_validation(self):
        self.assertFalse(self.plain.options.query_auth_enabled)
        for name, entry in _entry_points(self.plain).items():
            with self.subTest(entry=name):
                with self.assertRaises(ValueError) as ctx:
                    entry()
                self.assertNotIn(REJECTED, str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
