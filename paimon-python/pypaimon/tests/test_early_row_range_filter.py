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
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate import Predicate
from pypaimon.manifest.schema.data_file_meta import DataFileMeta


class TestManifestReadRowRangePerformance(unittest.TestCase):
    """Reproduce: scan planning constructs ALL manifest entries even when
    row_ranges only match a few files."""

    NUM_COMMITS = 20
    ROWS_PER_COMMIT = 10

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'manifest.merge-min-count': '1',
        })
        cls.catalog.create_table('default.test_row_range_perf', schema, False)
        cls.table = cls.catalog.get_table('default.test_row_range_perf')
        cls.pa_schema = pa_schema

        write_builder = cls.table.new_batch_write_builder()
        for i in range(cls.NUM_COMMITS):
            tw = write_builder.new_write()
            tc = write_builder.new_commit()
            start_id = i * cls.ROWS_PER_COMMIT
            data = pa.Table.from_pydict({
                'id': list(range(start_id, start_id + cls.ROWS_PER_COMMIT)),
                'value': [f'v{j}' for j in range(cls.ROWS_PER_COMMIT)],
            }, schema=pa_schema)
            tw.write_arrow(data)
            cmts = tw.prepare_commit()
            for msg in cmts:
                for nf in msg.new_files:
                    nf.first_row_id = start_id
            tc.commit(cmts)
            tw.close()
            tc.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_scan_constructs_all_entries_without_early_row_range_filter(self):
        """With manifest.merge-min-count=1, all entries are in one manifest.
        Querying with _ROW_ID BETWEEN 5 AND 14 should return 2 files, but
        the current code constructs DataFileMeta for ALL 20 entries because
        the row-range filter runs after full object construction."""
        construction_count = [0]
        original_init = DataFileMeta.__init__

        def counting_init(self_meta, *args, **kwargs):
            construction_count[0] += 1
            original_init(self_meta, *args, **kwargs)

        predicate = Predicate(method='between', index=None,
                              field='_ROW_ID', literals=[5, 14])

        with patch.object(DataFileMeta, '__init__', counting_init):
            rb = self.table.new_read_builder().with_filter(predicate)
            splits = rb.new_scan().plan().splits()

        total_files = sum(len(s.files) for s in splits)
        self.assertEqual(total_files, 2)

        actual = self.table.new_read_builder().with_filter(predicate) \
            .new_read().to_arrow(splits)
        self.assertEqual(sorted(actual.column('id').to_pylist()),
                         list(range(5, 15)))

        # Before fix: all 20 entries constructed. After fix: only matching ones.
        self.assertLess(
            construction_count[0], self.NUM_COMMITS,
            f"Early row-range filter should skip most entries, "
            f"but {construction_count[0]} DataFileMeta were constructed "
            f"(expected < {self.NUM_COMMITS})")


if __name__ == '__main__':
    unittest.main()
