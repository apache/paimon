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

"""Nested scanner hooks retain the active snapshot, limits and statistics."""

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.file_scanner import FileScanner

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


@pytest.fixture
def two_file_table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32())])
    catalog.create_table('default.files', Schema.from_pyarrow_schema(schema, options={
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        'source.split.target-size': '1 b',
    }), False)
    table = catalog.get_table('default.files')
    for value in (1, 2):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.table({'id': [value]}, schema=schema))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    return table


def _assert_limited_snapshot(table, plan):
    assert plan.snapshot_id == 2
    assert len(plan.splits()) == 1
    assert sum(len(split.files) for split in plan.splits()) == 1
    read = table.new_read_builder().with_limit(1).new_read()
    assert read.to_arrow(plan.splits()).to_pylist() == [{'id': 1}]


def test_plan_files_hook_reconfiguration_updates_the_active_scan(two_file_table):
    scan = two_file_table.new_read_builder().new_scan()
    calls = []

    class LimitedFiles(FileScanner):
        def plan_files(self):
            calls.append(self)
            self.limit = 1
            return super().plan_files()

    scanner = LimitedFiles(scan)
    scan.file_scanner = scanner
    plan = scan.plan()
    _assert_limited_snapshot(two_file_table, plan)
    assert calls == [scanner]


def test_scan_hook_reconfiguration_keeps_active_statistics(two_file_table):
    scan = two_file_table.new_read_builder().new_scan()
    calls = []

    class LimitedScan(FileScanner):
        def scan(self):
            calls.append(self)
            self.limit = 1
            return super().scan()

    scanner = LimitedScan(scan)
    scan.file_scanner = scanner
    plan, stats = scan.scan_with_stats()
    _assert_limited_snapshot(two_file_table, plan)
    assert stats.manifest_files_total > 0
    assert stats.entries_total == 2
    assert plan.stats() is stats
    assert calls == [scanner]
