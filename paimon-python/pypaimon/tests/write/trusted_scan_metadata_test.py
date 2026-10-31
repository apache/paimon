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

"""Python writer metadata lookups stay outside public query authorization."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.execution.python.read.table_scan import PythonTableScan
from pypaimon.execution.python.write.bucket_metadata import dynamic_bucket_data_buckets
from pypaimon.execution.python.write.file_store_write import FileStoreWrite
from pypaimon.execution.python.write.postpone_bucket import PythonPostponeBucketPlanner


@pytest.mark.parametrize('operation', ['sequence', 'dynamic', 'postpone'])
def test_trusted_metadata_lookup_uses_private_python_scanner(operation):
    class Scan:
        @property
        def file_scanner(self):
            raise AssertionError('Writer metadata entered public scanner dispatch')

    scan = Scan()
    builder = Mock()
    builder.new_scan.return_value = scan
    builder.with_filter.return_value = builder
    builder.new_predicate_builder.return_value = SimpleNamespace(and_predicates=lambda values: None)
    table = SimpleNamespace(
        partition_keys=[], new_read_builder=lambda: builder,
        options=SimpleNamespace(scan_manifest_parallelism=lambda: 4))
    entries = [SimpleNamespace(
        bucket=bucket, total_buckets=8, partition=SimpleNamespace(values=['a']),
        file=SimpleNamespace(max_sequence_number=sequence))
        for bucket, sequence in [(3, 9), (4, 7)]]
    scanner = Mock(skip_level0=True)

    def plan_files():
        assert scanner.skip_level0 is False
        return entries

    scanner.plan_files.side_effect = plan_files
    scanner.manifest_scanner.return_value = (['manifest'], None)
    scanner.manifest_file_manager.read_entries_parallel.return_value = entries
    with patch.object(PythonTableScan, '_file_scanner', return_value=scanner) as private, \
            patch.object(PythonTableScan, '_scanner', side_effect=AssertionError('SELECT authorization')):
        if operation == 'sequence':
            writer = object.__new__(FileStoreWrite)
            writer.table = table
            writer.options = Mock(bucket=lambda: 8)
            writer.restore_snapshot_id = None
            writer._runtime_total_buckets = {}
            writer._previous_total_buckets = {}
            writer._sequence_read_table = lambda: table
            assert writer._load_seq_number_stats(()) == {3: 9, 4: 7}
        elif operation == 'dynamic':
            with patch('pypaimon.index.dynamic_bucket.bucket_read_builder', return_value=builder):
                assert dynamic_bucket_data_buckets(table, (), object()) == {3, 4}
        else:
            assert PythonPostponeBucketPlanner._load_bucket_metadata(table) == {('a',): 8}
            scanner.manifest_file_manager.read_entries_parallel.assert_called_once_with(
                ['manifest'], max_workers=4)
    private.assert_called_once_with()
