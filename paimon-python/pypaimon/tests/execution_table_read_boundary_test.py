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

"""Public special-table readers keep execution behind their operations."""

import subprocess
import sys
import textwrap
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa


def test_importing_and_configuring_special_table_readers_is_lazy():
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent("""
            import sys
            from pypaimon.table.format import FormatTableRead, FormatTableScan
            from pypaimon.table.object import ObjectTableRead, ObjectTableScan
            from pypaimon.table.system.system_table_read import SystemTableRead
            from pypaimon.table.system.system_table_scan import SystemTableScan

            table = object()
            FormatTableRead(table, projection=['id'], limit=1)
            FormatTableScan(table, partition_filter={'part': 'a'})
            ObjectTableRead(table, projection=['name'], limit=1)
            ObjectTableScan(table)
            SystemTableRead(table, [], predicate=None, limit=1)
            SystemTableScan(table)
            for module in (
                'format_table_read', 'format_table_scan',
                'object_table_read', 'object_table_scan',
                'system_table_read', 'system_table_scan',
            ):
                assert 'pypaimon.execution.python.read.' + module not in sys.modules, module
            assert 'pypaimon_rust' not in sys.modules
        """)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


def test_native_system_scan_never_materializes_the_python_provider():
    from pypaimon.table.system.system_table_scan import SystemSplit, SystemTableScan

    rows = pa.table({'id': [7, 11]})
    provider = SimpleNamespace(
        _build_arrow_table=Mock(side_effect=AssertionError('Python provider')),
        base_table=SimpleNamespace(options=SimpleNamespace(native_read_enabled=lambda: True)),
    )
    with patch('pypaimon.execution.native.read.system_table.try_read_native_system_table', return_value=rows), \
            patch.dict(sys.modules, {'pypaimon.execution.python.read.system_table_scan': None}):
        plan = SystemTableScan(provider).plan()
    provider._build_arrow_table.assert_not_called()
    split, = plan.splits()
    assert isinstance(split, SystemSplit)
    assert split.arrow_table().equals(rows)


def test_python_system_scan_materializes_once_and_keeps_the_public_split():
    from pypaimon.table.system.system_table_scan import SystemSplit, SystemTableScan

    rows = pa.table({'id': [3]})
    provider = SimpleNamespace(_build_arrow_table=Mock(return_value=rows))
    with patch('pypaimon.execution.native.read.system_table.try_read_native_system_table', return_value=None):
        plan = SystemTableScan(provider).plan()
    provider._build_arrow_table.assert_called_once_with()
    split, = plan.splits()
    assert isinstance(split, SystemSplit)
    assert split.arrow_table().equals(rows)
