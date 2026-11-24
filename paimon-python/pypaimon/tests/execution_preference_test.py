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

"""Explicit backend preferences survive the Native product defaults."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.read.plan import Plan
from pypaimon.read.read_dispatch import configure_indexed_read, native_plan_requested
from pypaimon.write.backend_dispatch import create_row_id_plan_execution


@pytest.mark.execution_defaults
@pytest.mark.parametrize('enabled', ['false', 'true'])
def test_explicit_planner_preference_wins_over_native_reader_default(enabled):
    table = SimpleNamespace(options=CoreOptions(Options({'scan.native-plan.enabled': enabled})))
    assert table.options.native_read_enabled()
    assert native_plan_requested(SimpleNamespace(table=table)) == (enabled == 'true')


@pytest.mark.python_read
def test_indexed_read_preserves_python_lane_without_changing_table_options():
    options = CoreOptions(Options({}))
    assert not options.native_read_enabled()
    table = SimpleNamespace(options=options, copy_without_time_travel=Mock(
        side_effect=AssertionError('Indexed read overrode the selected Python reader')))
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True):
        assert configure_indexed_read(table) is table
    assert options.options.to_map() == {}


@pytest.mark.execution_defaults
def test_python_write_preference_keeps_row_id_routing_in_python():
    from pypaimon.execution.python.write.row_id_plan import PythonRowIdPlan

    table = SimpleNamespace(options=CoreOptions(Options({'write.native.enabled': 'false'})))
    assert table.options.native_read_enabled()
    assert table.options.native_plan_enabled()
    with patch('pypaimon.execution.native.write.row_id_plan.create_native_row_id_plan',
               side_effect=AssertionError('Python write preference selected Native row-ID routing')):
        execution = create_row_id_plan_execution(table, Plan([], 7))
    assert isinstance(execution, PythonRowIdPlan)
    assert not execution.groups
