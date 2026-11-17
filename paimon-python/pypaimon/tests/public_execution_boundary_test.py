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

"""Public imports and actual Native temporal reads keep Python execution unloaded."""

import subprocess
import sys

import pytest

from pypaimon.execution.native.runtime import native_runtime_available


def test_public_imports_and_shared_validation_do_not_load_either_executor():
    script = r'''
import importlib
import importlib.abc
import sys

class RejectExecutors(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution.python.', 'pypaimon.execution.native.')):
            raise AssertionError('Shared API loaded execution: ' + fullname)

sys.meta_path.insert(0, RejectExecutors())
for name in (
    'pypaimon.multimodal', 'pypaimon.api.rest_api', 'pypaimon.table.file_store_table',
    'pypaimon.catalog.table_query_auth', 'pypaimon.filesystem.local_file_io',
    'pypaimon.filesystem.pyarrow_file_io', 'pypaimon.ray.merge_condition',
    'pypaimon.ray.vector_index_build',
):
    importlib.import_module(name)
import pypaimon.globalindex as indexes
for name in indexes.__all__:
    getattr(indexes, name)
from pypaimon.schema.data_type_casts import supports_cast
from pypaimon.schema.data_types import AtomicType
assert supports_cast(AtomicType('INT'), AtomicType('BIGINT'))
assert not any(name.startswith(('pypaimon.execution.python.', 'pypaimon.execution.native.'))
               for name in sys.modules)
'''
    result = subprocess.run(
        [sys.executable, '-B', '-c', script], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stderr


@pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
def test_native_temporal_reads_never_import_python_execution(tmp_path):
    script = r'''
# NATIVE_TEMPORAL_BOUNDARY: the test requires the current SDK in this process.
import importlib.abc
import sys

class RejectPythonExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pypaimon.execution.python' or fullname.startswith('pypaimon.execution.python.'):
            raise AssertionError('Native temporal loaded Python execution: ' + fullname)

sys.meta_path.insert(0, RejectPythonExecution())
from pypaimon_rust.datafusion import SQLContext
context = SQLContext()
context.register_catalog('paimon', {'warehouse': sys.argv[1]})
context.sql('CREATE SCHEMA paimon.temporal_boundary')
for name, extra, values in (
    ('anchors', '', '(1, 1), (1, 3)'),
    ('samples', ', value INT', '(1, 0, 0), (1, 4, 40)'),
):
    definition = "CREATE TABLE paimon.temporal_boundary." + name + " (episode INT, event_time BIGINT" + extra
    context.sql(definition + """) WITH (
        'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')""")
    columns = '(episode, event_time)' if name == 'anchors' else '(episode, event_time, value)'
    context.sql('INSERT INTO paimon.temporal_boundary.' + name + ' ' + columns + ' VALUES ' + values)

from pypaimon import CatalogFactory
from pypaimon.multimodal import join_asof, interpolate, join_window
from pypaimon.multimodal.query import ScanQuery
catalog = CatalogFactory.create({'warehouse': sys.argv[1]})
options = {'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'}
anchors = ScanQuery(catalog.get_table('temporal_boundary.anchors').copy(options))
samples = ScanQuery(catalog.get_table('temporal_boundary.samples').copy(options)).select('value')
asof = join_asof(anchors, samples, on='event_time', by='episode')
assert asof.to_arrow()['value'].to_pylist() == [0, 0]
linear = interpolate(anchors, samples, on='event_time', by='episode')
assert linear.to_arrow()['value'].to_pylist() == [10., 30.]
window = join_window(anchors, samples, on='event_time', by='episode',
                     preceding=1, following=1, aggregations={'mean_value': ('value', 'mean')})
assert window.to_arrow()['mean_value'].to_pylist() == [0., 40.]
reader = asof.to_arrow_batch_reader(batch_size=1)
try:
    assert reader.read_next_batch().num_rows == 1
finally:
    reader.close()
assert not any(name.startswith('pypaimon.execution.python') for name in sys.modules)
'''
    result = subprocess.run(
        [sys.executable, '-B', '-c', script, str(tmp_path)], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, universal_newlines=True, timeout=90)
    assert result.returncode == 0, result.stderr
