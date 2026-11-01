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

"""Index contracts and Native planning remain independent of Python execution."""

from pathlib import Path
import subprocess
import sys
import textwrap


def test_public_index_contracts_and_native_plans_do_not_load_python_execution():
    script = r"""
import importlib.abc
import pickle
import sys
from contextlib import ExitStack
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

class RejectPythonExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('pypaimon.execution.python'):
            raise AssertionError('Native index planning imported Python execution: ' + fullname)

sys.meta_path.insert(0, RejectPythonExecution())
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.data_evolution_index_source_meta import DataEvolutionIndexSourceMeta
from pypaimon.index.deletion_vector_meta import DeletionVectorMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.index.pk.primary_key_index_definition import PrimaryKeyIndexFamily
from pypaimon.index.pk.primary_key_index_definitions import PrimaryKeyIndexDefinitions
from pypaimon.index.pk.primary_key_index_source_file import PrimaryKeyIndexSourceFile
from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.table.source.search_types import PrimaryKeyFullTextScanPlan, PrimaryKeyVectorScanPlan

# These are transport contracts, so retaining their original class identity
# matters independently of where the two executors keep their internal state.
source = PrimaryKeyIndexSourceFile('data.parquet', 3)
metadata = (
    PrimaryKeyIndexSourceMeta(1, [source]),
    DataEvolutionIndexSourceMeta(7),
    IndexFileMeta('btree', 'index', 17, 3,
                  {'data.parquet': DeletionVectorMeta('data.parquet', 4, 9, 1)},
                  global_index_meta=GlobalIndexMeta(0, 2, 0)),
)
transported = pickle.loads(pickle.dumps(metadata))
assert all(type(actual) is type(expected) for actual, expected in zip(transported, metadata))
assert transported[0].source_files == (source,)
assert transported[1].scan_snapshot_id == 7
assert transported[2].dv_ranges == metadata[2].dv_ranges
assert type(transported[2].global_index_meta) is GlobalIndexMeta

schema = TableSchema(fields=[
    DataField(0, 'id', AtomicType('INT')),
    DataField(1, 'embedding', ArrayType('ARRAY', AtomicType('FLOAT'))),
    DataField(2, 'body', AtomicType('STRING')),
], primary_keys=['id'], options={
    'bucket': '1', 'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true',
    'pk-btree.index.columns': 'id', 'pk-vector.index.columns': 'embedding',
    'fields.embedding.pk-vector.index.type': 'ivf-flat',
    'pk-full-text.index.columns': 'body',
})
definitions = PrimaryKeyIndexDefinitions.create(schema).definitions
assert [(value.column, value.field_id, value.family) for value in definitions] == [
    ('id', 0, PrimaryKeyIndexFamily.BTREE),
    ('embedding', 1, PrimaryKeyIndexFamily.VECTOR),
    ('body', 2, PrimaryKeyIndexFamily.FULL_TEXT),
]

with TemporaryDirectory() as directory, ExitStack() as stack:
    table = FileStoreTable(FileIO.get(directory), Identifier('default', 'indexed'), directory,
                           schema, CatalogEnvironment(catalog_loader=object()))
    predicate = PredicateBuilder(table.fields).greater_than('id', 0)
    native_plan = SimpleNamespace(splits=lambda: [], snapshot_id=lambda: 7, stats=lambda: None)
    core_scan = Mock()
    core_scan.plan.return_value = native_plan
    core_builder = Mock()
    core_builder.with_filter.return_value = core_builder
    core_builder.with_projection.return_value = core_builder
    core_builder.new_scan.return_value = core_scan
    stack.enter_context(patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True))
    stack.enter_context(patch('pypaimon.execution.native.table._resolved_schema_file_io_options', return_value={}))
    stack.enter_context(patch('pypaimon.execution.native.read.builder._native_read_builder',
                              return_value=core_builder))
    scan = table.new_read_builder().with_filter(predicate).new_scan()
    plan = scan.plan()
    assert plan.snapshot_id == 7 and plan.splits() == []
    assert scan._file_scanner is None
    core_builder.with_filter.assert_called_once_with(
        {'method': 'greaterThan', 'field': 'id', 'literals': [0]})
    core_scan.plan.assert_called_once_with()

    # Use the same Core Scan/Plan/Read protocol as the Native search adapters.
    search_plan = SimpleNamespace(snapshot=lambda: None, splits=lambda: [])
    result = SimpleNamespace(snapshot_id=lambda: None, positions=lambda: [], splits=lambda: [])
    reader = SimpleNamespace(read_plan=Mock(return_value=result))
    search_builder = Mock()
    search_builder.with_limit.return_value = search_builder
    search_builder.new_vector_search_scan.return_value.scan.return_value = search_plan
    search_builder.new_full_text_scan.return_value.scan.return_value = search_plan
    search_builder.new_vector_search_read.return_value = reader
    search_builder.new_full_text_read.return_value = reader
    native_table = Mock()
    native_table.new_vector_search_builder.return_value = search_builder
    native_table.new_full_text_search_builder.return_value = search_builder
    for module in ('native_vector_search', 'native_full_text_search'):
        prefix = 'pypaimon.execution.native.read.' + module
        stack.enter_context(patch(prefix + '.native_runtime_available', return_value=True))
        stack.enter_context(patch(prefix + '._native_table', return_value=native_table))
    stack.enter_context(patch('pypaimon.execution.native.read.native_vector_search._catalog_metastore',
                              return_value='rest'))
    stack.enter_context(patch('pypaimon.execution.native.read.native_full_text_search._rest_catalog_supported',
                              return_value=True))
    wire_plan = object()
    convert = stack.enter_context(patch(
        'pypaimon.execution.native.read.native_search_plan._plan_to_native', return_value=wire_plan))
    for family in ('vector', 'text'):
        if family == 'vector':
            builder = table.new_vector_search_builder().with_vector_column('embedding').with_limit(2)
            builder.with_query_vector([1., 0.])
            scan, read = builder.new_vector_search_scan(), builder.new_vector_search_read()
            expected_plan = PrimaryKeyVectorScanPlan
        else:
            builder = table.new_full_text_search_builder().with_query('body', 'needle').with_limit(2)
            scan, read = builder.new_full_text_scan(), builder.new_full_text_read()
            expected_plan = PrimaryKeyFullTextScanPlan
        plan = scan.scan()
        assert type(plan) is expected_plan and plan.splits() == []
        answer = read.read_plan(plan)
        assert answer.snapshot_id == 0 and answer.splits == ()
        assert scan._python_executor is None and read._python_executor is None
        assert convert.call_args[0][0] is plan
        reader.read_plan.assert_called_with(wire_plan)
    search_builder.with_vector_column.assert_called_with('embedding')
    search_builder.with_query.assert_called_with('body', 'needle')
    failure = OSError('Core query failed')
    reader.read_plan.side_effect = failure
    try:
        read.read_plan(plan)
    except OSError as error:
        assert error is failure
    else:
        raise AssertionError('Native query failure was hidden')
    assert read._python_executor is None
assert not any(name.startswith('pypaimon.execution.python') for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
