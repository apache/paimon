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

"""Default execution, explicit Python selection and missing-runtime fallback."""

import sys

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.write.write_dispatch import NativeTableWrite


pytestmark = pytest.mark.execution_defaults


@pytest.mark.parametrize('mode', ['append', 'primary-key', 'data-evolution'])
@pytest.mark.parametrize('backend', [
    pytest.param('native', marks=pytest.mark.skipif(
        not native_runtime_available(), reason='pypaimon-rust not installed')),
    'python', 'missing-runtime',
])
def test_default_execution_roundtrip(native_rest_catalog, monkeypatch, mode, backend):
    options = {'bucket': '1' if mode == 'primary-key' else '-1'}
    if mode == 'data-evolution':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    if backend == 'python':
        options.update({key: 'false' for key in (
            'scan.native-plan.enabled', 'read.native.enabled',
            'write.native.enabled', 'commit.native.enabled')})
    elif backend == 'missing-runtime':
        monkeypatch.setitem(sys.modules, 'pypaimon_rust', None)
        monkeypatch.setitem(sys.modules, 'pypaimon_rust.datafusion', None)

    catalog = native_rest_catalog
    schema = pa.schema([('id', pa.int64()), ('value', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if mode == 'primary-key' else [], options=options), False)
    table = catalog.get_table('default.t')
    data = pa.Table.from_pylist([{'id': 1, 'value': 'a'}, {'id': 2, 'value': 'b'}], schema=schema)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    native = backend == 'native'
    try:
        assert isinstance(writer, NativeTableWrite) == native
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
        assert (commit._native_commit is not None) == native
        assert (commit._python_commit is None) == native
    finally:
        writer.close()
        commit.close()

    read_builder = table.new_read_builder()
    scan = read_builder.new_scan()
    splits = scan.plan().splits()
    assert scan._execution._is_native == native
    if native:
        def reject_python_read(*args, **kwargs):
            raise AssertionError('Default Native read fell back to Python')

        monkeypatch.setattr('pypaimon.execution.python.read.table_read.PythonTableRead._materialize',
                            reject_python_read)
    actual = read_builder.new_read().to_arrow(splits).to_pylist()
    assert sorted(actual, key=lambda row: row['id']) == data.to_pylist()
