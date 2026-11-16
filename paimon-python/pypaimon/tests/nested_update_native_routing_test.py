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

"""Public table routing for nested-sequence-overwrite-on-equal=false.

Native 0.5.x does not implement the equal-sequence retention policy. When any
nested_update field sets the option to false, both Native read and Native write
must yield to Python even if the Rust runtime is installed and Native defaults
are enabled.
"""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.native_compatibility import (
    nested_update_overwrite_on_equal_false_reason,
    require_native_compatible,
)
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.write.table_write import BatchTableWrite
from pypaimon.write.write_dispatch import NativeBatchTableWrite


_NESTED = pa.struct([('key', pa.int64()), ('value', pa.string()), ('seq', pa.int64())])
_SCHEMA = pa.schema([
    ('id', pa.int64()),
    ('updates', pa.list_(_NESTED)),
])


def _options(overwrite=None, **extra):
    options = {
        'bucket': '1',
        'file.format': 'parquet',
        'merge-engine': 'aggregation',
        'fields.updates.aggregate-function': 'nested_update',
        'fields.updates.nested-key': 'key',
        'fields.updates.nested-sequence-field': 'seq',
        'read.native.enabled': 'true',
        'write.native.enabled': 'true',
        'commit.native.enabled': 'false',
    }
    if overwrite is not None:
        options['fields.updates.nested-sequence-overwrite-on-equal'] = overwrite
    options.update(extra)
    return options


def _table(tmp_path, overwrite=None, name='t', **extra):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / name)})
    catalog.create_database('default', True)
    catalog.create_table(
        'default.t',
        Schema.from_pyarrow_schema(_SCHEMA, primary_keys=['id'], options=_options(overwrite, **extra)),
        False)
    return catalog.get_table('default.t')


def _batch(first='first', second='second'):
    return pa.Table.from_pylist([
        {'id': 1, 'updates': [{'key': 1, 'value': first, 'seq': 7}]},
        {'id': 1, 'updates': [{'key': 1, 'value': second, 'seq': 7}]},
    ], schema=_SCHEMA)


def _read_values(table):
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    rows = builder.new_read().to_arrow(splits).to_pylist()
    assert len(rows) == 1
    return [item['value'] for item in rows[0]['updates']]


@pytest.mark.python_read
@pytest.mark.python_write
def test_compatibility_helper_only_flags_explicit_false():
    assert nested_update_overwrite_on_equal_false_reason(CoreOptions(Options({}))) is None
    assert nested_update_overwrite_on_equal_false_reason(CoreOptions(Options(_options('true')))) is None
    assert nested_update_overwrite_on_equal_false_reason(CoreOptions(Options(_options(None)))) is None
    reason = nested_update_overwrite_on_equal_false_reason(CoreOptions(Options(_options('false'))))
    assert reason is not None
    assert 'nested-sequence-overwrite-on-equal=false' in reason
    with pytest.raises(RuntimeError, match='without a Python fallback'):
        require_native_compatible(CoreOptions(Options(_options('false'))), allow_python_fallback=False)


@pytest.mark.python_read
@pytest.mark.python_write
def test_native_read_bypassed_for_overwrite_on_equal_false(tmp_path):
    table = _table(tmp_path, 'false', name='read_false')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        # Force Python write for the seed so this case isolates read routing.
        assert isinstance(writer, BatchTableWrite)
        assert not isinstance(writer, NativeBatchTableWrite)
        writer.write_arrow(_batch())
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    # Prove routing does not depend on the Rust runtime being absent.
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.read.table_read.NativeTableRead') as native_read, \
            patch('pypaimon.execution.native.write.native_write.create_native_write',
                  side_effect=AssertionError('create_native_write must not run for read')):
        values = _read_values(table)
    native_read.assert_not_called()
    assert values == ['first']


@pytest.mark.python_read
@pytest.mark.python_write
def test_native_write_bypassed_for_overwrite_on_equal_false(tmp_path):
    table = _table(tmp_path, 'false', name='write_false')
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.write.native_write.create_native_write') as create_native, \
            patch('pypaimon.write.write_dispatch.NativeBatchTableWrite',
                  side_effect=AssertionError('NativeBatchTableWrite selected')):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            assert isinstance(writer, BatchTableWrite)
            assert not isinstance(writer, NativeBatchTableWrite)
            writer.write_arrow(_batch())
            messages = writer.prepare_commit()
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()
    create_native.assert_not_called()

    # Re-open and read through the public API after commit / persistence.
    reopened = CatalogFactory.create({'warehouse': str(tmp_path / 'write_false')}).get_table('default.t')
    with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
            patch('pypaimon.execution.native.read.table_read.NativeTableRead') as native_read:
        assert _read_values(reopened) == ['first']
    native_read.assert_not_called()

    # A later equal-sequence write must still keep first after another commit.
    builder = reopened.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, BatchTableWrite)
        writer.write_arrow(_batch('ignored', 'also-ignored'))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    with patch('pypaimon.execution.native.read.table_read.NativeTableRead') as native_read:
        assert _read_values(reopened) == ['first']
    native_read.assert_not_called()


@pytest.mark.python_read
@pytest.mark.python_write
@pytest.mark.parametrize('overwrite', [None, 'true'])
def test_default_and_true_keep_native_eligible(tmp_path, overwrite):
    table = _table(tmp_path, overwrite, name='compat_%s' % (overwrite or 'default'))
    assert nested_update_overwrite_on_equal_false_reason(table.options) is None
    assert require_native_compatible(table.options, allow_python_fallback=True) is None
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(_batch('first', 'second'))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    # Default / explicit true keep historical equal-sequence overwrite.
    assert _read_values(table) == ['second']
