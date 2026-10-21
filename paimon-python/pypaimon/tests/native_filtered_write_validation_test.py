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

"""Ignored events do not reach nested value validation or physical writers."""

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.write.write_dispatch import NativeTableWrite


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_NATIVE = [False, pytest.param(True, marks=pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed'))]


def _payload(mode, value):
    if mode == 'array':
        return pa.list_(pa.field('item', pa.int32(), nullable=False)), [value]
    if mode == 'row':
        return pa.struct([pa.field('value', pa.int32(), nullable=False)]), {'value': value}
    return pa.map_(pa.string(), pa.field('value', pa.int32(), nullable=False)), [('key', value)]


def _table(tmp_path, native, mode):
    payload_type, _ = _payload(mode, None)
    schema = pa.schema([('id', pa.int32()), ('op', pa.string()), ('payload', payload_type)])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, primary_keys=['id'], options={
        'bucket': '1', 'rowkind.field': 'op', 'ignore-delete': 'true',
        'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}), False)
    return catalog.get_table('default.t'), schema


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('mode', ['array', 'row', 'map'])
@pytest.mark.parametrize('ignored_kind', ['-D', '-U'])
@pytest.mark.parametrize('bucket', [None, 0])
def test_ignored_nested_nulls_leave_only_valid_rows(tmp_path, native, mode, ignored_kind, bucket):
    table, schema = _table(tmp_path, native, mode)
    _, invalid = _payload(mode, None)
    _, valid = _payload(mode, 42)
    data = pa.Table.from_pylist([
        {'id': 1, 'op': ignored_kind, 'payload': invalid},
        {'id': 2, 'op': '+I', 'payload': valid},
    ], schema=schema)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite) == native
    try:
        writer.write_arrow(data, bucket)
        messages = writer.prepare_commit()
        assert sum(file.row_count for message in messages for file in message.new_files) == 1
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist() == [
        {'id': 2, 'op': '+I', 'payload': valid}]


@pytest.mark.parametrize('native', _NATIVE)
@pytest.mark.parametrize('mode', ['array', 'row', 'map'])
def test_surviving_nested_nulls_are_rejected(tmp_path, native, mode):
    table, schema = _table(tmp_path, native, mode)
    _, invalid = _payload(mode, None)
    data = pa.Table.from_pylist([{'id': 1, 'op': '+I', 'payload': invalid}], schema=schema)
    writer = table.new_batch_write_builder().new_write()
    assert isinstance(writer, NativeTableWrite) == native
    try:
        with pytest.raises((ValueError, RuntimeError, pa.ArrowInvalid)):
            writer.write_arrow(data)
            writer.prepare_commit()
    finally:
        writer.abort()
