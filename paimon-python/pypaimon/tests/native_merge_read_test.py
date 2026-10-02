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

"""Native merge capabilities must never bypass Python fallback validation."""

import sys
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.native_plan import native_reader_available
from pypaimon.write.native_commit import (
    create_native_write_table, from_native_commit_messages)


_SCHEMA = pa.schema([(name, pa.int64()) for name in ('id', 'seq', 'value')])
_ADVANCED = {
    'merge-engine': 'partial-update',
    'fields.seq.sequence-group': 'value',
    'fields.value.aggregate-function': 'sum',
}


def _table(tmp_path, options=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    opts = {'bucket': '1', 'file.format': 'parquet', 'read.native.enabled': 'true',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false'}
    opts.update(options or {})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, primary_keys=['id'], options=opts), False)
    return catalog.get_table('default.t')


@pytest.mark.native_plan
@pytest.mark.skipif(not native_reader_available(), reason='native reader required')
@pytest.mark.parametrize('options', [_ADVANCED, {
    'merge-engine': 'partial-update', 'ignore-delete': 'true',
}])
@pytest.mark.parametrize('streaming', [False, True])
def test_native_merge_reads_configurations_outside_python_subset(tmp_path, options, streaming):
    table = _table(tmp_path, options)
    # Separate commits require the reader to merge, even if the writer has
    # already combined all rows within each file.
    for seq, value in [(2, 10), (1, 3), (3, 7)]:
        writer = create_native_write_table(table).new_batch_write_builder().new_write()
        commit = table.new_batch_write_builder().new_commit()
        try:
            writer.write_arrow(pa.record_batch([[1], [seq], [value]], schema=_SCHEMA))
            messages = from_native_commit_messages(table, writer.prepare_commit())
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
        if streaming:
            batches = read.to_arrow_batch_reader(splits)
            try:
                actual = batches.read_all()
            finally:
                batches.close()
        else:
            actual = read.to_arrow(splits)
    assert actual.to_pylist() == [
        {'id': 1, 'seq': 3, 'value': 20 if options is _ADVANCED else 7}]


@pytest.mark.python_plan
@pytest.mark.python_write
@pytest.mark.parametrize('backend', ['setup_failure', 'missing_binding'])
@pytest.mark.parametrize('api', ['arrow', 'streaming', 'iterator'])
@pytest.mark.parametrize('parallelism', [1, 2])
def test_unsupported_python_merge_fails_before_raw_split_dispatch(
        tmp_path, backend, api, parallelism):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    commit = builder.new_commit()
    try:
        writer.write_arrow(pa.table([[1], [1], [10]], schema=_SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    splits = table.new_read_builder().new_scan().plan().splits()
    assert splits and all(split.raw_convertible for split in splits)
    # Keep the raw plan: its Python dispatch would bypass the merge reader.
    read = table.copy(_ADVANCED).new_read_builder().new_read()
    splits = splits * parallelism
    for split in splits:
        split._native_split = object()
    if backend == 'missing_binding':
        failure = patch.dict(sys.modules, {
            'pypaimon_rust': None, 'pypaimon_rust.datafusion': None})
    else:
        failure = patch('pypaimon.read.native_plan._prepare_native_read',
                        side_effect=RuntimeError('native setup failed'))
    with failure, patch.object(read, '_create_split_read') as dispatch:
        with pytest.raises(NotImplementedError, match='sequence-group'):
            if api == 'arrow':
                read.to_arrow(splits, parallelism=parallelism)
            elif api == 'streaming':
                read.to_arrow_batch_reader(splits, parallelism=parallelism).read_all()
            else:
                list(read.to_iterator(splits))
        dispatch.assert_not_called()


@pytest.mark.python_read
@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('options,match', [
    ({'sequence.field': 'missing'}, 'can not be found'),
    ({'sequence.field': 'seq,seq'}, 'defined repeatedly'),
    ({'sequence.field': 'seq', 'fields.seq.aggregate-function': 'sum'},
     'Should not define aggregation'),
    ({'sequence.field': 'seq', 'merge-engine': 'first-row'}, 'FIRST_ROW'),
])
def test_invalid_sequence_configuration_is_rejected_before_dispatch(tmp_path, native, options, match):
    table = _table(tmp_path).copy(dict(options, **{'read.native.enabled': str(native).lower()}))
    with pytest.raises(ValueError, match=match):
        table.new_read_builder().new_read()


@pytest.mark.python_read
def test_python_only_reader_retains_eager_merge_validation(tmp_path):
    table = _table(tmp_path, dict(_ADVANCED, **{'read.native.enabled': 'false'}))
    with pytest.raises(NotImplementedError, match='sequence-group'):
        table.new_read_builder().new_read()
