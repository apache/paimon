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

"""Overwrite skips writer restoration and publishes a replacement HASH index."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.tests import native_plan_rest_test
from pypaimon.execution.native.write.native_write import NativeTableWrite


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
rest_catalog = native_plan_rest_test.rest_catalog
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]
_SCHEMA = pa.schema([('id', pa.int32()), ('pt', pa.string()), ('op', pa.string()), ('value', pa.int32())])


def _table(catalog, native, dynamic):
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['pt'], primary_keys=['id', 'pt'], options={
            'bucket': '-1' if dynamic else '1', 'dynamic-bucket.target-row-num': '1',
            'rowkind.field': 'op', 'ignore-delete': 'true', 'changelog-producer': 'input',
            'write.native.enabled': str(native).lower(), 'commit.native.enabled': str(native).lower(),
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}), False)
    return catalog.get_table('default.t')


def _write(builder, data, native, overwrite=False):
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, NativeTableWrite) == native
        writer.write_arrow(pa.Table.from_pylist(data, schema=_SCHEMA))
        messages = writer.prepare_commit()
        with ExitStack() as guards:
            if native:
                guards.enter_context(patch.object(
                    commit.file_store_commit, 'overwrite' if overwrite else 'commit',
                    side_effect=AssertionError('Python commit fallback')))
            commit.commit(messages)
        return messages
    finally:
        writer.close()
        commit.close()


def _rows(table):
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pylist()


def _hash_counts(table):
    snapshot = table.snapshot_manager().get_latest_snapshot()
    counts = {}
    for entry in IndexFileHandler(table).scan(snapshot):
        if entry.index_file.index_type == 'HASH':
            counts.setdefault(tuple(entry.partition.values), []).append(entry.index_file.row_count)
    return {partition: sorted(values) for partition, values in counts.items()}


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('dynamic,restore', [(False, None), (True, None), (True, 0), (True, 1)])
def test_overwrite_replaces_sequence_and_index_state_without_restoring_old_data(
        rest_catalog, native, dynamic, restore):
    catalog, _ = rest_catalog
    table = _table(catalog, native, dynamic)
    seed = _write(table.new_batch_write_builder(), [
        dict(id=1, pt='a', op='+I', value=value) for value in (10, 11, 12)
    ] + [dict(id=2, pt='b', op='+I', value=20)], native)
    assert max(file.max_sequence_number for message in seed if message.partition == ('a',)
               for file in message.new_files) == 2
    builder = table.new_batch_write_builder().overwrite({'pt': 'a'})
    if restore is not None:
        builder.with_restore_snapshot(restore)
    overwritten = _write(builder, [
        dict(id=3, pt='a', op='+I', value=99),
        dict(id=4, pt='a', op='-D', value=999),
        dict(id=5, pt='a', op='-U', value=999),
    ], native, overwrite=True)
    assert all(not message.changelog_files for message in overwritten)
    files = [file for message in overwritten for file in message.new_files]
    assert len(files) == 1
    assert (files[0].min_sequence_number, files[0].max_sequence_number) == (0, 0)
    assert _rows(table) == [dict(id=2, pt='b', op='+I', value=20), dict(id=3, pt='a', op='+I', value=99)]
    if dynamic:
        assert _hash_counts(table) == {('a',): [1], ('b',): [1]}

    # The next ordinary writer restores only the replacement's key and sequence
    # state, while untouched partitions retain their existing HASH files.
    appended = _write(table.new_batch_write_builder(), [
        dict(id=3, pt='a', op='+I', value=100), dict(id=6, pt='a', op='+I', value=60),
    ], native)
    same_bucket = [file for message in appended if message.bucket == 0 for file in message.new_files]
    assert min(file.min_sequence_number for file in same_bucket) == 1
    assert _rows(table) == [dict(id=2, pt='b', op='+I', value=20),
                            dict(id=3, pt='a', op='+I', value=100), dict(id=6, pt='a', op='+I', value=60)]
    if dynamic:
        assert _hash_counts(table) == {('a',): [1, 1], ('b',): [1]}
