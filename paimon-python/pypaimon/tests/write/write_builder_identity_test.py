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

"""Public commit identities survive builder reuse and both write modes."""

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


def _table(tmp_path, postpone=False):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    options = {
        'bucket': '-2' if postpone else '-1', 'postpone.default-bucket-num': '2',
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }
    if postpone:
        options.pop('row-tracking.enabled')
        options.pop('data-evolution.enabled')
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.data', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if postpone else [], options=options), False)
    return catalog.get_table('default.data'), schema


@pytest.mark.parametrize('stream', [False, True])
def test_public_builder_commit_user_is_captured_by_write_update_and_commit(tmp_path, stream):
    table, schema = _table(tmp_path)
    create_builder = table.new_stream_write_builder if stream else table.new_batch_write_builder
    builder = create_builder()
    assert builder.with_commit_user('application-1') is builder
    writer, commit, update = builder.new_write(), builder.new_commit(), builder.new_update()
    assert builder.with_commit_user('application-2') is builder
    # Reconfiguring a reusable builder does not change existing operations.
    assert writer.commit_user == commit.commit_user == update.commit_user == 'application-1'
    assert builder.new_update().commit_user == 'application-2'
    next_commit = builder.new_commit()
    try:
        assert next_commit.commit_user == 'application-2'
    finally:
        next_commit.close()

    try:
        writer.write_arrow(pa.table({'id': [1], 'value': [10]}, schema=schema))
        messages = writer.prepare_commit(7) if stream else writer.prepare_commit()
        commit.commit(messages, 7) if stream else commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    snapshot = table.snapshot_manager().get_latest_snapshot()
    assert snapshot.commit_user == 'application-1'
    assert snapshot.commit_identifier == (7 if stream else BATCH_COMMIT_IDENTIFIER)

    updater = update.new_update_by_row_id(8) if stream else update.new_update_by_row_id()
    assert updater.commit_user == 'application-1'
    messages = updater.update_columns(pa.table({'_ROW_ID': [0], 'value': [11]}), ['value'])
    recovered = create_builder().with_commit_user('application-1').new_commit()
    try:
        recovered.commit(messages, 8) if stream else recovered.commit(messages)
    finally:
        recovered.close()
    snapshot = table.snapshot_manager().get_latest_snapshot()
    assert snapshot.commit_user == 'application-1'
    assert snapshot.commit_identifier == (8 if stream else BATCH_COMMIT_IDENTIFIER)
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist() == [{'id': 1, 'value': 11}]


def test_postpone_builder_public_commit_user_reaches_the_committed_snapshot(tmp_path):
    table, schema = _table(tmp_path, postpone=True)
    builder = table.new_postpone_fixed_bucket_write_builder()
    assert builder.with_commit_user('postpone-job') is builder
    writer, commit = builder.new_write(), builder.new_commit()
    assert writer.commit_user == commit.commit_user == 'postpone-job'
    try:
        writer.write_arrow(pa.table({'id': [1, 2], 'value': [10, 20]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    snapshot = table.snapshot_manager().get_latest_snapshot()
    assert snapshot.commit_user == 'postpone-job'
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).sort_by('id').to_pylist() == [
        {'id': 1, 'value': 10}, {'id': 2, 'value': 20}]
