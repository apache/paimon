################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""End-to-end Python read of a persisted partial-update file that *physically*
contains retraction rows (``_VALUE_KIND`` = DELETE / UPDATE_BEFORE).

No standard writer can persist retractions for a partial-update table, so this
coverage cannot be produced by writing rows:

* the native writer applies a ``RowKindFilter`` **before** persisting under
  ``ignore-delete``, so DELETE / UPDATE_BEFORE never reach the file;
* the Python ``KeyValueDataWriter`` hardcodes ``_VALUE_KIND = 0`` (see the
  ``# TODO: support real row kind here`` note) and its in-writer merge buffer
  would fold a retract away before flush anyway.

So the fixture is hand-crafted: a single level-0 KV data file whose
``_VALUE_KIND`` column carries DELETE(3) and UPDATE_BEFORE(1), described by a
``DataFileMeta`` with ``delete_row_count > 0`` and committed through the normal
commit machinery (which writes a real manifest + snapshot).

``delete_row_count > 0`` is load-bearing: it makes the split *not*
raw-convertible, so the read routes through ``SortMergeReaderWithMinHeap`` +
``PartialUpdateMergeFunction`` -- the retract-skip branch this PR adds -- rather
than ``RawFileSplitRead`` (which would bypass the merge function and leak the
raw retract rows). Deleting the skip branch makes the first retract raise
``NotImplementedError`` here, so the test is non-vacuous.
"""

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.commit_message import CommitMessage


def _partial_update_table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([
        pa.field('id', pa.int64(), nullable=False),
        pa.field('value', pa.int64(), nullable=True),
    ])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'], options={
            'bucket': '1',
            'file.format': 'parquet',
            'merge-engine': 'partial-update',
            'partial-update.ignore-delete': 'true',
            'read.native.enabled': 'false',
        }), False)
    return catalog.get_table('default.t')


def _persist_kv_file_with_retractions(table):
    """Write one KV data file holding physical retracts and return its meta.

    Physical layout mirrors ``KeyValueDataWriter._add_system_fields``:
    ``[_KEY_id, _SEQUENCE_NUMBER, _VALUE_KIND, <value side: id, value>]``,
    rows sorted by (``_KEY_id``, ``_SEQUENCE_NUMBER``) so the merge heap groups
    equal keys oldest-to-newest.

    * key 1: INSERT(seq 1) then DELETE(seq 2) -- existing key, keeps the insert
    * key 2: UPDATE_BEFORE(seq 3) only        -- retract-only key, must vanish
    """
    kv_schema = pa.schema([
        pa.field('_KEY_id', pa.int64(), nullable=False),
        pa.field('_SEQUENCE_NUMBER', pa.int64(), nullable=False),
        pa.field('_VALUE_KIND', pa.int8(), nullable=False),
        pa.field('id', pa.int64(), nullable=False),
        pa.field('value', pa.int64(), nullable=True),
    ])
    arrow_table = pa.Table.from_arrays([
        pa.array([1, 1, 2], type=pa.int64()),       # _KEY_id
        pa.array([1, 2, 3], type=pa.int64()),       # _SEQUENCE_NUMBER
        pa.array([0, 3, 1], type=pa.int8()),        # _VALUE_KIND: INSERT, DELETE, UPDATE_BEFORE
        pa.array([1, 1, 2], type=pa.int64()),       # id (value side)
        pa.array([100, 100, 200], type=pa.int64()),  # value (value side)
    ], schema=kv_schema)

    file_name = 'data-persisted-retractions-0.parquet'
    file_path = table.path_factory().bucket_path((), 0) + '/' + file_name
    table.file_io.write_parquet(file_path, arrow_table)

    id_field = table.trimmed_primary_keys_fields[0]
    return DataFileMeta(
        file_name=file_name,
        file_size=table.file_io.get_file_size(file_path),
        row_count=3,
        min_key=GenericRow([1], [id_field]),
        max_key=GenericRow([2], [id_field]),
        key_stats=SimpleStats(
            GenericRow([1], [id_field]), GenericRow([2], [id_field]), [0]),
        value_stats=SimpleStats.empty_stats(),
        min_sequence_number=1,
        max_sequence_number=3,
        schema_id=0,
        level=0,
        extra_files=[],
        # >0 => split is NOT raw-convertible => read goes through the merge
        # function (the retract-skip branch) instead of RawFileSplitRead.
        delete_row_count=2,
        value_stats_cols=[],
    )


def _read_sorted(table):
    read_builder = table.new_read_builder()
    splits = read_builder.new_scan().plan().splits()
    if not splits:
        return []
    return sorted(
        read_builder.new_read().to_arrow(splits).to_pylist(),
        key=lambda row: row['id'])


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.python_write
@pytest.mark.python_commit
def test_python_read_skips_persisted_partial_update_retractions(tmp_path):
    table = _partial_update_table(tmp_path)
    meta = _persist_kv_file_with_retractions(table)
    message = CommitMessage(
        partition=(), bucket=0, new_files=[meta], total_buckets=1)
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit([message])
    finally:
        commit.close()

    # key 1 keeps its insert (DELETE at seq 2 skipped); key 2 is retract-only
    # (UPDATE_BEFORE) and is absent -- exactly the ignore-delete semantics the
    # merge function implements, now proven over a persisted retract file.
    assert _read_sorted(table) == [{'id': 1, 'value': 100}]
