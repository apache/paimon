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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Object events survive filtering, routing and Row-to-Arrow conversion."""

from typing import Any, List

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.projected_row import ProjectedRow
from pypaimon.table.row.row_kind import RowKind
from pypaimon.write.native_write import NativeTableWrite


@pytest.fixture(
    params=[
        pytest.param(False, marks=pytest.mark.python_write, id='python'),
        pytest.param(True, marks=pytest.mark.native_plan, id='native'),
    ]
)
def native(request):
    return request.param


def make_table(tmp_path, native, bucket='1', options=None, partition=False):
    fields: List[pa.Field] = [
        pa.field('id', pa.int32(), nullable=False),
        pa.field('v', pa.int32()),
    ]
    if options and 'rowkind.field' in options:
        fields.append(pa.field('op', pa.string()))
    if partition:
        fields.append(pa.field('pt', pa.string(), nullable=False))
    schema = pa.schema(fields)
    settings = {
        'bucket': bucket,
        'file.format': 'parquet',
        'write.native.enabled': str(native).lower(),
        'commit.native.enabled': 'false',
        'read.native.enabled': 'false',
        'scan.native-plan.enabled': 'false',
    }
    settings.update(options or {})
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table(
        'default.events',
        Schema.from_pyarrow_schema(
            schema,
            primary_keys=['id', 'pt'] if partition else ['id'],
            partition_keys=['pt'] if partition else [],
            options=settings,
        ),
        False,
    )
    table = catalog.get_table('default.events')
    assert isinstance(table, FileStoreTable)
    return table, schema


def new_builder(table, fixed=False):
    return (
        table.new_postpone_fixed_bucket_write_builder()
        if fixed
        else table.new_batch_write_builder()
    )


def assert_backend(writer, native):
    assert isinstance(writer, NativeTableWrite) == native
    if native:
        assert writer._python_writer is None
        assert writer._native_writer is not None
        assert type(writer._native_writer).__module__ == 'pypaimon_rust.datafusion'


def physical_rows(table, messages):
    return [
        row
        for message in messages
        for file in message.new_files
        for row in pq.ParquetFile(file.file_path).read().to_pylist()
    ]


def read_merged_rows(table):
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    assert splits and all(not split.raw_convertible for split in splits)
    assert sum(len(split.files) for split in splits) == 2
    return builder.new_read().to_arrow(splits).to_pylist()


@pytest.mark.parametrize('bucket', ['1', '-1', '-2'])
def test_object_delete_across_commits(tmp_path, native, bucket):
    table, _ = make_table(
        tmp_path, native, bucket, {'postpone.default-bucket-num': '1'}
    )
    for kind in (RowKind.INSERT, RowKind.DELETE):
        builder = new_builder(table, fixed=bucket == '-2')
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_row(GenericRow([1, 10], table.fields, kind))
            assert_backend(writer, native)
            messages = writer.prepare_commit()
            assert [row['_VALUE_KIND'] for row in physical_rows(table, messages)] == [
                kind.value
            ]
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()
    # Two overlapping files select merge reading, independently of delete-count metadata.
    assert read_merged_rows(table) == []


@pytest.mark.parametrize(
    'options,expected',
    [
        ({}, [0, 1, 2, 3]),
        ({'rowkind.field': 'op'}, [0, 1, 2, 3]),
        ({'ignore-delete': 'true'}, [0, 2]),
        ({'ignore-update-before': 'true'}, [0, 2, 3]),
        ({'ignore-delete': 'true', 'ignore-update-before': 'true'}, [0, 2]),
        ({'rowkind.field': 'op', 'ignore-delete': 'true'}, [0, 2]),
    ],
)
def test_object_events_and_configured_precedence(tmp_path, native, options, expected):
    # Ordinary postpone files preserve source events without merging equal keys.
    table, _ = make_table(tmp_path, native, '-2', options)
    writer = new_builder(table).new_write()
    try:
        for kind in RowKind:
            values: List[Any] = [kind.value, 10]
            object_kind = kind
            if 'rowkind.field' in options:
                values.append(kind.to_string())
                object_kind = RowKind((kind.value + 1) % 4)
            writer.write_row(GenericRow(values, table.fields, object_kind))
        assert_backend(writer, native)
        rows = physical_rows(table, writer.prepare_commit())
        assert [(row['id'], row['_VALUE_KIND']) for row in rows] == [
            (k, k) for k in expected
        ]
        assert all(row['v'] == 10 for row in rows)
    finally:
        writer.close()


@pytest.mark.parametrize(
    'bucket,fixed,options',
    [
        ('1', False, {'ignore-delete': 'true'}),
        ('-1', False, {'ignore-update-before': 'true'}),
        ('-2', True, {'ignore-delete': 'true'}),
    ],
)
def test_filter_precedes_invalid_routing(tmp_path, native, bucket, fixed, options):
    # Python's automatic postpone planner must also filter before sizing inputs.
    if native and fixed:
        options = dict(options, **{'postpone.default-bucket-num': '1'})
    table, _ = make_table(tmp_path, native, bucket, options, partition=True)
    writer = new_builder(table, fixed).new_write()
    try:
        kinds = (
            (RowKind.UPDATE_BEFORE, RowKind.DELETE)
            if 'ignore-delete' in options
            else (RowKind.UPDATE_BEFORE,)
        )
        for kind in kinds:
            # These values cannot be hashed, converted to Arrow, or used as a partition.
            writer.write_row(GenericRow([[], object(), []], table.fields, kind))
        assert_backend(writer, native)
        assert writer.prepare_commit() == []
        assert not list(tmp_path.rglob('*.parquet'))
    finally:
        writer.close()


def test_internal_row_events_and_arrow_default_insert(tmp_path, native):
    table, schema = make_table(tmp_path, native)
    builder = new_builder(table)
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist([{'id': 10, 'v': 10}], schema=schema))
        for kind in RowKind:
            writer.write_row(
                ProjectedRow([0, 1]).replace_row(
                    GenericRow([kind.value, 20], table.fields, kind)
                )
            )
        writer.write_row(GenericRow([11, 30], table.fields))
        writer.write_arrow(pa.Table.from_pylist([{'id': 12, 'v': 40}], schema=schema))
        assert_backend(writer, native)
        messages = writer.prepare_commit()
        rows = sorted(physical_rows(table, messages), key=lambda row: row['id'])
        assert [(row['id'], row['_VALUE_KIND']) for row in rows] == [
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 3),
            (10, 0),
            (11, 0),
            (12, 0),
        ]
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()


def test_stream_object_events(tmp_path, native):
    table, _ = make_table(tmp_path, native, '-2')
    writer = table.new_stream_write_builder().new_write()
    try:
        for checkpoint, kind in enumerate(RowKind):
            writer.write_row(GenericRow([checkpoint, 10], table.fields, kind))
            assert_backend(writer, native)
            rows = physical_rows(table, writer.prepare_commit(checkpoint))
            assert [row['_VALUE_KIND'] for row in rows] == [kind.value]
    finally:
        writer.close()


def test_arrow_filter_preserves_sliced_chunk_order_and_null_values(tmp_path, native):
    table, schema = make_table(
        tmp_path,
        native,
        '-2',
        {
            'rowkind.field': 'op',
            'ignore-delete': 'true',
            'ignore-update-before': 'true',
        },
    )
    # The slice excludes two valid rows and spans chunks with repeated keys.
    data = pa.table(
        {
            'id': [99, 7, 2, 7, 4, 2, 98],
            'v': [99, 10, 20, None, 40, None, 98],
            'op': ['+I', '+I', '-D', '+U', '-U', '+I', '+I'],
        },
        schema=schema,
    )
    data = pa.Table.from_batches(data.to_batches(max_chunksize=4)).slice(1, 5)
    writer = new_builder(table).new_write()
    try:
        writer.write_arrow(data)
        assert_backend(writer, native)
        messages = writer.prepare_commit()
        rows = physical_rows(table, messages)
        assert [(row['id'], row['v'], row['_VALUE_KIND']) for row in rows] == [
            (7, 10, 0),
            (7, None, 2),
            (2, None, 0),
        ]
        assert [row['_SEQUENCE_NUMBER'] for row in rows] == [0, 1, 2]
        assert all(
            file.delete_row_count == 0 for msg in messages for file in msg.new_files
        )
    finally:
        writer.close()


def test_empty_and_filtered_batches_do_not_affect_next_checkpoint(tmp_path, native):
    table, schema = make_table(
        tmp_path,
        native,
        '-2',
        {'rowkind.field': 'op', 'ignore-delete': 'true'},
    )
    writer = table.new_stream_write_builder().new_write()
    try:
        writer.write_arrow_batch(pa.RecordBatch.from_pylist([], schema=schema))
        writer.write_arrow_batch(
            pa.RecordBatch.from_pylist(
                [
                    {'id': 1, 'v': 10, 'op': '-U'},
                    {'id': 2, 'v': 20, 'op': '-D'},
                ],
                schema=schema,
            )
        )
        assert_backend(writer, native)
        assert writer.prepare_commit(0) == []
        assert not list(tmp_path.rglob('*.parquet'))

        writer.write_arrow_batch(
            pa.RecordBatch.from_pylist(
                [{'id': 3, 'v': None, 'op': '+I'}], schema=schema
            )
        )
        rows = physical_rows(table, writer.prepare_commit(1))
        assert [
            (row['id'], row['v'], row['_VALUE_KIND'], row['_SEQUENCE_NUMBER'])
            for row in rows
        ] == [(3, None, 0, 0)]
    finally:
        writer.close()


@pytest.mark.parametrize(
    'events,error',
    [
        (['+I', None], 'Unknown row kind string: None'),
        (['-D', 'invalid'], 'Unknown row kind string: invalid'),
        (['invalid', None], 'Unknown row kind string: invalid'),
        ([None, 'invalid'], 'Unknown row kind string: None'),
    ],
)
def test_invalid_arrow_events_reject_the_whole_batch(tmp_path, events, error):
    table, schema = make_table(
        tmp_path,
        False,
        '-2',
        {'rowkind.field': 'op', 'ignore-delete': 'true'},
    )
    writer = new_builder(table).new_write()
    try:
        data = pa.record_batch(
            {'id': [1, 2], 'v': [10, 20], 'op': events}, schema=schema
        )
        with pytest.raises(ValueError, match=error):
            writer.write_arrow_batch(data)
        assert writer.prepare_commit() == []
        assert not list(tmp_path.rglob('*.parquet'))
    finally:
        writer.close()
