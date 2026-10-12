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

"""PK BLOB filtering and LIMIT must leave unselected payloads unopened."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory
from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.row.blob import BlobDescriptor
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan


def _table(tmp_path, engine='deduplicate', batch_size=8192, partitioned=False):
    from pypaimon_rust.datafusion import SQLContext

    options = {'bucket': '1', 'merge-engine': engine, 'read.batch-size': str(batch_size),
               'blob-descriptor-field': 'payload,output', 'read.native.enabled': 'true',
               'write.native.enabled': 'true', 'scan.native-plan.enabled': 'true'}
    if engine == 'aggregation':
        options['fields.default-aggregate-function'] = 'last_value'
    ctx = SQLContext()
    ctx.register_catalog('paimon', {'warehouse': str(tmp_path)})
    ctx.sql('CREATE SCHEMA paimon.pkblob')
    columns = 'id INT, tag INT, payload BLOB, output BLOB'
    key, partition = 'PRIMARY KEY (id)', ''
    if partitioned:
        columns += ', pt STRING'
        key, partition = 'PRIMARY KEY (pt, id)', ' PARTITIONED BY (pt)'
    properties = ', '.join("'%s' = '%s'" % item for item in options.items())
    ctx.sql('CREATE TABLE paimon.pkblob.t (' + columns + ', ' + key + ')' + partition
            + ' WITH (' + properties + ')')
    table = CatalogFactory.create({'warehouse': str(tmp_path)}).get_table('pkblob.t')
    if engine == 'first-row':
        # Java pk clustering permits first-row DVs. Loaded options let the
        # reader consume level-0 files directly, without requiring compaction.
        table = table.copy({'pk-clustering-override': 'true', 'deletion-vectors.enabled': 'true',
                            'deletion-vectors.merge-on-read': 'true'})
    return table


def _descriptor(tmp_path, name, value=None):
    file = tmp_path / name
    if value is not None:
        file.write_bytes(value)
    return BlobDescriptor(file.as_uri(), 0, len(value) if value is not None else 8).serialize()


def _write(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    assert isinstance(writer, NativeTableWrite)
    try:
        writer.write_arrow(pa.Table.from_pylist(
            rows, schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        commit.commit(writer.prepare_commit())
        assert writer._python_writer is None
    finally:
        writer.close()
        commit.close()


def _read(table, projection, predicate=None, limit=None, parallelism=1, streaming=False, splits=None):
    builder = table.new_read_builder().with_projection(projection)
    if predicate is not None:
        builder.with_filter(predicate)
    if limit is not None:
        builder.with_limit(limit)
    splits = builder.new_scan().plan().splits() if splits is None else splits
    reader = builder.new_read()
    with patch.object(reader, '_create_split_read', side_effect=AssertionError('Python read fallback')):
        if streaming:
            with reader.to_arrow_batch_reader(splits, parallelism=parallelism, blob_parallelism=2) as batches:
                return pa.Table.from_batches(list(batches), schema=batches.schema).to_pylist()
        return reader.to_arrow(splits, parallelism=parallelism, blob_parallelism=2).to_pylist()


@pytest.mark.parametrize('engine', ['deduplicate', 'first-row', 'partial-update', 'aggregation'])
@pytest.mark.parametrize('batch_size', [1, 8192])
@pytest.mark.parametrize('payload_filter', [False, True])
def test_pk_blob_limit_stops_before_later_invalid_uri(tmp_path, engine, batch_size, payload_filter):
    table = _table(tmp_path, engine, batch_size)
    payload = _descriptor(tmp_path, 'payload', b'selected')
    output = _descriptor(tmp_path, 'output', b'returned')
    missing = _descriptor(tmp_path, 'missing')
    _write(table, [dict(id=1, tag=1, payload=payload, output=output),
                   dict(id=2, tag=1, payload=missing, output=missing)])
    pb = table.new_read_builder().new_predicate_builder()
    predicate = pb.equal('payload', b'selected') if payload_filter else None
    assert _read(table, ['id', 'payload', 'output'], predicate, 1) == [
        dict(id=1, payload=b'selected', output=b'returned')]


@pytest.mark.parametrize('limit', [None, 1])
@pytest.mark.parametrize('streaming', [False, True])
def test_pk_output_blob_is_resolved_only_for_matching_rows(tmp_path, limit, streaming):
    table = _table(tmp_path)
    rejected = _descriptor(tmp_path, 'rejected', b'no')
    selected = _descriptor(tmp_path, 'selected', b'yes')
    output = _descriptor(tmp_path, 'output', b'returned')
    missing = _descriptor(tmp_path, 'missing')
    _write(table, [dict(id=1, tag=1, payload=rejected, output=missing),
                   dict(id=2, tag=1, payload=selected, output=output)])
    pb = table.new_read_builder().new_predicate_builder()
    assert _read(table, ['output', 'id'], pb.equal('payload', b'yes'), limit,
                 streaming=streaming) == [dict(output=b'returned', id=2)]


def test_pk_ordinary_conjunct_filters_before_payload_access(tmp_path):
    table = _table(tmp_path)
    payload = _descriptor(tmp_path, 'payload', b'yes')
    missing = _descriptor(tmp_path, 'missing')
    _write(table, [dict(id=1, tag=0, payload=missing, output=missing),
                   dict(id=2, tag=1, payload=payload, output=payload)])
    pb = table.new_read_builder().new_predicate_builder()
    predicate = pb.and_predicates([pb.equal('tag', 1), pb.equal('payload', b'yes')])
    assert _read(table, ['id'], predicate, 1) == [dict(id=2)]


@pytest.mark.parametrize('batch_size', [1, 8192])
def test_pk_blob_limit_shrinks_candidate_batches_after_rejected_rows(tmp_path, batch_size):
    table = _table(tmp_path, batch_size=batch_size)
    missing = _descriptor(tmp_path, 'must-not-open-after-second-match')
    rejected = _descriptor(tmp_path, 'rejected', b'no')
    selected = _descriptor(tmp_path, 'selected', b'yes')
    _write(table, [dict(id=1, tag=1, payload=rejected, output=missing),
                   dict(id=2, tag=1, payload=selected, output=selected),
                   dict(id=3, tag=1, payload=selected, output=selected),
                   dict(id=4, tag=1, payload=missing, output=missing)])
    pb = table.new_read_builder().new_predicate_builder()
    assert _read(table, ['id', 'output'], pb.equal('payload', b'yes'), 2) == [
        dict(id=2, output=b'yes'), dict(id=3, output=b'yes')]


@pytest.mark.parametrize('limit', [None, 1, 2])
def test_pk_blob_or_short_circuits_payload_access(tmp_path, limit):
    table = _table(tmp_path)
    missing = _descriptor(tmp_path, 'unneeded-branch')
    selected = _descriptor(tmp_path, 'selected', b'yes')
    rejected = _descriptor(tmp_path, 'rejected', b'no')
    _write(table, [dict(id=1, tag=1, payload=missing, output=missing),
                   dict(id=2, tag=0, payload=selected, output=missing),
                   dict(id=3, tag=0, payload=rejected, output=missing)])
    pb = table.new_read_builder().new_predicate_builder()
    predicate = pb.or_predicates([pb.equal('tag', 1), pb.equal('payload', b'yes')])
    assert _read(table, ['id'], predicate, limit) == [dict(id=1), dict(id=2)][:limit]


def test_pk_blob_nested_boolean_branches_only_resolve_pending_rows(tmp_path):
    table = _table(tmp_path)
    missing = _descriptor(tmp_path, 'unneeded-branch')
    selected = _descriptor(tmp_path, 'selected', b'yes')
    _write(table, [dict(id=1, tag=1, payload=missing, output=missing),
                   dict(id=2, tag=0, payload=selected, output=missing)])
    pb = table.new_read_builder().new_predicate_builder()
    predicate = pb.or_predicates([
        pb.and_predicates([pb.equal('tag', 1), pb.equal('id', 1)]),
        pb.and_predicates([pb.equal('payload', b'yes'), pb.equal('payload', b'yes')])])
    assert _read(table, ['id'], predicate) == [dict(id=1), dict(id=2)]


def test_pk_blob_predicate_payload_is_reused_for_output(tmp_path):
    table = _table(tmp_path)
    inner_descriptor = _descriptor(tmp_path, 'must-not-resolve-returned-bytes')
    payload = _descriptor(tmp_path, 'outer', inner_descriptor)
    _write(table, [dict(id=1, tag=1, payload=payload, output=payload)])
    pb = table.new_read_builder().new_predicate_builder()
    assert _read(table, ['payload'], pb.equal('payload', inner_descriptor)) == [
        dict(payload=inner_descriptor)]


@pytest.mark.parametrize('limit', [None, 1])
@pytest.mark.parametrize('not_null', [False, True])
def test_pk_blob_null_predicates_do_not_open_payload(tmp_path, limit, not_null):
    table = _table(tmp_path)
    missing = _descriptor(tmp_path, 'must-not-read-for-null-check')
    _write(table, [dict(id=1, tag=1, payload=missing, output=missing),
                   dict(id=2, tag=1, payload=None, output=None)])
    pb = table.new_read_builder().new_predicate_builder()
    predicate = pb.is_not_null('payload') if not_null else pb.is_null('payload')
    assert _read(table, ['id'], predicate, limit) == [dict(id=1 if not_null else 2)]


@pytest.mark.parametrize('engine', ['deduplicate', 'first-row', 'partial-update', 'aggregation'])
def test_pk_blob_limit_and_payload_filter_run_after_merge(tmp_path, engine):
    table = _table(tmp_path, engine)
    good = _descriptor(tmp_path, 'good', b'kept')
    missing = _descriptor(tmp_path, 'discarded-version')
    older, newer = (good, missing) if engine == 'first-row' else (missing, good)
    for reference in (older, newer):
        _write(table, [dict(id=1, tag=1, payload=reference, output=reference)])
    pb = table.new_read_builder().new_predicate_builder()
    assert _read(table, ['id', 'output'], pb.equal('output', b'kept'), 1) == [
        dict(id=1, output=b'kept')]


@pytest.mark.parametrize('parallelism', [None, 1, 4])
@pytest.mark.parametrize('streaming', [False, True])
def test_pk_blob_limit_is_shared_across_partitions(tmp_path, parallelism, streaming):
    table = _table(tmp_path, partitioned=True)
    payload = _descriptor(tmp_path, 'payload', b'yes')
    missing = _descriptor(tmp_path, 'missing')
    _write(table, [dict(id=1, tag=1, payload=payload, output=payload, pt='a'),
                   dict(id=2, tag=1, payload=missing, output=missing, pt='b')])
    splits = sorted(table.new_read_builder().new_scan().plan().splits(),
                    key=lambda split: split.files[0].file_path)
    assert len(splits) == 2
    assert _read(table, ['id', 'payload'], limit=1, parallelism=parallelism,
                 streaming=streaming, splits=splits) == [dict(id=1, payload=b'yes')]


def test_pk_blob_zero_limit_and_descriptor_output_require_no_payload(tmp_path):
    table = _table(tmp_path)
    missing = _descriptor(tmp_path, 'missing')
    _write(table, [dict(id=1, tag=1, payload=missing, output=missing)])
    splits = table.new_read_builder().new_scan().plan().splits()
    assert _read(table, ['payload'], limit=0, splits=splits) == []
    assert _read(table.copy({'blob-as-descriptor': 'true'}), ['payload'], limit=1) == [dict(payload=missing)]
    with pytest.raises(ValueError, match='missing'):
        _read(table, ['payload'], limit=1)


@pytest.mark.parametrize('payload_filter', [False, True])
def test_streaming_pk_blob_row_kinds_resolve_only_selected_payload(tmp_path, payload_filter):
    table = _table(tmp_path)
    payload = _descriptor(tmp_path, 'payload', b'selected')
    missing = _descriptor(tmp_path, 'unselected')
    _write(table, [dict(id=1, tag=1, payload=payload, output=payload),
                   dict(id=2, tag=1, payload=missing, output=missing)])
    builder = table.new_stream_read_builder().with_projection(['id', 'payload'])
    scan = builder.new_streaming_scan()
    if table.options.native_plan_enabled():
        scan.restore(table.snapshot_manager().get_latest_snapshot().id)
        splits = scan.plan().splits()
    else:
        splits = scan._create_delta_plan(table.snapshot_manager().get_latest_snapshot()).splits()
    assert splits and all(split.is_streaming for split in splits)
    predicate = builder.new_predicate_builder().equal('payload', b'selected') if payload_filter else None
    reader = TableRead(table, predicate, builder.read_type(), include_row_kind=True, limit=1)
    with patch.object(reader, '_create_split_read', side_effect=AssertionError('Python read fallback')):
        assert reader.to_arrow(splits).to_pylist() == [
            {'_row_kind': '+I', 'id': 1, 'payload': b'selected'}]


def test_pk_row_kind_limit_is_global_across_snapshot_and_delta_splits(tmp_path):
    from pypaimon.read.native_plan import native_read, native_split_from_python

    table = _table(tmp_path, partitioned=True)
    payload = _descriptor(tmp_path, 'payload', b'selected')
    missing = _descriptor(tmp_path, 'unselected-delta')
    _write(table, [dict(id=1, tag=1, payload=payload, output=payload, pt='a')])
    splits = table.new_read_builder().new_scan().plan().splits()
    _write(table, [dict(id=2, tag=1, payload=missing, output=missing, pt='b')])
    scan = table.new_stream_read_builder().new_streaming_scan()
    if table.options.native_plan_enabled():
        scan.restore(table.snapshot_manager().get_latest_snapshot().id)
        splits += scan.plan().splits()
    else:
        splits += scan._create_delta_plan(table.snapshot_manager().get_latest_snapshot()).splits()
    assert len(splits) == 2 and not splits[0].is_streaming and splits[1].is_streaming
    # Exercise the Rust quota directly: TableRead's outer truncation must not
    # conceal extra rows or payload reads in the core row-kind path.
    fields = [field for field in table.fields if field.name in ('id', 'payload')]
    rust_splits = [native_split_from_python(split) for split in splits]
    reader = native_read(table, rust_splits, read_type=fields, include_row_kind=True, limit=1)
    try:
        rows = [row for batch in reader for row in batch.to_pylist()]
    finally:
        reader.close()
    assert len(rows) == 1
    assert rows[0]['id'] == 1 and rows[0]['payload'] == b'selected'
