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

"""Row-tracking metadata and filters use Java's data-file versions."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema

pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('id', pa.int32()), ('v', pa.int32()), ('w', pa.int32())])


def _create_table(tmp_path, schema, options):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options=options), False)
    return catalog.get_table('default.t')


def _table(tmp_path, evolution, native_write=True, deletion_vectors=False):
    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': str(evolution).lower(),
               'write.native.enabled': str(native_write).lower(),
               'deletion-vectors.enabled': str(deletion_vectors).lower()}
    table = _create_table(tmp_path, _SCHEMA, options)
    for start in (100, 200):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.table({'id': [start, start + 1], 'v': [10, 20],
                                         'w': [100, 200]}, schema=_SCHEMA))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    return table


def _update(table, field, row_id, value):
    builder = table.new_batch_write_builder()
    messages = builder.new_update().update_by_arrow_with_row_id(pa.table({
        '_ROW_ID': pa.array([row_id], pa.int64()), field: pa.array([value], pa.int32())}))
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()


def _read(table, native, projection, predicate=None, limit=None):
    copy = table.copy({'read.native.enabled': str(native).lower(),
                       'scan.native-plan.enabled': str(native).lower()})
    builder = copy.new_read_builder().with_projection(projection)
    if predicate is not None:
        builder.with_filter(predicate)
    if limit is not None:
        builder.with_limit(limit)
    reader = builder.new_read()
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(reader, '_create_split_read',
                                             side_effect=AssertionError('Python read fallback')))
        return reader.to_arrow(builder.new_scan().plan().splits(), parallelism=1)


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('native_write', [False, True])
@pytest.mark.parametrize('deletion_vectors', [False, True])
def test_partial_column_updates_keep_latest_metadata_provider(tmp_path, native, native_write, deletion_vectors):
    table = _table(tmp_path, True, native_write, deletion_vectors)
    _update(table, 'v', 0, 77)
    _update(table, 'w', 1, 88)
    for projection in (['_SEQUENCE_NUMBER'], ['id', '_SEQUENCE_NUMBER'],
                       ['_SEQUENCE_NUMBER', 'w', '_ROW_ID', 'v', 'id']):
        result = _read(table, native, projection)
        assert result.column_names == projection
        assert not result.schema.field('_SEQUENCE_NUMBER').nullable
        assert result['_SEQUENCE_NUMBER'].null_count == 0
        if 'id' in projection:
            result = result.sort_by('id')
            assert result['id'].to_pylist() == [100, 101, 200, 201]
            assert result['_SEQUENCE_NUMBER'].to_pylist() == [4, 4, 2, 2]
            if 'v' in projection:
                assert result['v'].to_pylist() == [77, 20, 10, 20]
                assert result['w'].to_pylist() == [100, 88, 100, 200]
                assert result['_ROW_ID'].to_pylist() == [0, 1, 2, 3]
        else:
            assert sorted(result['_SEQUENCE_NUMBER'].to_pylist()) == [2, 2, 4, 4]


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('evolution', [False, True])
def test_metadata_predicates_with_unprojected_columns(tmp_path, native, evolution):
    table = _table(tmp_path, evolution)
    if evolution:
        _update(table, 'v', 0, 77)
    latest = 3 if evolution else 1
    pb = table.new_read_builder().with_projection(['id', 'v', '_ROW_ID', '_SEQUENCE_NUMBER']).new_predicate_builder()
    cases = [
        (pb.equal('_SEQUENCE_NUMBER', latest), [100, 101]),
        (pb.is_null('_SEQUENCE_NUMBER'), []),
        (pb.is_not_null('_SEQUENCE_NUMBER'), [100, 101, 200, 201]),
        (pb.and_predicates([pb.equal('_SEQUENCE_NUMBER', latest), pb.equal('id', 101)]), [101]),
        (pb.or_predicates([pb.equal('_SEQUENCE_NUMBER', latest), pb.equal('id', 200)]), [100, 101, 200]),
        (pb.or_predicates([pb.equal('_SEQUENCE_NUMBER', 2), pb.equal('_ROW_ID', 0)]), [100, 200, 201]),
    ]
    for predicate, expected in cases:
        result = _read(table, native, ['id'], predicate).sort_by('id')
        assert result.column_names == ['id']
        assert result['id'].to_pylist() == expected
    result = _read(table, native, ['id'], pb.equal('_SEQUENCE_NUMBER', latest), limit=1)
    assert result.num_rows == 1
    assert result['id'][0].as_py() in [100, 101]


@pytest.mark.parametrize('native', [False, True])
def test_physical_versions_override_manifest_only_when_not_null(tmp_path, native):
    from dataclasses import replace
    from pathlib import Path
    import pyarrow.parquet as pq

    table = _create_table(tmp_path, _SCHEMA, {
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'true'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        data = pa.table({'id': [100, 101], 'v': [10, 20], 'w': [100, 200]}, schema=_SCHEMA)
        writer.write_arrow(data)
        messages = writer.prepare_commit()
        file = messages[0].new_files[0]
        data = data.append_column('_SEQUENCE_NUMBER', pa.array([7, None], pa.int64()))
        pq.write_table(data, file.file_path)
        messages[0].new_files[0] = replace(file, file_size=Path(file.file_path).stat().st_size)
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    result = _read(table, native, ['id', '_SEQUENCE_NUMBER']).sort_by('id')
    assert result['_SEQUENCE_NUMBER'].to_pylist() == [7, 1]
    pb = table.new_read_builder().with_projection(['id', '_SEQUENCE_NUMBER']).new_predicate_builder()
    assert _read(table, native, ['id'], pb.equal('_SEQUENCE_NUMBER', 7))['id'].to_pylist() == [100]
    assert _read(table, native, ['id'], pb.equal('_SEQUENCE_NUMBER', 1))['id'].to_pylist() == [101]


@pytest.mark.parametrize('native', [False, True])
def test_case_sensitive_user_column_does_not_shadow_metadata(tmp_path, native):
    schema = pa.schema([('id', pa.int32()), ('_sequence_number', pa.int32())])
    table = _create_table(tmp_path, schema, {
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
        'write.native.enabled': 'true'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [100, 101], '_sequence_number': [40, 50]}, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    projection = ['id', '_sequence_number', '_SEQUENCE_NUMBER']
    pb = table.new_read_builder().with_projection(projection).new_predicate_builder()
    predicate = pb.and_predicates([pb.equal('_SEQUENCE_NUMBER', 1), pb.equal('_sequence_number', 50)])
    assert _read(table, native, projection, predicate).to_pydict() == {
        'id': [101], '_sequence_number': [50], '_SEQUENCE_NUMBER': [1]}


@pytest.mark.parametrize('user_field,metadata', [
    ('_sequence_number', '_SEQUENCE_NUMBER'), ('_row_id', '_ROW_ID')])
def test_insensitive_metadata_name_collision_is_ambiguous(tmp_path, user_field, metadata):
    from pypaimon.write.native_commit import create_native_write_table

    table = _create_table(tmp_path, pa.schema([(user_field, pa.int32())]), {
        'row-tracking.enabled': 'true', 'write.native.enabled': 'true'})
    reader = create_native_write_table(table).new_read_builder().with_case_sensitive(False)
    for name in (metadata, user_field):
        with pytest.raises(ValueError, match='Ambiguous'):
            reader.with_projection([name]).new_read().read_arrow([])
        with pytest.raises(ValueError, match='Ambiguous'):
            reader.with_filter({'method': 'equal', 'field': name, 'literals': [1]})
