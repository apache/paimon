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

"""The common Rust index builder prepares full-text and vector indexes over REST."""

import struct
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.read.native_plan import _native_table, _predicate_to_native, native_method_available
from pypaimon.tests import native_sorted_index_build_test as index_tests
from pypaimon.write.native_commit import from_native_commit_messages

rest_catalog = index_tests.rest_catalog
pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_method_available('Table', 'new_global_index_build_builder'),
    reason='Common Rust global index builder required')]

KINDS = ['full-text', 'ivf-flat', 'ivf-pq', 'ivf-sq', 'ivf-rq', 'diskann']
SCHEMA = pa.schema([('id', pa.int32()), ('name', pa.string()),
                    ('embedding', pa.list_(pa.float32(), 2)), ('pt', pa.int32())])
ROWS = [{'id': i, 'name': 'paimon lake' if i % 2 == 0 else None,
         'embedding': [float(i % 4), float(i // 4)], 'pt': i // 16} for i in range(32)]


def _options(kind, rows_per_shard=100):
    options = {'global-index.row-count-per-shard': str(rows_per_shard)}
    if kind != 'full-text':
        options[kind + '.dimension'] = '2'
        if kind != 'diskann':
            options[kind + '.nlist'] = '1'
        if kind == 'ivf-pq':
            options['ivf-pq.pq.m'] = '1'
        if kind == 'diskann':
            options['diskann.pq.bits'] = '4'
    return options


def _table(catalog, options=None):
    settings = {'vector-index.search-mode': 'fast', 'full-text-index.search-mode': 'fast'}
    settings.update(options or {})
    table = index_tests._create(catalog, schema=SCHEMA, partitioned=True, options=settings)
    index_tests._append(table, ROWS, schema=SCHEMA)
    return table


def _builder(table, kind, rows_per_shard=100):
    return (_native_table(table).new_global_index_build_builder()
            .with_index_column('name' if kind == 'full-text' else 'embedding')
            .with_index_type(' ' + kind.upper() + ' ')
            .with_options(_options(kind, rows_per_shard)))


def _build(table, builder):
    return from_native_commit_messages(table, builder.build())


@pytest.mark.parametrize('kind', KINDS)
@pytest.mark.parametrize('native_commit', [False, True])
def test_prepare_partition_then_commit_and_query(rest_catalog, kind, native_commit):
    catalog, _ = rest_catalog
    table = _table(catalog, {'commit.native.enabled': str(native_commit).lower()})
    predicate = table.new_read_builder().new_predicate_builder()
    builder = _builder(table, kind, rows_per_shard=4)
    builder.with_partition_filter(_predicate_to_native(predicate.greater_or_equal('pt', 0)))
    builder.with_partition_filter(_predicate_to_native(predicate.equal('pt', 1)))
    before = table.snapshot_manager().get_latest_snapshot().id
    messages = _build(table, builder)
    files = index_tests._message_files(messages)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert not index_tests._files(table)
    assert len(files) == 4
    assert {message.partition for message in messages} == {(1,)}
    assert {file.index_type for file in files} == {kind}
    assert sum(file.row_count for file in files) == 16
    assert all(struct.unpack('>iiq', file.global_index_meta.source_meta)
               == (0x44454958, 1, before) for file in files)
    index_tests._commit(table, messages)
    assert table.snapshot_manager().get_latest_snapshot().id == before + 1
    assert _build(table, builder) == []
    classic = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    if kind == 'full-text':
        result = (classic.new_full_text_search_builder().with_query('name', '{"match":{"query":"paimon"}}')
                  .with_partition_filter(predicate.equal('pt', 1)).with_limit(64).execute_local())
        assert sorted(result.results()) == list(range(16, 32, 2))
    else:
        result = (classic.new_vector_search_builder().with_vector_column('embedding')
                  .with_query_vector(ROWS[16]['embedding']).with_limit(1)
                  .with_partition_filter(predicate.equal('pt', 1)).execute_local())
        assert len(list(result.results())) == 1
        assert 16 <= list(result.results())[0] < 32


@pytest.mark.parametrize('kind', KINDS)
def test_all_families_use_external_index_placement(rest_catalog, kind, tmp_path):
    catalog, _ = rest_catalog
    external = (tmp_path / 'indexes').as_uri()
    table = _table(catalog, {'global-index.external-path': external})
    messages = _build(table, _builder(table, kind))
    assert messages
    for file in index_tests._message_files(messages):
        assert file.external_path.startswith(external + '/')
        assert table.file_io.exists(file.external_path)
    index_tests._commit(table, messages)
    assert len(index_tests._files(table)) == 2


@pytest.mark.parametrize('kind', ['full-text', 'ivf-flat'])
@pytest.mark.parametrize('native_commit', [False, True])
def test_indexed_column_changes_reject_messages_and_preserve_files(rest_catalog, kind, native_commit):
    catalog, _ = rest_catalog
    table = _table(catalog, {'commit.native.enabled': str(native_commit).lower()})
    messages = _build(table, _builder(table, kind))
    column = 'name' if kind == 'full-text' else 'embedding'
    changed = 'changed' if kind == 'full-text' else [99., 99.]
    data = pa.table({'_ROW_ID': pa.array([0], pa.int64()),
                     column: pa.array([changed], SCHEMA.field(column).type)})
    update = table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(data)
    index_tests._commit(table, update)
    before = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises(Exception, match='Global index source conflict'):
        index_tests._commit(table, messages)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert all(table.file_io.exists(index_tests._path(table, file))
               for file in index_tests._message_files(messages))


def test_python_full_text_build_delegates_to_common_native_entry(rest_catalog):
    catalog, _ = rest_catalog
    table = _table(catalog)
    predicate = table.new_read_builder().new_predicate_builder().equal('pt', 1)
    with patch.object(GlobalIndexBuilder, '_build_generic_index',
                      side_effect=AssertionError('Python index build ran')):
        messages = GlobalIndexBuilder(table, 'name', index_type='full-text',
                                      partition_filter=predicate, options=_options('full-text', 4)).build()
    assert len(index_tests._message_files(messages)) == 4
    assert {message.partition for message in messages} == {(1,)}
    index_tests._commit(table, messages)


@pytest.mark.parametrize('option_source', ['build', 'table'])
@pytest.mark.parametrize('stop_words', [['paimon', 'apache'], 'paimon;apache'])
def test_native_full_text_options_match_python_list_encoding(rest_catalog, option_source, stop_words):
    import json
    from pypaimon.globalindex.full_text.native_full_text_global_index_reader import NativeFullTextIndexOptions

    catalog, _ = rest_catalog
    options = {'full-text.stop-words': stop_words, 'full-text.with-position': True}
    table_options = {'full-text.' + key: value for key, value in
                     NativeFullTextIndexOptions.from_options(options).to_native_options().items()}
    table = _table(catalog, table_options if option_source == 'table' else None)
    messages = GlobalIndexBuilder(table, 'name', index_type='full-text',
                                  options=options if option_source == 'build' else {}).build()
    native_meta = [json.loads(file.global_index_meta.index_meta.decode('utf-8'))
                   for file in index_tests._message_files(messages)]
    assert all(meta['stop-words'] == 'paimon;apache' and meta['with-position'] == 'true'
               for meta in native_meta)
    index_tests._commit(table, messages)
    query = '{"match":{"query":"paimon"}}'
    result = table.new_full_text_search_builder().with_query('name', query).with_limit(64).execute_local()
    assert not list(result.results())
    # Rebuild through Python's original writer and compare serialized options and results.
    classic = table.copy({'write.native.enabled': 'false'})
    classic.drop_global_index('name', index_type='full-text')
    messages = GlobalIndexBuilder(classic, 'name', index_type='full-text',
                                  options=options if option_source == 'build' else {}).build()
    classic_meta = [NativeFullTextIndexOptions.deserialize(file.global_index_meta.index_meta).to_native_options()
                    for file in index_tests._message_files(messages)]
    assert classic_meta == native_meta
    index_tests._commit(classic, messages)
    result = classic.new_full_text_search_builder().with_query('name', query).with_limit(64).execute_local()
    assert not list(result.results())
