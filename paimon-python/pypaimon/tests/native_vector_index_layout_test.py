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

"""Exercise the public PyPaimon APIs with the Rust centroid and MultiVector implementations."""

import pickle
import sys
from collections import Counter
from dataclasses import replace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory
from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.snapshot.snapshot_manager import SnapshotManager
from pypaimon.table.source.search_types import VectorSearchScanPlan
from pypaimon.table.source.vector_search_split import RawVectorSearchSplit
from pypaimon.tests import native_sorted_index_build_test as indexes
from pypaimon.tests.native_ray_index_build_test import _Ray
from pypaimon.tests.native_vector_search_test import _rows, _scores, _single
from pypaimon.write.commit_message import CommitMessage

rest_catalog = indexes.rest_catalog
pytestmark = pytest.mark.native_plan

SCHEMA = pa.schema([('id', pa.int32()), ('embedding', pa.list_(pa.float32())),
                    ('status', pa.string()), ('tenant', pa.int32()), ('pt', pa.int32())])
ROWS = [dict(id=i, embedding=vector, status=status, tenant=tenant, pt=0)
        for i, (vector, status, tenant) in enumerate([
            ([0, 0], 'drop', 1), ([1, 0], None, 2), ([20, 0], 'keep', 1),
            ([21, 0], 'drop', 2), (None, 'keep', 1)])]
RANGE = {'ivf-flat.dimension': 2, 'ivf-flat.nlist': 1, 'ivf-flat.metric': 'l2'}
CENTROID = {**RANGE, 'ivf-flat.sharding': 'centroid', 'ivf-flat.centroid.nlist': 2}


@pytest.fixture(autouse=True)
def vector_layout_runtime():
    sdk = pytest.importorskip('pypaimon_rust.datafusion')
    if not (hasattr(sdk.VectorSearchBuilder, 'with_multi_vector')
            and hasattr(sdk.GlobalIndexBuildPlan, 'replacement_messages')):
        pytest.skip('Requires centroid and MultiVector APIs in pypaimon-rust')


@pytest.fixture
def filesystem_catalog(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    return catalog


@pytest.fixture(params=['filesystem', 'rest'])
def catalog(request):
    if request.param == 'rest':
        return request.getfixturevalue('rest_catalog')[0]
    return request.getfixturevalue('filesystem_catalog')


def _table(catalog, schema=SCHEMA, partitioned=False, **options):
    return indexes._create(catalog, schema=schema, partitioned=partitioned, options={
        'read.native.enabled': 'true', 'vector-index.search-mode': 'fast',
        'scalar-index.prefilter.search-mode': 'full', 'global-index.column-update-action': 'THROW_ERROR',
        **options,
    })


def _ids(table, result):
    return sorted(_rows(table, result).column('id').to_pylist())


def test_centroid_shards_and_bitmap_filter(catalog):
    table = _table(catalog)
    indexes._append(table, ROWS, SCHEMA)
    assert table.create_global_index('embedding', 'ivf-flat', options=CENTROID,
                                     extra_fields={'status': 'bitmap', 'tenant': 'bitmap'}) == 5
    files = indexes._files(table)
    roles = Counter(file.global_index_meta.index_meta[:10] for file in files)
    assert roles == {b'PAIMCENT\x01R': 1, b'PAIMCENT\x01S': 2, b'PAIMCENT\x01B': 2}
    fields = {table.field_dict[name].id for name in ('status', 'tenant')}
    assert all(set(file.global_index_meta.extra_field_ids) == fields for file in files)
    assert sum(file.row_count for file in files if file.global_index_meta.index_meta[9:10] == b'S') == 4

    p = table.new_read_builder().new_predicate_builder()
    predicate = p.or_predicates([
        p.and_predicates([p.equal('status', 'keep'), p.equal('tenant', 1)]),
        p.and_predicates([p.is_null('status'), p.equal('tenant', 2)]),
    ])
    options = {'centroid.nprobe': 2, 'global-index.filter.refine-from-data': False}
    single = _single(table, [0, 0], 2).with_filter(predicate).with_options(options)
    assert not hasattr(single.new_vector_search_read(), 'read_batch')
    assert _ids(table, single.execute_local()) == [1, 2]
    batch = (table.new_batch_vector_search_builder().with_vector_column('embedding').with_limit(1)
             .with_query_vectors([[0, 0], [21, 0]]).with_filter(predicate).with_options(options))
    plan = pickle.loads(pickle.dumps(batch.new_vector_search_scan().scan()))
    assert [_ids(table, result) for result in batch.execute_batch_local()] == [[1], [2]]
    reader = batch.new_batch_vector_search_read()
    assert not hasattr(reader, 'read')
    assert [_ids(table, result) for result in reader.read_batch_plan(plan)] == [[1], [2]]
    assert [_ids(table, result) for result in reader.read_batch(iter(plan.splits()))] == [[1], [2]]

    # A predicate with a non-Bitmap branch still uses exact scalar row checks.
    residual = p.or_predicates([p.equal('status', 'keep'), p.less_than('id', 1)])
    assert _ids(table, _single(table, [0, 0], 4).with_filter(residual)
                .with_option('centroid.nprobe', 2).execute_local()) == [0, 2]


def test_bitmap_plan_does_not_raw_score_rows_outside_probed_shards(filesystem_catalog):
    table = _table(filesystem_catalog, partitioned=True)
    indexes._append(table, ROWS + [dict(id=10, embedding=[0, 0], status='other', tenant=1, pt=1)], SCHEMA)
    table.create_global_index('embedding', 'ivf-flat', partitions={'pt': 0},
                              options={**CENTROID, 'extra_fields': '{"status":"bitmap"}'})
    table.create_global_index('embedding', 'ivf-flat', partitions={'pt': 1}, options=RANGE)
    table.create_global_index('status', 'btree', partitions={'pt': 1})

    predicate = table.new_read_builder().new_predicate_builder().equal('status', 'keep')
    builder = (_single(table, [0, 0], 1).with_filter(predicate).with_option('centroid.nprobe', 1))
    assert builder.execute_local().is_empty()
    plan = pickle.loads(pickle.dumps(builder.new_vector_search_scan().scan()))
    reader = pickle.loads(pickle.dumps(builder.new_vector_search_read()))
    assert reader.read_plan(plan).is_empty()
    assert reader.read(plan.splits()).is_empty()
    assert reader.read_plan(VectorSearchScanPlan(plan.splits(), plan.snapshot())).is_empty()
    # A caller-created raw split explicitly requests scoring its ranges.
    splits = [RawVectorSearchSplit(s.row_ranges, s.scalar_index_files, s.index_type)
              if isinstance(s, RawVectorSearchSplit) else s for s in plan.splits()]
    assert _ids(table, reader.read_plan(VectorSearchScanPlan(splits, plan.snapshot()))) == [2]


def test_filesystem_range_search_keeps_scalar_filter_semantics(filesystem_catalog):
    table = _table(filesystem_catalog, partitioned=True)
    rows = [dict(id=i, embedding=[i / 1000, 0], status=None, tenant=1, pt=0) for i in range(50)]
    rows += [dict(id=i, embedding=[i, 0], status=None, tenant=1, pt=0) for i in range(100, 150)]
    indexes._append(table, rows + [dict(ROWS[0], pt=1)], SCHEMA)
    table.create_global_index('embedding', 'ivf-flat', partitions={'pt': 0}, options={**RANGE, 'ivf-flat.nlist': 4})
    p = table.new_read_builder().new_predicate_builder()
    builder = (_single(table, [0, 0], 3).with_filter(p.greater_or_equal('id', 100))
               .with_partition_filter(p.equal('pt', 0)).with_option('ivf.nprobe', 1))
    reader = builder.new_vector_search_read()
    plan = builder.new_vector_search_scan().scan()
    assert _ids(table, builder.execute_local()) == [100, 101, 102]
    assert _ids(table, reader.read(iter(plan.splits()))) == [100, 101, 102]
    assert _ids(table, reader.read_plan(VectorSearchScanPlan(iter(plan.splits()), plan.snapshot()))) == [100, 101, 102]

    # A centroid build in another partition cannot change the selected range's reader.
    table.create_global_index('embedding', 'ivf-flat', partitions={'pt': 1}, options=CENTROID)
    assert _ids(table, reader.read_plan(plan)) == [100, 101, 102]
    assert _ids(table, builder.execute_local()) == [100, 101, 102]

    # A query over both layouts uses Java's candidate post-filter semantics.
    mixed = (_single(table, [0, 0], 3).with_filter(p.greater_or_equal('id', 100))
             .with_option('ivf.nprobe', 1))
    assert mixed.execute_local().is_empty()
    mixed_plan = mixed.new_vector_search_scan().scan()
    assert mixed.new_vector_search_read().read(mixed_plan.splits()).is_empty()


def test_filesystem_hybrid_search_after_centroid_rebuild(filesystem_catalog):
    table = _table(filesystem_catalog)
    indexes._append(table, ROWS, SCHEMA)
    table.create_global_index('embedding', 'ivf-flat', options=RANGE)
    builder = table.new_hybrid_search_builder().with_limit(1).add_vector_route('embedding', [0, 0], 1)
    assert _ids(table, builder.execute_local()) == [0]
    table.create_global_index('embedding', 'ivf-flat', options=CENTROID, extra_fields={'status': 'bitmap'})
    assert _ids(table, builder.execute_local()) == [0]
    predicate = table.new_read_builder().new_predicate_builder().is_null('status')
    builder.with_filter(predicate)
    assert _ids(table, builder.execute_local()) == [1]
    route, = builder.route_builders()
    assert _ids(table, route.execute_local()) == [1]


def test_captured_hybrid_routes_keep_native_range_semantics(filesystem_catalog):
    schema = SCHEMA.append(pa.field('second_embedding', SCHEMA.field('embedding').type))
    table = _table(filesystem_catalog, schema, partitioned=True)
    rows = [dict(id=i, embedding=[i / 1000, 0], status=None, tenant=1, pt=0) for i in range(50)]
    rows += [dict(id=i, embedding=[i, 0], status=None, tenant=1, pt=0) for i in range(100, 150)]
    rows += [dict(ROWS[0], pt=1, second_embedding=[0, 0])]
    indexes._append(table, rows, schema)
    table.create_global_index('embedding', 'ivf-flat', partitions={'pt': 0}, options={**RANGE, 'ivf-flat.nlist': 4})
    table.create_global_index('second_embedding', 'ivf-flat', partitions={'pt': 1}, options=CENTROID)
    predicate = table.new_read_builder().new_predicate_builder().greater_or_equal('id', 100)
    builder = (table.new_hybrid_search_builder().with_limit(3).with_filter(predicate)
               .add_vector_route('embedding', [0, 0], 3, options={'ivf.nprobe': '1'})
               .add_vector_route('second_embedding', [0, 0], 3))
    expected = builder.execute_local()
    assert expected.is_empty()
    routes = pickle.loads(pickle.dumps(builder.route_builders()))
    assert builder.rank([builder.to_route_result(route, route.execute_local()) for route in routes]).is_empty()
    captured = routes[0].search_builder.new_vector_search_scan().scan()
    standalone = (_single(table, [0, 0], 3).with_filter(predicate).with_option('ivf.nprobe', 1))
    assert standalone.new_vector_search_read().read_plan(captured).is_empty()


def test_captured_hybrid_text_route_uses_native_index(filesystem_catalog):
    from pypaimon.execution.native.table import _native_table
    from pypaimon.execution.native.write.native_commit import from_native_commit_messages

    table = _table(filesystem_catalog)
    indexes._append(table, ROWS, SCHEMA)
    table.create_global_index('embedding', 'ivf-flat', options=CENTROID)
    build = _native_table(table).new_global_index_build_builder()
    indexes._commit(table, from_native_commit_messages(
        table, build.with_index_column('status').with_index_type('full-text').build()))
    builder = (table.new_hybrid_search_builder().with_limit(2).add_vector_route('embedding', [0, 0], 1)
               .add_full_text_route('status', '{"match":{"query":"keep"}}', 1))
    with patch.dict(sys.modules, {'paimon_ftindex': None}):
        expected = builder.execute_local()
        routes = pickle.loads(pickle.dumps(builder.route_builders()))
        actual = builder.rank([builder.to_route_result(route, route.execute_local()) for route in routes])
    assert len(expected.results().to_list()) == 2
    assert _scores(actual) == pytest.approx(_scores(expected))


def test_centroid_build_resolves_field_json_and_scopes_extra_fields(catalog):
    table = _table(catalog, **{
        'fields.embedding.global-index.vector.index-options': '{"sharding":"centroid","centroid.nlist":2}',
        'extra_fields': '{"status":"bitmap"}',
    })
    indexes._append(table, ROWS, SCHEMA)
    assert table.create_global_index('embedding', 'ivf-flat', options=RANGE) == 5
    assert table.create_global_index('embedding', 'ivf-flat', options=RANGE, extra_fields={'status': 'bitmap'}) == 5
    assert table.create_global_index('id', 'btree') > 0


def test_explicit_sharding_overrides_legacy_field_defaults(catalog):
    table = _table(catalog, **{'fields.embedding.pk-vector.index.options': '{"sharding":"range"}'})
    indexes._append(table, ROWS, SCHEMA)
    assert table.create_global_index('embedding', 'ivf-flat', options=CENTROID) == 3


def test_only_changed_field_defaults_override_flat_sharding(catalog):
    table = _table(catalog, **{
        'fields.embedding.sharding': 'centroid',
        'fields.embedding.index-options': '{"sharding":"range","fields.embedding.sharding":"centroid"}',
        'extra_fields': '{"status":"bitmap"}',
    })
    indexes._append(table, ROWS, SCHEMA)
    assert table.create_global_index('embedding', 'ivf-flat', options=RANGE) == 1
    assert not indexes._files(table)[0].global_index_meta.is_centroid_index()


def test_echoed_table_options_keep_table_precedence(catalog):
    table = _table(catalog, **{'fields.embedding.sharding': 'centroid', 'extra_fields': '{"status":"bitmap"}'})
    indexes._append(table, ROWS, SCHEMA)
    options = {**table.options.options.to_map(), **RANGE, 'ivf-flat.sharding': 'range'}
    assert table.create_global_index('embedding', 'ivf-flat', options=options) == 1
    assert not indexes._files(table)[0].global_index_meta.is_centroid_index()
    with pytest.raises(ValueError, match='extra_fields requires a centroid'):
        table.create_global_index('embedding', 'ivf-flat', options=options, extra_fields={'status': 'bitmap'})


@pytest.mark.parametrize('execution', ['local', 'ray'])
def test_rebuild_replaces_centroid_generation_and_drop_removes_bitmaps(catalog, monkeypatch, execution):
    table = _table(catalog)
    indexes._append(table, ROWS, SCHEMA)
    assert table.create_global_index('embedding', 'ivf-flat', options=CENTROID) == 3
    assert _ids(table, _single(table, [0, 0], 1).execute_local()) == [0]
    old = {file.file_name for file in indexes._files(table)}
    if execution == 'ray':
        monkeypatch.setitem(sys.modules, 'ray', _Ray())
    assert table.create_global_index('embedding', 'ivf-flat', options=CENTROID,
                                     extra_fields='{"status":"bitmap"}', execution=execution) == 5
    assert old.isdisjoint(file.file_name for file in indexes._files(table))
    assert table.drop_global_index('embedding', 'ivf-flat') == 5
    assert indexes._files(table) == []


def test_drop_matches_centroid_bitmap_fields_with_older_runtime(rest_catalog):
    table = _table(rest_catalog[0])
    indexes._append(table, ROWS, SCHEMA)
    assert table.create_global_index('embedding', 'ivf-flat', options=CENTROID,
                                     extra_fields={'status': 'bitmap'}) == 5
    with patch('pypaimon_rust.datafusion.GlobalIndexBuildPlan', object):
        assert table.drop_global_index('embedding', 'ivf-flat') == 5
    assert indexes._files(table) == []


def test_centroid_bitmap_commit_checks_source_and_retained_index(catalog):
    table = _table(catalog)
    indexes._append(table, ROWS, SCHEMA)
    build = GlobalIndexBuilder(table, 'embedding', 'ivf-flat', options=CENTROID, extra_fields={'status': 'bitmap'})
    staged = build.build()

    def update(target, column, value):
        updater = target.new_batch_write_builder().new_update()
        indexes._commit(target, updater.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], column: value})))

    update(table, 'status', ['keep'])
    with pytest.raises(RuntimeError, match='Global index source conflict'):
        indexes._commit(table, staged)
    assert indexes._files(table) == []
    indexes._commit(table, build.build())

    ignore = table.copy({'global-index.column-update-action': 'IGNORE'})
    update(ignore, 'tenant', [3])
    with pytest.raises(RuntimeError, match='active centroid index with Bitmap fields'):
        update(ignore, 'status', ['drop'])
    predicate = table.new_read_builder().new_predicate_builder().equal('status', 'keep')
    result = _single(table, [0, 0], 1).with_filter(predicate).with_option('centroid.nprobe', 2).execute_local()
    assert _ids(table, result) == [0]


def test_python_commit_rejects_conflicting_and_incomplete_centroid_groups(catalog):
    table = _table(catalog)
    indexes._append(table, ROWS, SCHEMA)
    build = GlobalIndexBuilder(table, 'embedding', 'ivf-flat', options=CENTROID, extra_fields={'status': 'bitmap'})
    first, second = build.build(), build.build()
    with pytest.raises(RuntimeError, match='centroid|Centroid'):
        indexes._commit(table, first + second)
    assert indexes._files(table) == []
    indexes._commit(table, first)

    entry = first[0].index_adds[0]
    with pytest.raises(RuntimeError, match='centroid|Centroid'):
        indexes._commit(table, [CommitMessage((), 0, [], index_deletes=[replace(entry, kind=1)])])
    assert _ids(table, _single(table, [0, 0], 1).execute_local()) == [0]


def test_centroid_generation_requires_a_source_snapshot(filesystem_catalog):
    table = _table(filesystem_catalog)
    indexes._append(table, ROWS, SCHEMA)
    messages = GlobalIndexBuilder(table, 'embedding', 'ivf-flat', options=CENTROID).build()
    for message in messages:
        for entry in message.index_adds:
            entry.index_file.global_index_meta.source_meta = None
    with pytest.raises(RuntimeError, match='centroid generation'):
        indexes._commit(table, messages)
    assert indexes._files(table) == []


def test_bitmap_column_update_drops_only_its_partition_index(catalog):
    table = _table(catalog, partitioned=True, **{'global-index.column-update-action': 'DROP_PARTITION_INDEX'})
    indexes._append(table, ROWS + [dict(row, id=10 + row['id'], pt=1) for row in ROWS], SCHEMA)
    assert table.create_global_index('embedding', 'ivf-flat', options=CENTROID,
                                     extra_fields={'status': 'bitmap'}) == 10
    from pypaimon.index.index_file_handler import IndexFileHandler

    handler = IndexFileHandler(table)
    retained = {entry.index_file.file_name for entry in handler.scan(None) if entry.partition.values == [1]}
    updater = table.new_batch_write_builder().new_update()
    indexes._commit(table, updater.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'status': ['keep']})))
    assert len(retained) == 5
    assert {entry.index_file.file_name for entry in handler.scan(None)} == retained


@pytest.mark.parametrize('extra_fields', [None, {'status': 'bitmap'}], ids=['vector', 'bitmap'])
def test_staged_partition_delete_rechecks_retained_centroid_index(catalog, extra_fields):
    table = _table(catalog, partitioned=True, **{'commit.native.enabled': 'true'})
    indexes._append(table, ROWS + [dict(row, id=10 + row['id'], pt=1) for row in ROWS], SCHEMA)
    partition = table.new_read_builder().new_predicate_builder().equal('pt', 0)
    staged = table.new_batch_write_builder().new_update().delete_by_predicate(partition)
    count = 5 if extra_fields else 3
    assert table.create_global_index('embedding', 'ivf-flat', options=CENTROID,
                                     extra_fields=extra_fields) == 2 * count
    with pytest.raises(RuntimeError, match='Delete affects an active centroid index'):
        indexes._commit(table, staged)

    retried = table.new_batch_write_builder().new_update().delete_by_predicate(partition)
    indexes._commit(table, retried)
    assert len(indexes._files(table)) == count
    assert _ids(table, _single(table, [0, 0], 1).execute_local()) == [10]


def test_python_commit_reads_a_historical_centroid_source_once(filesystem_catalog):
    table = _table(filesystem_catalog)
    indexes._append(table, ROWS, SCHEMA)
    source_id = table.snapshot_manager().get_latest_snapshot().id
    build = GlobalIndexBuilder(table, 'embedding', 'ivf-flat', options=CENTROID, extra_fields={'status': 'bitmap'})
    staged = build.build()
    indexes._append(table, [dict(ROWS[0], id=20)], SCHEMA)

    get_snapshot = SnapshotManager.get_snapshot_by_id
    seen = []

    def track(manager, snapshot_id):
        seen.append(snapshot_id)
        return get_snapshot(manager, snapshot_id)

    with patch.object(SnapshotManager, 'get_snapshot_by_id', track):
        indexes._commit(table, staged)
    assert seen.count(source_id) == 1
    assert len(indexes._files(table)) == 5


@pytest.mark.parametrize('column, value, conflicts', [('status', ['keep'], True), ('tenant', [2], False)])
def test_centroid_build_checks_compacted_source_columns(catalog, column, value, conflicts):
    table = _table(catalog, **{'commit.native.enabled': 'true'})
    indexes._append(table, ROWS, SCHEMA)
    build = GlobalIndexBuilder(table, 'embedding', 'ivf-flat', options=CENTROID, extra_fields={'status': 'bitmap'})
    staged = build.build()
    updater = table.new_batch_write_builder().new_update()
    indexes._commit(table, updater.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], column: value})))

    # Observe a COMPACT snapshot with the real replacement data files and row IDs.
    latest = SnapshotManager.get_latest_snapshot

    def compacted(manager):
        return replace(latest(manager), commit_kind='COMPACT')

    with patch.object(SnapshotManager, 'get_latest_snapshot', compacted):
        if conflicts:
            with pytest.raises(RuntimeError, match='Global index source conflict'):
                indexes._commit(table, staged)
        else:
            indexes._commit(table, staged)
    assert len(indexes._files(table)) == (0 if conflicts else 5)


def test_centroid_build_rejects_deleted_partial_column_source(filesystem_catalog):
    table = _table(filesystem_catalog)
    indexes._append(table, ROWS, SCHEMA)
    updater = table.new_batch_write_builder().new_update()
    updates = updater.update_by_arrow_with_row_id(pa.table({'_ROW_ID': [0], 'status': ['keep']}))
    indexes._commit(table, updates)
    staged = GlobalIndexBuilder(table, 'embedding', 'ivf-flat', options=CENTROID,
                                extra_fields={'status': 'bitmap'}).build()
    indexes._commit(table, [CommitMessage(msg.partition, msg.bucket, [], deleted_files=msg.new_files)
                            for msg in updates])
    with pytest.raises(RuntimeError, match='Global index source conflict'):
        indexes._commit(table, staged)
    assert indexes._files(table) == []


@pytest.mark.parametrize('inner', [pa.list_(pa.float32()), pa.list_(pa.float32(), 2)], ids=['array', 'vector'])
def test_multi_vector_maxsim_and_single_vector_results(catalog, inner):
    schema = pa.schema([('id', pa.int32()), ('embedding', pa.list_(inner))])
    table = _table(catalog, schema)
    rows = [[(1, 0), (1, 0)], [(0, 1)], [(1, 1)], [], None]
    indexes._append(table, [dict(id=i, embedding=value) for i, value in enumerate(rows)], schema)
    assert table.create_global_index('embedding', 'ivf-flat', options={'dimension': 2, 'nlist': 1}) == 1

    vectors = [[1, 0], [0, 1]]
    builder = (table.new_vector_search_builder().with_vector_column('embedding')
               .with_multi_vector(vectors).with_limit(1))
    vectors[0][0] = -1
    result = builder.execute_local()
    assert _ids(table, result) == [2]
    assert list(_scores(result).values()) == pytest.approx([2 ** 0.5])
    plan = builder.new_vector_search_scan().scan()
    reader = pickle.loads(pickle.dumps(builder.new_vector_search_read()))
    assert _scores(reader.read_plan(pickle.loads(pickle.dumps(plan)))) == pytest.approx(_scores(result))
    assert _ids(table, builder.with_query_vector([1, 0]).execute_local()) == [0]


def test_invalid_multi_vector_queries_and_index_layout(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    table = _table(catalog, pa.schema([('embedding', pa.list_(pa.list_(pa.float32(), 2)))]))
    builder = table.new_vector_search_builder().with_vector_column('embedding').with_limit(1)
    for vectors in ([], [[]], [[1, 0], [1]], [[float('nan'), 0]]):
        with pytest.raises(ValueError, match='query matrix'):
            builder.with_multi_vector(vectors)
    with pytest.raises(Exception, match='dimension'):
        builder.with_multi_vector([[1, 0, 0]]).new_vector_search_read()
    with pytest.raises(Exception, match='range sharding'):
        table.create_global_index('embedding', 'ivf-flat', options=CENTROID)
    with pytest.raises(Exception, match='cosine'):
        table.create_global_index('embedding', 'ivf-flat', options=RANGE)


def test_matrix_search_requires_an_embedding_list_column(filesystem_catalog):
    table = _table(filesystem_catalog)
    builder = _single(table).with_multi_vector([[1, 0], [0, 1]])
    for operation in (builder.new_vector_search_scan, builder.new_vector_search_read, builder.execute_local):
        with pytest.raises(ValueError, match='requires a MultiVector column'):
            operation()
