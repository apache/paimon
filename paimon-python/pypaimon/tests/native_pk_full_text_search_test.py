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

"""Java primary-key full-text files searched and read through Rust core."""

import shutil
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest

from pypaimon import Schema
from pypaimon.api.api_response import ErrorResponse
from pypaimon.read.native_plan import _native_table, _predicate_to_native
from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
from pypaimon.table.source.primary_key_scored_result import PrimaryKeyScoredResult
from pypaimon.tests import native_vector_search_test as vectors
from pypaimon.tests.vector_search_filter_test import match_query

rest_catalog = vectors.rest_catalog
golden_catalog = vectors.golden_catalog
pytestmark = pytest.mark.native_plan


@pytest.fixture
def source(rest_catalog, golden_catalog):
    catalog, server = rest_catalog
    golden = golden_catalog.get_table('default.test_pk_full_text_golden')
    schema = golden.table_schema
    catalog.create_table('default.pk_text', Schema(
        schema.fields, schema.partition_keys, schema.primary_keys, schema.options), False)
    destination = Path(server.data_path) / server.warehouse / 'default' / 'pk_text'
    for directory in ('bucket-0', 'index', 'manifest', 'snapshot'):
        shutil.copytree(str(Path(golden.table_path) / directory), str(destination / directory))
    return catalog.get_table('default.pk_text'), server


def _builder(table, text='Paimon', limit=10):
    return table.new_full_text_search_builder().with_query('content', match_query(text)).with_limit(limit)


def _native(table, text='Paimon', limit=10):
    return (_native_table(table).new_full_text_search_builder()
            .with_query('content', match_query(text)).with_limit(limit))


def _compare(table, text='Paimon', limit=10):
    classic = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    native = table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    expected = _builder(classic, text, limit).execute_local()
    with patch.object(FullTextSearchBuilderImpl, 'new_full_text_scan',
                      side_effect=AssertionError('Python full-text search ran')):
        actual = _builder(native, text, limit).execute_local()
    assert isinstance(actual, PrimaryKeyScoredResult)
    assert actual.snapshot_id == expected.snapshot_id
    assert [(p.partition_bytes, p.bucket, p.data_file_name, p.row_position) for p in actual.positions] == [
        (p.partition_bytes, p.bucket, p.data_file_name, p.row_position) for p in expected.positions]
    assert [p.score for p in actual.positions] == pytest.approx([p.score for p in expected.positions], abs=1e-6)
    assert len(actual.splits) == len(expected.splits)
    for actual_split, expected_split in zip(actual.splits, expected.splits):
        assert len(actual_split.files) == 1
        assert actual_split.row_ranges() == expected_split.row_ranges()
        assert actual_split.scores() == pytest.approx(expected_split.scores(), abs=1e-6)
    rows = vectors._rows(native, actual)
    assert rows.to_pylist() == vectors._rows(classic, expected).to_pylist()
    return rows


@pytest.mark.parametrize('snapshot', [None, 1, 5, 8])
@pytest.mark.parametrize('text', ['Paimon', 'Flink', 'no match'])
@pytest.mark.parametrize('limit', [1, 10])
def test_java_pk_archives_preserve_positions_scores_and_snapshot(source, snapshot, text, limit):
    table, _ = source
    if snapshot is not None:
        table = table.copy({'scan.snapshot-id': str(snapshot)})
    rows = _compare(table, text, limit)
    if snapshot is None and text == 'Paimon' and limit == 10:
        assert sorted(rows.column('id').to_pylist()) == [1, 3]


@pytest.mark.parametrize('projection', [['id'], ['content', 'id'], []])
def test_core_result_keeps_scored_selections_and_materializes_only_the_projection(source, projection):
    table, _ = source
    result = _native(table).execute_local()
    assert result.snapshot_id() == table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises(Exception, match='physical file positions'):
        result.row_ids()
    positions = result.positions()
    assert positions and len(positions) == len(result)
    reader = result.new_read_builder().with_projection(projection)
    batches = list(reader.read())
    assert sum(batch.num_rows for batch in batches) == len(result)
    assert all(batch.schema.names == projection + ['__paimon_search_score'] for batch in batches)
    scores = [score for batch in batches for score in batch.column('__paimon_search_score').to_pylist()]
    assert scores == pytest.approx([p[4] for p in positions], abs=1e-6)
    # The original result can be read again without scanning or searching again.
    assert sum(batch.num_rows for batch in reader.read()) == len(result)


def test_captured_plan_does_not_resolve_latest_again_and_accepts_another_query(source):
    table, server = source
    builder = _native(table)
    plan = builder.new_full_text_scan().scan()
    snapshot_id = plan.snapshot_id()
    expected = _native(table, 'Flink', 1).execute_local().positions()
    reader = _native(table, 'Flink', 1).new_full_text_read()
    unavailable = server._mock_response(ErrorResponse('TABLE', 'pk_text', 'snapshot unavailable', 503), 503)
    with patch.object(server, '_table_snapshot_handle', return_value=unavailable):
        result = reader.read_plan(plan)
        assert result.snapshot_id() == snapshot_id
        assert result.positions() == expected
        assert sum(batch.num_rows for batch in result.new_read_builder().with_projection(['id']).read()) == len(result)


def test_search_returns_metadata_without_opening_user_parquet_files(source):
    table, server = source
    expected = _native(table).execute_local().positions()
    files = list((Path(server.data_path) / server.warehouse / 'default' / 'pk_text' / 'bucket-0').glob('*.parquet'))
    assert expected and files
    with ExitStack() as restore:
        for path in files:
            hidden = path.with_suffix('.held')
            path.rename(hidden)
            restore.callback(hidden.rename, path)
        result = _native(table).execute_local()
        assert result.positions() == expected
        with pytest.raises(Exception, match='NotFound|not found|No such file|does not exist'):
            result.new_read_builder().with_projection(['id']).read()


@pytest.mark.parametrize('case', ['table', 'filter', 'kind', 'column'])
def test_pk_plan_cannot_be_read_under_another_context(source, case):
    table, _ = source
    plan = _native(table).new_full_text_scan().scan()
    if case == 'table':
        catalog = table.catalog_environment.catalog_loader.load()
        schema = table.table_schema
        catalog.create_table('default.other_text', Schema(
            schema.fields, schema.partition_keys, schema.primary_keys, schema.options), False)
        reader = _native(catalog.get_table('default.other_text')).new_full_text_read()
        with pytest.raises(Exception, match='different table, column or filter'):
            reader.read_plan(plan)
    elif case == 'filter':
        reader = _native(table)
        reader.with_filter(_predicate_to_native(table.new_read_builder().new_predicate_builder().equal('id', 1)))
        with pytest.raises(Exception, match='non-partition filters'):
            reader.new_full_text_read()
    elif case == 'kind':
        changed = table.copy({'pk-full-text.index.columns': ''})
        reader = _native(changed).new_full_text_read()
        with pytest.raises(Exception, match='different table, column or filter'):
            reader.read_plan(plan)
    else:
        reader = (_native_table(table).new_full_text_search_builder()
                  .with_query('id', match_query('Paimon')).with_limit(10))
        with pytest.raises(Exception, match='character string'):
            reader.new_full_text_read()


@pytest.mark.parametrize('mode', ['full', 'detail'])
@pytest.mark.parametrize('native', [False, True])
def test_pk_fast_only_contract_is_unchanged(source, mode, native):
    table, _ = source
    selected = table.copy({'read.native.enabled': str(native).lower(), 'full-text-index.search-mode': mode})
    with pytest.raises(NotImplementedError, match='FAST'):
        _builder(selected).execute_local()


def test_native_pk_search_failure_does_not_retry_python(source):
    table, server = source
    table = table.copy({'read.native.enabled': 'true'})
    unavailable = server._mock_response(ErrorResponse('TABLE', 'pk_text', 'snapshot unavailable', 503), 503)
    with patch.object(server, '_table_snapshot_handle', return_value=unavailable), \
            patch.object(FullTextSearchBuilderImpl, 'new_full_text_scan',
                         side_effect=AssertionError('Python search retried')):
        with pytest.raises(Exception, match='snapshot unavailable'):
            _builder(table).execute_local()
