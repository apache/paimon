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

import json
import os
import sys
import tarfile

import pytest

from pypaimon.catalog.catalog_factory import CatalogFactory


GOLDEN_ARCHIVE = os.path.join(
    os.path.dirname(__file__), "resources", "pk_global_index_golden.tar.gz")


@pytest.fixture(scope="module")
def catalog(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("pk_global_index_golden")
    with tarfile.open(GOLDEN_ARCHIVE, "r:gz") as archive:
        if sys.version_info >= (3, 12):
            archive.extractall(warehouse, filter="data")
        else:
            archive.extractall(warehouse)
    return CatalogFactory.create({"warehouse": str(warehouse)})


def _read_search_result(table, result):
    read_builder = table.new_read_builder()
    plan = read_builder.new_scan().with_global_index_result(result).plan()
    return read_builder.new_read().to_arrow(plan.splits())


def _require_native(module):
    try:
        __import__(module)
    except (ImportError, OSError) as exc:
        pytest.skip("%s native dependency is unavailable: %s" % (module, exc))


def test_java_primary_key_scalar_indexes(catalog):
    table = catalog.get_table("default.test_pk_global_index_golden")

    name_read = table.new_read_builder()
    name_read.with_filter(
        name_read.new_predicate_builder().equal("name", "name-4"))
    name_result = name_read.new_read().to_arrow(name_read.new_scan().plan().splits())
    assert name_result.column("id").to_pylist() == [4]

    category_read = table.new_read_builder()
    category_read.with_filter(
        category_read.new_predicate_builder().equal("category", "odd"))
    category_result = category_read.new_read().to_arrow(
        category_read.new_scan().plan().splits())
    assert sorted(category_result.column("id").to_pylist()) == [1, 3, 5]


def test_java_primary_key_vector_index(catalog):
    _require_native("paimon_vindex")
    table = catalog.get_table("default.test_pk_vector_golden")
    result = (table.new_vector_search_builder()
              .with_vector_column("embedding")
              .with_query_vector([1.0, 0.0, 0.0, 0.0])
              .with_limit(2)
              .execute_local())
    rows = _read_search_result(table, result)
    assert sorted(rows.column("id").to_pylist()) == [1, 2]

    predicate = table.new_read_builder().new_predicate_builder().equal("id", 3)
    filtered = (table.new_vector_search_builder()
                .with_vector_column("embedding")
                .with_query_vector([1.0, 0.0, 0.0, 0.0])
                .with_filter(predicate)
                .with_limit(2)
                .execute_local())
    filtered_rows = _read_search_result(table, filtered)
    assert filtered_rows.column("id").to_pylist() == [3]


def test_java_primary_key_full_text_index(catalog):
    _require_native("paimon_ftindex")
    table = catalog.get_table("default.test_pk_full_text_golden")
    query = json.dumps({"match": {"query": "Paimon"}}, separators=(",", ":"))
    result = (table.new_full_text_search_builder()
              .with_query("content", query)
              .with_limit(10)
              .execute_local())
    rows = _read_search_result(table, result)
    assert sorted(rows.column("id").to_pylist()) == [1, 3]


def test_java_primary_key_vector_refinement_matches_scalar(catalog):
    from unittest import mock
    from pypaimon.table.source import vector_search_read as scoring

    _require_native("paimon_vindex")
    table = catalog.get_table("default.test_pk_vector_golden")

    def search():
        return (table.new_vector_search_builder()
                .with_vector_column("embedding")
                .with_query_vector([1.0, 0.0, 0.0, 0.0])
                .with_option("ivf.refine_factor", "2")
                .with_limit(2).execute_local())

    with mock.patch.object(scoring, "_compute_scores", lambda *args: None):
        expected = search()
    with mock.patch.object(scoring, "_compute_scores", wraps=scoring._compute_scores) as fast:
        actual = search()
    assert actual.positions == expected.positions
    assert actual.positions
    assert fast.call_count > 0


def test_java_primary_key_raw_vectors_match_scalar(catalog):
    from dataclasses import replace
    from unittest import mock
    from pypaimon.table.source import vector_search_read as scoring
    from pypaimon.table.source.primary_key_vector_scan import PrimaryKeyVectorScanPlan

    table = catalog.get_table("default.test_pk_vector_golden")
    builder = (table.new_vector_search_builder()
               .with_vector_column("embedding")
               .with_query_vector([1.0, 0.0, 0.0, 0.0]).with_limit(2))
    plan = builder.new_vector_search_scan().scan()
    raw_plan = PrimaryKeyVectorScanPlan(plan.snapshot_id, [
        replace(split, payloads=(), uncovered_data_files=tuple(
            file.file_name for file in split.data_split.files)) for split in plan.splits()])
    reader = builder.new_vector_search_read()
    with mock.patch.object(scoring, "_compute_scores", lambda *args: None):
        expected = list(reader._raw_candidates(raw_plan))
    with mock.patch.object(scoring, "_compute_scores", wraps=scoring._compute_scores) as fast:
        actual = list(reader._raw_candidates(raw_plan))
    assert actual == expected
    assert actual
    assert fast.call_count > 0
