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

"""Lance exports evaluate payload-mode BLOB predicates on payload bytes."""

import json

import pyarrow as pa
import pytest

from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.tests import multimodal_lance_test as lance_tests

conn = lance_tests.conn


@pytest.mark.parametrize("as_descriptor", [False, True])
@pytest.mark.parametrize("include_payload", [False, True])
@pytest.mark.parametrize("where, payload_ids, descriptor_ids", [
    ("payload = 'abc'", [1], []),
    ("payload != 'abc'", [2, 4, 5], [1, 2, 4, 5]),
    ("payload IN ('abc', 'def')", [1, 2], []),
    ("payload = ''", [4], []),
])
def test_export_lance_preserves_blob_predicate_mode(
        conn, tmp_path, as_descriptor, include_payload, where, payload_ids, descriptor_ids):
    values = [b"abc", b"def", None, b"", b"abcd"]
    table = lance_tests._table(conn, "payload_filter", {"id": pa.int32(), "payload": pa.large_binary()}, [
        {"id": index + 1, "payload": value} for index, value in enumerate(values)
    ]).copy({"blob-as-descriptor": str(as_descriptor).lower()})
    columns = ["payload", "id"] if include_payload else ["id"]
    query = table.scan().where(where).select(columns)
    expected_ids = descriptor_ids if as_descriptor else payload_ids
    assert sorted(row["id"] for row in query.to_list()) == expected_ids
    expected = [({"payload": values[row_id - 1], "id": row_id} if include_payload else {"id": row_id})
                for row_id in expected_ids]
    path = tmp_path / "payload_filter.lance"

    query.export_lance(str(path))

    assert sorted(lance_tests._rows(path), key=lambda row: row["id"]) == expected


@pytest.mark.parametrize("as_descriptor", [False, True])
def test_export_lance_keeps_blob_filter_mode_under_column_masking(conn, tmp_path, as_descriptor):
    table = lance_tests._table(conn, "masked_filter", {
        "id": pa.int32(), "payload": pa.large_binary(), "label": pa.string(),
    }, [{"id": 1, "payload": b"abc", "label": "private"},
        {"id": 2, "payload": b"def", "label": "private"}])
    auth = TableQueryAuthResult(None, {"label": json.dumps({"name": "CONCAT", "inputs": ["masked"]})})
    table.raw_table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: auth
    table = table.copy({"query-auth.enabled": "true", "blob-as-descriptor": str(as_descriptor).lower()})
    query = table.scan().where("payload = 'abc'").select(["id", "label"])
    expected = [] if as_descriptor else [{"id": 1, "label": "masked"}]
    assert query.to_list() == expected
    path = tmp_path / "masked_filter.lance"

    query.export_lance(str(path))

    assert lance_tests._rows(path) == expected
