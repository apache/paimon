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

"""Payload-mode exports must not publish unresolved BlobViewStruct bytes."""

import json

import pyarrow as pa
import pytest

from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.table.row.blob import BlobViewStruct
from pypaimon.tests import multimodal_lance_test as lance_tests

conn = lance_tests.conn


def _view_table(conn, resolution, null=False):
    source = lance_tests._table(conn, "source", {"id": pa.int32(), "payload": pa.large_binary()}, [
        {"id": 1, "payload": b"body"},
    ])
    row_id = source.scan().with_row_id().to_list()[0]["_ROW_ID"]
    reference = BlobViewStruct.of(source.raw_table, "payload", row_id).serialize()
    options = {"blob-view-field": "payload"}
    if resolution == "disabled":
        options["blob-view.resolve.enabled"] = "false"
    table = lance_tests._table(conn, "views", {
        "id": pa.int32(), "payload": pa.large_binary(), "out": pa.large_binary(), "label": pa.string(),
    }, [{"id": 1, "payload": None if null else reference, "out": b"other", "label": "private"}], options=options)
    table = table.copy({"blob-as-descriptor": "false"})
    if resolution == "no_loader":
        table.raw_table.catalog_environment.catalog_loader = None
    return table, reference


def _authorize(table, masks):
    auth = TableQueryAuthResult(None, {name: json.dumps(rule) for name, rule in masks.items()})
    table.raw_table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: auth
    return table.copy({"query-auth.enabled": "true"})


@pytest.mark.parametrize("resolution", ["disabled", "no_loader"])
@pytest.mark.parametrize("direct_ref", [False, True])
@pytest.mark.parametrize("mode", ["create", "overwrite"])
def test_export_lance_rejects_unresolved_view_in_payload_auth_read(conn, tmp_path, resolution, direct_ref, mode):
    table, reference = _view_table(conn, resolution)
    masks = {"label": {"name": "CONCAT", "inputs": ["safe"]}}
    column = "payload"
    if direct_ref:
        column = "out"
        masks[column] = {"name": "FIELD_REF", "fieldRef": {"index": 1, "name": "payload", "type": "BLOB"}}
    query = _authorize(table, masks).scan().select([column, "label"])
    assert query.to_list() == [{column: reference, "label": "safe"}]
    path = tmp_path / "unresolved.lance"
    if mode == "overwrite":
        lance_tests.lance.write_dataset(pa.table({"id": [99]}), str(path))

    with pytest.raises((ValueError, OSError), match="(?i)view"):
        query.export_lance(str(path), mode=mode)

    if mode == "overwrite":
        assert lance_tests._rows(path) == [{"id": 99}]
    else:
        with pytest.raises(ValueError):
            lance_tests.lance.dataset(str(path))


@pytest.mark.parametrize("resolution", ["disabled", "no_loader"])
def test_export_lance_rejects_unresolved_view_in_payload_predicate_read(conn, tmp_path, resolution):
    table, reference = _view_table(conn, resolution)
    query = table.scan().where("payload IS NOT NULL").select("payload")
    assert query.to_list() == [{"payload": reference}]

    with pytest.raises((ValueError, OSError), match="(?i)view"):
        query.export_lance(str(tmp_path / "predicate.lance"))


@pytest.mark.parametrize("resolution", ["disabled", "no_loader"])
@pytest.mark.parametrize("mode", ["create", "overwrite"])
def test_export_lance_rejects_cast_mask_of_unresolved_view(conn, tmp_path, resolution, mode):
    table, reference = _view_table(conn, resolution)
    query = _authorize(table, {"out": {
        "name": "CAST", "fieldRef": {"index": 1, "name": "payload", "type": "BYTES"}, "type": "BYTES",
    }}).scan().select("out")
    assert query.to_list() == [{"out": reference}]
    path = tmp_path / "cast_view.lance"
    if mode == "overwrite":
        lance_tests.lance.write_dataset(pa.table({"id": [99]}), str(path))

    with pytest.raises((ValueError, OSError), match="(?i)view"):
        query.export_lance(str(path), mode=mode)

    if mode == "overwrite":
        assert lance_tests._rows(path) == [{"id": 99}]
    else:
        with pytest.raises(ValueError):
            lance_tests.lance.dataset(str(path))


@pytest.mark.parametrize("resolution", ["disabled", "no_loader"])
@pytest.mark.parametrize("null_mask", [False, True])
def test_export_lance_allows_null_unresolved_view_outputs(conn, tmp_path, resolution, null_mask):
    table, _ = _view_table(conn, resolution, null=not null_mask)
    masks = {"label": {"name": "CONCAT", "inputs": ["safe"]}}
    if null_mask:
        masks["payload"] = {"name": "NULL"}
    query = _authorize(table, masks).scan().select("payload")
    assert query.to_list() == [{"payload": None}]
    path = tmp_path / "null_view.lance"

    query.export_lance(str(path))

    assert lance_tests._rows(path) == [{"payload": None}]
