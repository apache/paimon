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

"""Tests for the daft + REST catalog code path."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft.catalog import Identifier, NotFoundError
from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableNotExistException,
)

from pypaimon.daft.daft_catalog import PaimonCatalog
from pypaimon.tests.rest.rest_base_test import RESTBaseTest

# ---------------------------------------------------------------------------
# Helpers: build a mock inner catalog that mimics RESTCatalog's interface
# ---------------------------------------------------------------------------


def _make_rest_inner(
    databases: list[str] | None = None,
    tables_by_db: dict[str, list[str]] | None = None,
):
    """Return a mock that quacks like a RESTCatalog."""
    inner = MagicMock(spec=pypaimon.catalog.catalog.Catalog)

    inner.list_databases = MagicMock(return_value=databases or [])
    inner.list_tables = MagicMock(side_effect=lambda db: (tables_by_db or {}).get(db, []))
    inner.drop_database = MagicMock()
    inner.drop_table = MagicMock()

    inner.get_database = MagicMock()
    inner.create_database = MagicMock()
    inner.get_table = MagicMock()
    inner.create_table = MagicMock()

    return inner


# ---------------------------------------------------------------------------
# _list_namespaces — REST path
# ---------------------------------------------------------------------------


def test_rest_list_namespaces_delegates_to_list_databases():
    inner = _make_rest_inner(databases=["db_a", "db_b", "db_c"])
    cat = PaimonCatalog(inner)

    result = cat.list_namespaces()

    inner.list_databases.assert_called_once()
    assert Identifier("db_a") in result
    assert Identifier("db_b") in result
    assert Identifier("db_c") in result


def test_rest_list_namespaces_with_pattern():
    inner = _make_rest_inner(databases=["prod_orders", "prod_users", "staging_data"])
    cat = PaimonCatalog(inner)

    result = cat.list_namespaces(pattern="prod")

    assert all(str(r).startswith("prod") for r in result)
    assert len(result) == 2


def test_rest_list_namespaces_empty():
    inner = _make_rest_inner(databases=[])
    cat = PaimonCatalog(inner)
    assert cat.list_namespaces() == []


# ---------------------------------------------------------------------------
# _list_tables — REST path
# ---------------------------------------------------------------------------


def test_rest_list_tables_delegates_to_list_databases_and_list_tables():
    inner = _make_rest_inner(
        databases=["db_a", "db_b"],
        tables_by_db={"db_a": ["orders", "users"], "db_b": ["events"]},
    )
    cat = PaimonCatalog(inner)

    result = cat.list_tables()

    assert Identifier("db_a", "orders") in result
    assert Identifier("db_a", "users") in result
    assert Identifier("db_b", "events") in result
    assert len(result) == 3


def test_rest_list_tables_calls_list_tables_per_database():
    inner = _make_rest_inner(
        databases=["db_a", "db_b"],
        tables_by_db={"db_a": ["t1"], "db_b": ["t2"]},
    )
    cat = PaimonCatalog(inner)
    cat.list_tables()

    assert inner.list_tables.call_count == 2
    inner.list_tables.assert_any_call("db_a")
    inner.list_tables.assert_any_call("db_b")


def test_rest_list_tables_with_pattern():
    inner = _make_rest_inner(
        databases=["db_a"],
        tables_by_db={"db_a": ["orders", "order_items", "users"]},
    )
    cat = PaimonCatalog(inner)

    result = cat.list_tables(pattern="db_a.order")

    assert Identifier("db_a", "orders") in result
    assert Identifier("db_a", "order_items") in result
    assert Identifier("db_a", "users") not in result


def test_rest_list_tables_empty_database():
    inner = _make_rest_inner(
        databases=["empty_db"],
        tables_by_db={"empty_db": []},
    )
    cat = PaimonCatalog(inner)
    assert cat.list_tables() == []


# ---------------------------------------------------------------------------
# _drop_namespace — REST path
# ---------------------------------------------------------------------------


def test_rest_drop_namespace_delegates_to_drop_database():
    inner = _make_rest_inner(databases=["my_db"])
    cat = PaimonCatalog(inner)

    cat.drop_namespace("my_db")

    inner.drop_database.assert_called_once_with("my_db", ignore_if_not_exists=False)


def test_rest_drop_namespace_not_found_raises_notfounderror():
    inner = _make_rest_inner()
    inner.drop_database.side_effect = DatabaseNotExistException("my_db")
    cat = PaimonCatalog(inner)

    with pytest.raises(NotFoundError):
        cat.drop_namespace("my_db")


# ---------------------------------------------------------------------------
# _drop_table — REST path
# ---------------------------------------------------------------------------


def test_rest_drop_table_delegates_to_drop_table():
    inner = _make_rest_inner()
    cat = PaimonCatalog(inner)

    cat.drop_table("my_db.my_table")

    inner.drop_table.assert_called_once_with("my_db.my_table", ignore_if_not_exists=False)


def test_rest_drop_table_not_found_raises_notfounderror():
    inner = _make_rest_inner()
    fake_ident = MagicMock()
    fake_ident.get_full_name.return_value = "my_db.my_table"
    inner.drop_table.side_effect = TableNotExistException(fake_ident)
    cat = PaimonCatalog(inner)

    with pytest.raises(NotFoundError):
        cat.drop_table("my_db.my_table")


# ---------------------------------------------------------------------------
# _has_namespace — strips catalog prefix from multi-part identifiers
# ---------------------------------------------------------------------------


def test_has_namespace_single_part():
    inner = _make_rest_inner()
    inner.get_database.return_value = MagicMock()
    cat = PaimonCatalog(inner)

    assert cat.has_namespace("my_db") is True
    inner.get_database.assert_called_once_with("my_db")


def test_has_namespace_not_found_returns_false():
    inner = _make_rest_inner()
    inner.get_database.side_effect = DatabaseNotExistException("nope")
    cat = PaimonCatalog(inner)

    assert cat.has_namespace("nope") is False


# ---------------------------------------------------------------------------
# _create_namespace — delegates properly
# ---------------------------------------------------------------------------


def test_create_namespace_single_part():
    inner = _make_rest_inner()
    cat = PaimonCatalog(inner)

    cat.create_namespace("new_db")

    inner.create_database.assert_called_once_with("new_db", ignore_if_exists=False)


class DaftRestReadTest(RESTBaseTest):

    def test_read_table_forwards_full_catalog_options_to_datasource(self):
        from pypaimon.daft.daft_datasource import PaimonDataSource
        from pypaimon.daft.daft_paimon import _read_table

        captured = {}
        original_init = PaimonDataSource.__init__

        def spy_init(_self, table, storage_config, catalog_options):
            captured["catalog_options"] = dict(catalog_options)
            return original_init(
                _self, table,
                storage_config=storage_config,
                catalog_options=catalog_options,
            )

        with patch.object(PaimonDataSource, "__init__", spy_init):
            _read_table(self.table, catalog_options=self.options)

        received = captured["catalog_options"]
        self.assertEqual(received.get("metastore"), "rest", received)
        self.assertIn("uri", received, received)
        self.assertIn("token", received, received)

    def test_read_table_enriches_io_config_with_rest_token(self):
        from pypaimon.daft import daft_io_config
        from pypaimon.daft.daft_paimon import _read_table

        token_payload = {
            "fs.oss.accessKeyId": "ak-from-dlf",
            "fs.oss.accessKeySecret": "sk-from-dlf",
            "fs.oss.securityToken": "sts-from-dlf",
        }
        fake_token = MagicMock()
        fake_token.token = token_payload
        fake_file_io = MagicMock()
        fake_file_io.token = fake_token

        captured = {}
        original_builder = daft_io_config._convert_paimon_catalog_options_to_io_config

        def spy_builder(opts):
            captured["opts"] = dict(opts)
            return original_builder(opts)

        oss_options = {**self.options, "warehouse": "morax_test"}
        oss_table_path = "oss://my-bucket/db.db/tbl-abc"

        with patch.object(self.table, "file_io", fake_file_io), \
             patch.object(self.table, "table_path", oss_table_path), \
             patch.object(daft_io_config, "_convert_paimon_catalog_options_to_io_config", spy_builder):
            _read_table(self.table, catalog_options=oss_options)

        for k, v in token_payload.items():
            self.assertEqual(captured["opts"].get(k), v, captured["opts"])
        self.assertEqual(captured["opts"].get("warehouse"), "oss://my-bucket", captured["opts"])
        fake_file_io.try_to_refresh_token.assert_called()

    def test_enrich_is_noop_when_not_rest_metastore(self):
        from pypaimon.daft.daft_paimon import _enrich_options_with_rest_token
        opts = {"warehouse": "/tmp/x", "metastore": "filesystem"}
        self.assertIs(_enrich_options_with_rest_token(opts, self.table), opts)

    def test_enrich_is_noop_when_file_io_has_no_refresh(self):
        from pypaimon.daft.daft_paimon import _enrich_options_with_rest_token
        with patch.object(self.table, "file_io", MagicMock(spec=[])):
            self.assertIs(_enrich_options_with_rest_token(self.options, self.table), self.options)

    def test_enrich_is_noop_when_token_is_none(self):
        from pypaimon.daft.daft_paimon import _enrich_options_with_rest_token
        fake_file_io = MagicMock()
        fake_file_io.token = None
        with patch.object(self.table, "file_io", fake_file_io):
            self.assertIs(_enrich_options_with_rest_token(self.options, self.table), self.options)
