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

"""Unit tests for PaimonCatalog REST catalog path (using mocks, no real server needed)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft.catalog import Identifier, NotFoundError
from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableNotExistException,
)

from pypaimon.daft.daft_catalog import PaimonCatalog

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
