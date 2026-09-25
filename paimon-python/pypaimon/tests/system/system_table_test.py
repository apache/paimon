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

"""Tests for the read-only SystemTable base class."""

import types
import unittest

from pypaimon.common.identifier import Identifier
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.schema.data_types import (
    ArrayType, AtomicType, DataField, RowType)
from pypaimon.table.system.system_table import SystemTable


_DUMMY_ROW_TYPE = RowType(False, [
    DataField(0, "key", AtomicType("STRING", nullable=False)),
    DataField(1, "value", AtomicType("STRING", nullable=True)),
])


class _DummySystemTable(SystemTable):
    """Concrete SystemTable used only by these unit tests.

    Subclasses are normally per-system-table (snapshots, schemas, ...).
    This stub exercises the abstract base contract independently of any
    real metadata source.
    """

    def system_table_name(self) -> str:
        return "dummy"

    def row_type(self) -> RowType:
        return _DUMMY_ROW_TYPE

    def _build_arrow_table(self):  # pragma: no cover - not exercised in this test
        import pyarrow as pa
        return pa.table({"key": [], "value": []})


_DATA_ROW_TYPE = RowType(False, [
    DataField(0, "id", AtomicType("INT", nullable=False)),
    DataField(1, "name", AtomicType("STRING", nullable=True)),
])


class _DataSystemTable(SystemTable):
    """A SystemTable with a few real rows, used to exercise read filtering."""

    def system_table_name(self) -> str:
        return "data"

    def row_type(self) -> RowType:
        return _DATA_ROW_TYPE

    def _build_arrow_table(self):
        import pyarrow as pa
        return pa.table({
            "id": pa.array([1, 2, 3], pa.int32()),
            "name": pa.array(["a", "b", "c"]),
        })


_ARRAY_ROW_TYPE = RowType(False, [
    DataField(0, "id", AtomicType("INT", nullable=False)),
    DataField(1, "tags",
              ArrayType(True, AtomicType("STRING", nullable=True))),
])


class _ArraySystemTable(SystemTable):
    """A SystemTable with a list-typed column (mirrors $files.write_cols)."""

    def system_table_name(self) -> str:
        return "arr"

    def row_type(self) -> RowType:
        return _ARRAY_ROW_TYPE

    def _build_arrow_table(self):
        import pyarrow as pa
        return pa.table({
            "id": pa.array([1, 2], pa.int32()),
            "tags": pa.array([["a"], ["b"]], pa.list_(pa.string())),
        })


def _fake_base(database: str = "db", table: str = "t", branch=None):
    """Construct a minimal stand-in for FileStoreTable.

    SystemTable only touches ``identifier``, ``file_io`` and ``table_path``
    on its base, so a SimpleNamespace covers the surface without dragging
    in catalog/schema bootstrap.
    """
    identifier = Identifier.create(database, table, branch=branch)
    return types.SimpleNamespace(
        identifier=identifier,
        file_io=object(),
        table_path="/tmp/" + database + "/" + table,
    )


class SystemTableTest(unittest.TestCase):

    def test_identifier_encodes_system_table_suffix(self):
        sys_table = _DummySystemTable(_fake_base())
        self.assertTrue(sys_table.identifier.is_system_table())
        self.assertEqual("dummy", sys_table.identifier.get_system_table_name())
        self.assertEqual("t", sys_table.identifier.get_table_name())
        self.assertEqual("db", sys_table.identifier.get_database_name())
        self.assertEqual("t$dummy", sys_table.identifier.get_object_name())

    def test_identifier_preserves_branch_segment(self):
        sys_table = _DummySystemTable(_fake_base(branch="dev"))
        self.assertEqual("dev", sys_table.identifier.get_branch_name())
        self.assertEqual("dummy", sys_table.identifier.get_system_table_name())
        self.assertEqual("t$branch_dev$dummy", sys_table.identifier.get_object_name())

    def test_base_table_handles_are_exposed(self):
        base = _fake_base()
        sys_table = _DummySystemTable(base)
        self.assertIs(base, sys_table.base_table)
        self.assertIs(base.file_io, sys_table.file_io)
        self.assertEqual(base.table_path, sys_table.table_path)

    def test_row_type_and_primary_keys_defaults(self):
        sys_table = _DummySystemTable(_fake_base())
        self.assertIs(_DUMMY_ROW_TYPE, sys_table.row_type())
        self.assertEqual([], sys_table.primary_keys())

    def test_write_and_search_builders_are_read_only(self):
        sys_table = _DummySystemTable(_fake_base())
        for method_name in (
                "new_stream_read_builder",
                "new_batch_write_builder",
                "new_stream_write_builder",
                "new_full_text_search_builder",
                "new_vector_search_builder",
                "new_hybrid_search_builder",
        ):
            with self.assertRaises(NotImplementedError) as ctx:
                getattr(sys_table, method_name)()
            self.assertIn("read-only", str(ctx.exception).lower(),
                          "method {}: {}".format(method_name, ctx.exception))


class SystemTableFilterTest(unittest.TestCase):
    """``with_filter`` on a system table applies the predicate at read time."""

    def _predicate_builder(self) -> PredicateBuilder:
        return PredicateBuilder(_DATA_ROW_TYPE.fields)

    def _read(self, predicate, projection=None):
        sys_table = _DataSystemTable(_fake_base())
        rb = sys_table.new_read_builder()
        if projection is not None:
            rb = rb.with_projection(projection)
        rb = rb.with_filter(predicate)
        splits = rb.new_scan().plan().splits()
        return rb.new_read().to_arrow(splits)

    def test_equal_filter_selects_matching_rows(self):
        table = self._read(self._predicate_builder().equal("id", 2))
        self.assertEqual(table.column("id").to_pylist(), [2])
        self.assertEqual(table.column("name").to_pylist(), ["b"])

    def test_greater_than_filter(self):
        table = self._read(self._predicate_builder().greater_than("id", 1))
        self.assertEqual(sorted(table.column("id").to_pylist()), [2, 3])

    def test_is_in_filter(self):
        table = self._read(self._predicate_builder().is_in("id", [1, 3]))
        self.assertEqual(sorted(table.column("id").to_pylist()), [1, 3])

    def test_filter_column_may_be_absent_from_projection(self):
        # Filter on `id` while projecting only `name`: the predicate is applied
        # before projection, so this must not raise a missing-column error.
        table = self._read(self._predicate_builder().equal("id", 3),
                           projection=["name"])
        self.assertEqual(table.column_names, ["name"])
        self.assertEqual(table.column("name").to_pylist(), ["c"])

    def test_string_match_filter_still_raises(self):
        # starts_with / ends_with / contains / like are not safe as final
        # Arrow row filters, so they still surface a clear NotImplementedError.
        pred = self._predicate_builder().contains("name", "b")
        sys_table = _DataSystemTable(_fake_base())
        rb = sys_table.new_read_builder().with_filter(pred)
        splits = rb.new_scan().plan().splits()
        read = rb.new_read()
        with self.assertRaises(NotImplementedError):
            read.to_arrow(splits)

    def test_comparison_on_array_column_raises(self):
        # PyArrow's ArrowNotImplementedError (a NotImplementedError subclass)
        # for a list column carries only a cryptic "no kernel" message. The
        # read must reject the comparison up front with the documented,
        # column-named message instead.
        pred = PredicateBuilder(_ARRAY_ROW_TYPE.fields).equal("tags", ["a"])
        sys_table = _ArraySystemTable(_fake_base())
        rb = sys_table.new_read_builder().with_filter(pred)
        splits = rb.new_scan().plan().splits()
        read = rb.new_read()
        with self.assertRaises(NotImplementedError) as ctx:
            read.to_arrow(splits)
        message = str(ctx.exception)
        self.assertIn("non-scalar", message)
        self.assertIn("tags", message)

    def test_null_check_on_array_column_is_allowed(self):
        # is_null / is_valid kernels accept nested input, so a null check on a
        # list column is filtered normally rather than rejected.
        pred = PredicateBuilder(_ARRAY_ROW_TYPE.fields).is_not_null("tags")
        sys_table = _ArraySystemTable(_fake_base())
        rb = sys_table.new_read_builder().with_filter(pred)
        splits = rb.new_scan().plan().splits()
        table = rb.new_read().to_arrow(splits)
        self.assertEqual(sorted(table.column("id").to_pylist()), [1, 2])

    def test_non_predicate_filter_raises(self):
        # with_filter(object()) must keep the public NotImplementedError
        # contract instead of leaking an internal AttributeError.
        sys_table = _DataSystemTable(_fake_base())
        rb = sys_table.new_read_builder().with_filter(object())
        splits = rb.new_scan().plan().splits()
        read = rb.new_read()
        with self.assertRaises(NotImplementedError):
            read.to_arrow(splits)


if __name__ == "__main__":
    unittest.main()
