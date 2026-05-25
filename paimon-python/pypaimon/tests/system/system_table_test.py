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
from pypaimon.schema.data_types import AtomicType, DataField, RowType
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
        ):
            with self.assertRaises(NotImplementedError) as ctx:
                getattr(sys_table, method_name)()
            self.assertIn("read-only", str(ctx.exception).lower(),
                          "method {}: {}".format(method_name, ctx.exception))


if __name__ == "__main__":
    unittest.main()
