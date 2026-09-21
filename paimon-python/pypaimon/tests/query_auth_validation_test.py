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
#  limitations under the License.
################################################################################

import json
import time
import unittest

from types import SimpleNamespace

import pyarrow as pa

from pypaimon.common.options import CoreOptions, Options
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.read.table_scan import TableScan, validate_auth_rules
from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan
from pypaimon.read.plan import Plan
from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.special_fields import SpecialFields


def _string_field(field_id, name):
    return DataField(field_id, name, AtomicType("STRING"))


def _struct_field(field_id, name, children):
    return DataField(field_id, name, RowType(True, children))


def _null_mask():
    return json.dumps({"name": "NULL"})


def _field_ref_mask(source, index=0):
    return json.dumps({
        "name": "FIELD_REF",
        "fieldRef": {"index": index, "name": source, "type": "STRING"},
    })


def _filter_json(field_name="dept", value="eng"):
    return json.dumps({
        "kind": "LEAF",
        "transform": {
            "name": "FIELD_REF",
            "fieldRef": {"index": 0, "name": field_name, "type": "STRING"},
        },
        "function": "EQUAL",
        "literals": [value],
    })


class _FakeSchema:
    def __init__(self, fields):
        self.fields = fields


class _FakeSchemaManager:
    def __init__(self, fields, error=None):
        self._fields = fields
        self._error = error

    def latest(self):
        if self._error is not None:
            raise self._error
        return _FakeSchema(self._fields)


class _FakeOptions:
    def blob_descriptor_fields(self):
        return set()

    def blob_view_fields(self):
        return set()

    def data_evolution_enabled(self):
        return False

    def row_tracking_enabled(self):
        return False


class _FakeTable:
    def __init__(self, fields, latest_fields=None, latest_error=None):
        self.fields = fields
        self.options = _FakeOptions()
        self.schema_manager = _FakeSchemaManager(
            fields if latest_fields is None else latest_fields, latest_error)


def _make_scan(auth_result, read_type, table):
    scan = TableScan.__new__(TableScan)
    scan._query_auth_fn = lambda select: auth_result
    scan._read_type = read_type
    scan.table = table
    return scan


def _make_read(table, read_type, nested_name_paths=None):
    read = TableRead.__new__(TableRead)
    read.table = table
    read.read_type = read_type
    read.nested_name_paths = nested_name_paths
    return read


class TestValidateAgainstSchema(unittest.TestCase):

    def test_stale_mask_after_rename_is_rejected(self):
        result = TableQueryAuthResult(None, {"email": _null_mask()})
        fields = [_string_field(0, "id"), _string_field(1, "mail")]
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        self.assertIn("email", str(ctx.exception))
        self.assertIn("rename or drop", str(ctx.exception))

    def test_stale_filter_after_rename_is_rejected(self):
        result = TableQueryAuthResult([_filter_json("dept")], None)
        fields = [_string_field(0, "id"), _string_field(1, "team")]
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        self.assertIn("Row filter", str(ctx.exception))
        self.assertIn("dept", str(ctx.exception))

    def test_missing_mask_column_names_the_column(self):
        result = TableQueryAuthResult(None, {"gone": _null_mask()})
        fields = [_string_field(0, "id")]
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        self.assertIn("gone", str(ctx.exception))
        self.assertIn("does not exist in table schema", str(ctx.exception))

    def test_missing_filter_column_names_the_column(self):
        result = TableQueryAuthResult([_filter_json("gone")], None)
        fields = [_string_field(0, "id")]
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        self.assertIn("gone", str(ctx.exception))
        self.assertIn("does not exist in table schema", str(ctx.exception))

    def test_mask_reading_a_masked_column_is_rejected(self):
        result = TableQueryAuthResult(None, {
            "email": _null_mask(),
            "alias": _field_ref_mask("email", 1),
        })
        fields = [
            _string_field(0, "id"),
            _string_field(1, "email"),
            _string_field(2, "alias"),
        ]
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        message = str(ctx.exception)
        self.assertIn("alias", message)
        self.assertIn("email", message)
        self.assertIn("raw value", message)

    def test_mask_reading_itself_is_allowed(self):
        result = TableQueryAuthResult(None, {"email": _field_ref_mask("email", 1)})
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        result.validate_against_schema(fields)

    def test_mask_reading_an_unmasked_column_is_allowed(self):
        result = TableQueryAuthResult(None, {"alias": _field_ref_mask("email", 1)})
        fields = [
            _string_field(0, "id"),
            _string_field(1, "email"),
            _string_field(2, "alias"),
        ]
        result.validate_against_schema(fields)

    def test_a_system_column_is_never_missing_from_the_schema(self):
        fields = [_string_field(0, "id")]
        for rules in (
                (None, {SpecialFields.ROW_ID.name: _null_mask()}),
                (None, {"id": _field_ref_mask("_SEQUENCE_NUMBER", 1)}),
                ([_filter_json("_VALUE_KIND", "x")], None)):
            TableQueryAuthResult(*rules).validate_against_schema(fields)

    def test_valid_rules_pass(self):
        result = TableQueryAuthResult([_filter_json("dept")], {"email": _null_mask()})
        fields = [
            _string_field(0, "id"),
            _string_field(1, "email"),
            _string_field(2, "dept"),
        ]
        result.validate_against_schema(fields)

    def test_no_rules_pass(self):
        result = TableQueryAuthResult(None, None)
        result.validate_against_schema([_string_field(0, "id")])


class TestValidateReadableWithoutRename(unittest.TestCase):

    def test_renamed_column_is_rejected(self):
        result = TableQueryAuthResult(None, {"mail": _null_mask()})
        latest = [_string_field(0, "id"), _string_field(1, "mail")]
        read = [_string_field(0, "id"), _string_field(1, "email")]
        with self.assertRaises(ValueError) as ctx:
            result.validate_readable_without_rename(latest, read)
        message = str(ctx.exception)
        self.assertIn("mail", message)
        self.assertIn("email", message)
        self.assertIn("renamed since", message)

    def test_renamed_filter_operand_is_rejected(self):
        result = TableQueryAuthResult([_filter_json("team")], None)
        latest = [_string_field(0, "id"), _string_field(1, "team")]
        read = [_string_field(0, "id"), _string_field(1, "dept")]
        with self.assertRaises(ValueError) as ctx:
            result.validate_readable_without_rename(latest, read)
        self.assertIn("Row filter", str(ctx.exception))
        self.assertIn("renamed since", str(ctx.exception))

    def test_dropped_and_readded_column_is_rejected(self):
        result = TableQueryAuthResult(None, {"email": _null_mask()})
        latest = [_string_field(0, "id"), _string_field(7, "email")]
        read = [_string_field(0, "id"), _string_field(1, "email")]
        with self.assertRaises(ValueError) as ctx:
            result.validate_readable_without_rename(latest, read)
        self.assertIn("dropped and re-added", str(ctx.exception))

    def test_matching_ids_pass(self):
        result = TableQueryAuthResult(None, {"email": _null_mask()})
        latest = [_string_field(0, "id"), _string_field(1, "email")]
        result.validate_readable_without_rename(latest, list(latest))

    def test_column_absent_from_read_schema_is_inert(self):
        result = TableQueryAuthResult(None, {"email": _null_mask()})
        latest = [_string_field(0, "id"), _string_field(1, "email")]
        read = [_string_field(0, "id")]
        result.validate_readable_without_rename(latest, read)

    def test_mask_input_renamed_is_rejected_when_target_readable(self):
        result = TableQueryAuthResult(None, {"alias": _field_ref_mask("email", 1)})
        latest = [
            _string_field(0, "id"),
            _string_field(1, "email"),
            _string_field(2, "alias"),
        ]
        read = [
            _string_field(0, "id"),
            _string_field(1, "mail"),
            _string_field(2, "alias"),
        ]
        with self.assertRaises(ValueError) as ctx:
            result.validate_readable_without_rename(latest, read)
        self.assertIn("renamed since", str(ctx.exception))

    def test_column_absent_from_latest_schema_is_skipped(self):
        result = TableQueryAuthResult(None, {"gone": _null_mask()})
        latest = [_string_field(0, "id")]
        result.validate_readable_without_rename(latest, list(latest))


class TestValidateReadType(unittest.TestCase):

    def _table_fields(self):
        return [
            _string_field(0, "id"),
            _struct_field(1, "s", [_string_field(2, "a"), _string_field(3, "b")]),
            _string_field(4, "dept"),
        ]

    def test_nested_subfield_projection_of_mask_target_is_rejected(self):
        result = TableQueryAuthResult(None, {"s": _null_mask()})
        read = [_string_field(0, "id"), _string_field(2, "s_a")]
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(
                self._table_fields(), read, [["id"], ["s", "a"]])
        message = str(ctx.exception)
        self.assertIn("'s'", message)
        self.assertIn("s.a", message)
        self.assertIn("partial column", message)

    def test_nested_subfield_projection_of_inactive_mask_input_is_allowed(self):
        result = TableQueryAuthResult(None, {"dept": _field_ref_mask("s", 1)})
        read = [_string_field(0, "id"), _string_field(2, "s_a")]
        result.validate_read_type(self._table_fields(), read, [["id"], ["s", "a"]])

    def test_nested_subfield_projection_of_filter_operand_is_rejected(self):
        result = TableQueryAuthResult([_filter_json("s")], None)
        read = [_string_field(0, "id"), _string_field(2, "s_a")]
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(
                self._table_fields(), read, [["id"], ["s", "a"]])
        self.assertIn("partial column", str(ctx.exception))

    def test_a_column_flattened_onto_a_system_name_is_rejected(self):
        table_fields = [
            _string_field(0, "id"),
            _struct_field(1, "_ROW", [_string_field(2, "ID")]),
        ]
        result = TableQueryAuthResult([_filter_json("_ROW_ID", "999")], None)
        read = [_string_field(0, "id"), _string_field(2, "_ROW_ID")]
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(table_fields, read, [["id"], ["_ROW", "ID"]])
        self.assertIn("field id", str(ctx.exception))

    def test_a_duplicate_projection_of_a_masked_system_column_is_rejected(self):
        row_id = SpecialFields.ROW_ID
        result = TableQueryAuthResult(None, {row_id.name: _null_mask()})
        read = [
            DataField(row_id.id, row_id.name, AtomicType("BIGINT")),
            DataField(row_id.id, row_id.name + "__0", AtomicType("BIGINT")),
        ]
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(self._table_fields(), read)
        self.assertIn("would be raw", str(ctx.exception))

    def test_a_single_projection_of_a_masked_system_column_is_allowed(self):
        row_id = SpecialFields.ROW_ID
        result = TableQueryAuthResult(None, {row_id.name: _null_mask()})
        read = [_string_field(0, "id"),
                DataField(row_id.id, row_id.name, AtomicType("BIGINT"))]
        result.validate_read_type(self._table_fields(), read)

    def test_nested_subfield_beside_the_complete_filter_operand_is_allowed(self):
        result = TableQueryAuthResult([_filter_json("s")], None)
        read = [
            _string_field(0, "id"),
            _struct_field(1, "s", [_string_field(2, "a"), _string_field(3, "b")]),
            _string_field(2, "s_a"),
        ]
        result.validate_read_type(
            self._table_fields(), read, [["id"], ["s"], ["s", "a"]])

    def test_nested_subfield_beside_the_complete_mask_target_is_rejected(self):
        result = TableQueryAuthResult(None, {"s": _null_mask()})
        read = [
            _string_field(0, "id"),
            _struct_field(1, "s", [_string_field(2, "a"), _string_field(3, "b")]),
            _string_field(2, "s_a"),
        ]
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(
                self._table_fields(), read, [["id"], ["s"], ["s", "a"]])
        self.assertIn("would be raw", str(ctx.exception))

    def test_nested_subfield_projection_of_unrelated_column_is_allowed(self):
        result = TableQueryAuthResult([_filter_json("dept")], {"id": _null_mask()})
        read = [
            _string_field(0, "id"),
            _string_field(4, "dept"),
            _string_field(2, "s_a"),
        ]
        result.validate_read_type(
            self._table_fields(), read, [["id"], ["dept"], ["s", "a"]])

    def test_top_level_projection_of_mask_target_is_allowed(self):
        result = TableQueryAuthResult(None, {"s": _null_mask()})
        table_fields = self._table_fields()
        read = [table_fields[0], table_fields[1]]
        result.validate_read_type(table_fields, read, None)

    def test_pruned_type_in_read_schema_is_rejected(self):
        result = TableQueryAuthResult(None, {"s": _null_mask()})
        table_fields = self._table_fields()
        read = [
            _string_field(0, "id"),
            _struct_field(1, "s", [_string_field(2, "a")]),
        ]
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(table_fields, read, None)
        self.assertIn("pruned type", str(ctx.exception))

    def test_no_rules_pass(self):
        result = TableQueryAuthResult(None, None)
        result.validate_read_type(
            self._table_fields(), [_string_field(2, "s_a")], [["s", "a"]])


class TestScanValidationWiring(unittest.TestCase):

    def test_scan_rejects_stale_mask_after_rename(self):
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        fields = [_string_field(0, "id"), _string_field(1, "mail")]
        scan = _make_scan(auth, fields, _FakeTable(fields))
        with self.assertRaises(ValueError) as ctx:
            scan._TableScan__auth_query()
        self.assertIn("email", str(ctx.exception))

    def test_scan_rejects_mask_reading_a_masked_column(self):
        auth = TableQueryAuthResult(None, {
            "email": _null_mask(),
            "alias": _field_ref_mask("email", 1),
        })
        fields = [
            _string_field(0, "id"),
            _string_field(1, "email"),
            _string_field(2, "alias"),
        ]
        scan = _make_scan(auth, fields, _FakeTable(fields))
        with self.assertRaises(ValueError) as ctx:
            scan._TableScan__auth_query()
        self.assertIn("raw value", str(ctx.exception))

    def test_scan_rejects_mask_stale_against_latest_schema_only(self):
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        read_fields = [_string_field(0, "id"), _string_field(1, "email")]
        latest_fields = [_string_field(0, "id"), _string_field(1, "mail")]
        scan = _make_scan(
            auth, read_fields, _FakeTable(read_fields, latest_fields=latest_fields))
        with self.assertRaises(ValueError) as ctx:
            scan._TableScan__auth_query()
        self.assertIn("email", str(ctx.exception))

    def test_scan_rejects_rule_column_renamed_in_read_schema(self):
        auth = TableQueryAuthResult(None, {"mail": _null_mask()})
        read_fields = [_string_field(0, "id"), _string_field(1, "email")]
        latest_fields = [_string_field(0, "id"), _string_field(1, "mail")]
        scan = _make_scan(
            auth, read_fields, _FakeTable(read_fields, latest_fields=latest_fields))
        with self.assertRaises(ValueError) as ctx:
            scan._TableScan__auth_query()
        self.assertIn("renamed since", str(ctx.exception))

    def test_scan_allows_valid_rules(self):
        auth = TableQueryAuthResult([_filter_json("dept")], {"email": _null_mask()})
        fields = [
            _string_field(0, "id"),
            _string_field(1, "email"),
            _string_field(2, "dept"),
        ]
        scan = _make_scan(auth, fields, _FakeTable(fields))
        self.assertIs(scan._TableScan__auth_query(), auth)

    def test_scan_without_rules_skips_validation(self):
        fields = [_string_field(0, "id")]
        scan = _make_scan(TableQueryAuthResult(None, None), fields, _FakeTable(fields))
        self.assertIsNone(scan._TableScan__auth_query())

    def test_scan_propagates_a_latest_schema_read_failure(self):
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        table = _FakeTable(fields, latest_error=RuntimeError("no schema"))
        scan = _make_scan(auth, fields, table)
        with self.assertRaises(RuntimeError):
            scan._TableScan__auth_query()


class TestReadValidationWiring(unittest.TestCase):

    def _table_fields(self):
        return [
            _string_field(0, "id"),
            _struct_field(1, "s", [_string_field(2, "a"), _string_field(3, "b")]),
        ]

    def test_read_rejects_nested_projection_of_mask_target(self):
        auth = TableQueryAuthResult(None, {"s": _null_mask()})
        table_fields = self._table_fields()
        read = _make_read(
            _FakeTable(table_fields),
            [_string_field(0, "id"), _string_field(2, "s_a")],
            [["id"], ["s", "a"]])
        with self.assertRaises(RuntimeError) as ctx:
            read._TableRead__authed_reader(None, auth)
        self.assertIn("partial column", str(ctx.exception))

    def test_read_rejects_nested_projection_of_filter_operand(self):
        auth = TableQueryAuthResult([_filter_json("s")], None)
        table_fields = self._table_fields()
        read = _make_read(
            _FakeTable(table_fields),
            [_string_field(0, "id"), _string_field(2, "s_a")],
            [["id"], ["s", "a"]])
        with self.assertRaises(RuntimeError) as ctx:
            read._TableRead__authed_reader(None, auth)
        self.assertIn("partial column", str(ctx.exception))


if __name__ == '__main__':
    unittest.main()


class TestCodexReviewFindings(unittest.TestCase):

    def test_projection_binding_a_rule_name_to_another_column_is_rejected(self):
        table_fields = [_string_field(0, "id"), _string_field(1, "s_a"),
                        _struct_field(2, "s", [_string_field(3, "a")])]
        result = TableQueryAuthResult(None, {"s_a": _null_mask()})
        read = [_string_field(0, "id"), _string_field(3, "s_a")]
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(table_fields, read, [["id"], ["s", "a"]])
        self.assertIn("field id", str(ctx.exception))

    def test_json_null_mask_is_rejected(self):
        result = TableQueryAuthResult(None, {"email": json.dumps(None)})
        with self.assertRaises(ValueError) as ctx:
            result.parsed_column_masking()
        self.assertIn("JSON null", str(ctx.exception))

    def test_inactive_mask_inputs_are_not_rule_fields(self):
        result = TableQueryAuthResult(None, {"dept": _field_ref_mask("s", 1)})
        self.assertNotIn("s", result.rule_field_names({"id"}))
        self.assertIn("s", result.rule_field_names({"dept"}))

    def test_a_mask_on_a_renamed_physical_system_looking_column_is_rejected(self):
        fields = [_string_field(0, "id"), _string_field(1, "renamed")]
        result = TableQueryAuthResult(None, {"rowkind": _null_mask()})
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        self.assertIn("rowkind", str(ctx.exception))

    def test_a_cross_mask_on_a_physical_system_looking_column_is_rejected(self):
        fields = [_string_field(0, "id"), _string_field(1, "email"),
                  _string_field(2, "_LEVEL")]
        result = TableQueryAuthResult(None, {
            "email": _null_mask(), "_LEVEL": _field_ref_mask("email", 1)})
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        self.assertIn("masked too", str(ctx.exception))

    def test_streaming_scan_validates_like_the_batch_scan(self):
        from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        fields = [_string_field(0, "id"), _string_field(1, "mail")]
        scan = AsyncStreamingTableScan.__new__(AsyncStreamingTableScan)
        scan._query_auth_fn = lambda select: auth
        scan._read_type = fields
        scan.table = _FakeTable(fields)
        with self.assertRaises(ValueError) as ctx:
            scan._AsyncStreamingTableScan__auth_query()
        self.assertIn("does not exist", str(ctx.exception))

    def test_duplicate_projection_alias_of_a_masked_column_is_rejected(self):
        table_fields = [_string_field(0, "id"), _string_field(1, "email")]
        result = TableQueryAuthResult(None, {"email": _null_mask()})
        read = [_string_field(1, "email"), _string_field(1, "email__0")]
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(table_fields, read, None)
        self.assertIn("email__0", str(ctx.exception))

    def test_ordinary_mask_input_must_exist(self):
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        result = TableQueryAuthResult(None, {"email": _field_ref_mask("gone", 2)})
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        self.assertIn("'gone'", str(ctx.exception))

    def test_inputs_of_a_mask_absent_from_the_read_schema_are_inert(self):
        latest = [_string_field(0, "id"), _string_field(1, "email")]
        read = [_string_field(0, "id"), _string_field(7, "email")]
        result = TableQueryAuthResult(None, {"gone": _field_ref_mask("email", 1)})
        result.validate_readable_without_rename(latest, read)

    def test_pruned_type_is_checked_for_filter_operands_too(self):
        table_fields = [_string_field(0, "id"),
                        _struct_field(1, "s", [_string_field(2, "a"), _string_field(3, "b")])]
        result = TableQueryAuthResult([_filter_json("s", "x")], None)
        read = [_string_field(0, "id"), _struct_field(1, "s", [_string_field(2, "a")])]
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(table_fields, read, None)
        self.assertIn("pruned type", str(ctx.exception))

    def test_duplicate_alias_of_an_unmasked_filter_operand_is_allowed(self):
        table_fields = [_string_field(0, "id"), _string_field(1, "email")]
        result = TableQueryAuthResult([_filter_json("email", "x")], None)
        read = [_string_field(1, "email"), _string_field(1, "email__0")]
        result.validate_read_type(table_fields, read, None)

    def test_duplicate_alias_of_an_unmasked_mask_input_is_allowed(self):
        table_fields = [_string_field(0, "id"), _string_field(1, "email"),
                        _string_field(2, "alias")]
        result = TableQueryAuthResult(None, {"alias": _field_ref_mask("email", 1)})
        read = [_string_field(2, "alias"), _string_field(1, "email"),
                _string_field(1, "email__0")]
        result.validate_read_type(table_fields, read, None)


class TestStreamingPlanAuthorization(unittest.TestCase):

    def _scan(self, stale):
        fields = [_string_field(0, "email")]
        latest = [_string_field(0, "mail")] if stale else fields
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        scan = AsyncStreamingTableScan.__new__(AsyncStreamingTableScan)
        scan.table = _FakeTable(fields, latest_fields=latest)
        scan._read_type = fields
        scan._query_auth_fn = lambda select: auth
        raw = Plan([object()], snapshot_id=7)
        scan._AsyncStreamingTableScan__create_initial_plan_raw = lambda *a: raw
        scan._create_delta_plan = lambda *a: raw
        scan._create_changelog_plan = lambda *a: raw
        scan.follow_up_scanner = object()
        return scan, auth

    def _entry(self, scan, path):
        if path == "initial":
            return lambda: scan._create_initial_plan(None)
        if path == "catchup":
            return lambda: scan._create_catch_up_plan(1, None)
        return lambda: scan._create_follow_up_plan(None)

    def test_every_plan_path_wraps_with_the_authorization(self):
        for path in ("initial", "delta", "catchup"):
            scan, auth = self._scan(stale=False)
            plan = self._entry(scan, path)()
            self.assertIsInstance(plan.splits()[0], QueryAuthSplit)
            self.assertIs(plan.splits()[0].auth_result, auth)

    def test_every_plan_path_refuses_a_stale_rule(self):
        for path in ("initial", "delta", "catchup"):
            scan, _ = self._scan(stale=True)
            with self.assertRaises(ValueError):
                self._entry(scan, path)()

    def test_a_system_column_mask_survives_a_rename_of_every_other_column(self):
        result = TableQueryAuthResult(None, {"_VALUE_KIND": _null_mask()})
        fields = [_string_field(0, "id"), _string_field(1, "renamed")]
        result.validate_against_schema(fields)

    def test_duplicate_alias_of_a_column_outside_the_rules_is_allowed(self):
        table_fields = [_string_field(0, "id"), _string_field(1, "email")]
        result = TableQueryAuthResult(None, {"email": _null_mask()})
        read = [_string_field(1, "email"), _string_field(0, "id"),
                _string_field(0, "id__0")]
        result.validate_read_type(table_fields, read, None)

    def test_an_indexless_transform_input_is_discovered(self):
        mask = json.dumps({"name": "CONCAT",
                           "inputs": [{"name": "gone", "type": "STRING"}]})
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        result = TableQueryAuthResult(None, {"email": mask})
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        self.assertIn("'gone'", str(ctx.exception))

    def test_an_indexless_input_reading_a_masked_column_is_rejected(self):
        mask = json.dumps({"name": "CONCAT",
                           "inputs": [{"name": "email", "type": "STRING"}]})
        fields = [_string_field(0, "email"), _string_field(1, "alias")]
        result = TableQueryAuthResult(None, {"email": _null_mask(), "alias": mask})
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        self.assertIn("masked too", str(ctx.exception))

    def test_an_indexless_system_input_is_refused_when_unprojected(self):
        for name in ("_ROW_ID", "_SEQUENCE_NUMBER", "_VALUE_KIND"):
            mask = json.dumps({"name": "CONCAT",
                               "inputs": [{"name": name, "type": "STRING"}]})
            fields = [_string_field(0, "id"), _string_field(1, "email")]
            result = TableQueryAuthResult(None, {"email": mask})
            result.validate_against_schema(fields)
            with self.assertRaises(RuntimeError) as ctx:
                result.validate_read_type(fields, fields)
            self.assertIn(name, str(ctx.exception))

    def test_a_structural_input_carrying_both_index_and_inputs_is_discovered(self):
        mask = json.dumps({"name": "CONCAT", "inputs": [
            {"name": "email", "index": 0, "type": "STRING", "inputs": []}]})
        fields = [_string_field(0, "email"), _string_field(1, "alias")]
        result = TableQueryAuthResult(None, {"email": _null_mask(), "alias": mask})
        with self.assertRaises(ValueError) as ctx:
            result.validate_against_schema(fields)
        self.assertIn("masked too", str(ctx.exception))

    def test_a_structural_system_input_is_refused_when_unprojected(self):
        mask = json.dumps({"name": "CONCAT", "inputs": [
            {"name": "_ROW_ID", "index": 0, "type": "STRING", "inputs": []}]})
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        result = TableQueryAuthResult(None, {"email": mask})
        result.validate_against_schema(fields)
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(fields, fields)
        self.assertIn("_ROW_ID", str(ctx.exception))

    def test_a_system_input_is_allowed_once_the_query_projects_it(self):
        mask = _field_ref_mask("_ROW_ID", 1)
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        read = fields + [_string_field(2147483642, "_ROW_ID")]
        result = TableQueryAuthResult(None, {"email": mask})
        result.validate_read_type(fields, read)

    def test_a_mask_on_an_unprojected_system_column_stays_inert(self):
        fields = [_string_field(0, "id")]
        result = TableQueryAuthResult(None, {"_ROW_ID": _null_mask()})
        result.validate_read_type(fields, fields)

    def test_a_rule_naming_a_column_added_after_a_generated_alias_is_rejected(self):
        latest = [_string_field(0, "email"), _string_field(3, "email__0")]
        read = [_string_field(0, "email"), _string_field(0, "email__0")]
        snapshot = [_string_field(0, "email")]
        result = TableQueryAuthResult(None, {"email__0": _null_mask()})
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(latest, read, None, snapshot)
        self.assertIn("field id", str(ctx.exception))

    def test_a_filter_naming_a_column_added_after_a_generated_alias_is_rejected(self):
        latest = [_string_field(0, "email"), _string_field(3, "s_a")]
        read = [_string_field(0, "email"), _string_field(2, "s_a")]
        snapshot = [_string_field(0, "email"), _string_field(2, "s_a")]
        result = TableQueryAuthResult([_filter_json("s_a", "eng")], None)
        with self.assertRaises(RuntimeError):
            result.validate_read_type(latest, read, None, snapshot)

    def test_a_widened_column_type_is_still_readable(self):
        latest = [DataField(0, "n", AtomicType("BIGINT"))]
        snapshot = [DataField(0, "n", AtomicType("INT"))]
        result = TableQueryAuthResult(None, {"n": _null_mask()})
        result.validate_read_type(latest, snapshot, None, snapshot)

    def test_a_pruned_type_is_still_rejected_against_the_snapshot(self):
        latest = [_string_field(0, "id"),
                  _struct_field(1, "s", [_string_field(2, "a"), _string_field(3, "b")])]
        snapshot = latest
        read = [_string_field(0, "id"), _struct_field(1, "s", [_string_field(2, "a")])]
        result = TableQueryAuthResult(None, {"s": _null_mask()})
        with self.assertRaises(RuntimeError) as ctx:
            result.validate_read_type(latest, read, None, snapshot)
        self.assertIn("pruned type", str(ctx.exception))

    def test_an_unrestricted_result_needs_no_latest_schema(self):
        from pypaimon.read.table_scan import validate_auth_rules
        table = _FakeTable([_string_field(0, "id")],
                           latest_error=RuntimeError("must not be read"))
        validate_auth_rules(table, TableQueryAuthResult(None, None))

    def test_a_filter_only_result_is_still_validated(self):
        from pypaimon.read.table_scan import validate_auth_rules
        table = _FakeTable([_string_field(0, "id")])
        auth = TableQueryAuthResult([_filter_json("gone", "x")], None)
        with self.assertRaises(ValueError):
            validate_auth_rules(table, auth)


class _OneBatch(RecordBatchReader):
    def __init__(self, batch):
        self._batch = batch

    def read_arrow_batch(self):
        batch, self._batch = self._batch, None
        return batch

    def close(self):
        pass


class _CountingCatalog:
    def __init__(self, fields):
        self.identifier = "db.t"
        self.catalog_loader = self
        self._fields = fields
        self.get_table_calls = 0

    def load(self):
        return self

    def get_table(self, identifier):
        self.get_table_calls += 1
        time.sleep(0.05)
        return SimpleNamespace(fields=self._fields)


def _exploding_schema_manager():
    def boom():
        raise AssertionError("the schema directory must not be listed")
    return SimpleNamespace(latest=boom)


class TestSchemaRefreshAsksTheCatalog(unittest.TestCase):

    def _table(self, cached, latest):
        return SimpleNamespace(
            fields=cached, options=CoreOptions(Options({})),
            is_primary_key_table=False,
            schema_manager=_exploding_schema_manager(),
            catalog_environment=_CountingCatalog(latest))

    def test_a_catalog_backed_table_never_lists_the_schema_directory(self):
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        table = self._table(fields, fields)
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        self.assertEqual(
            [f.name for f in validate_auth_rules(table, auth)], ["id", "email"])
        self.assertEqual(table.catalog_environment.get_table_calls, 1)

    def test_a_rename_seen_only_by_the_catalog_still_fails_closed(self):
        cached = [_string_field(0, "id"), _string_field(1, "email")]
        renamed = [_string_field(0, "id"), _string_field(1, "mail")]
        table = self._table(cached, renamed)
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        with self.assertRaises(ValueError):
            validate_auth_rules(table, auth)

    def _reader(self, table, fields):
        read = TableRead(table, None, fields)
        batch = pa.RecordBatch.from_pydict(
            {"id": ["1"], "email": ["SECRET"]},
            schema=PyarrowFieldParser.from_paimon_schema(fields))
        read._create_split_read = lambda split, **kw: SimpleNamespace(
            create_reader=lambda: _OneBatch(batch))
        return read

    def test_a_read_asks_the_catalog_once_across_workers_and_rule_objects(self):
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        table = self._table(fields, fields)
        read = self._reader(table, fields)
        splits = [
            QueryAuthSplit(object(), TableQueryAuthResult(None, {"email": _null_mask()}))
            for _ in range(8)]
        self.assertEqual(
            read.to_arrow(splits, parallelism=4).to_pydict(),
            {"id": ["1"] * 8, "email": [None] * 8})
        self.assertEqual(table.catalog_environment.get_table_calls, 1)

    def test_a_reused_reader_refreshes_between_reads(self):
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        table = self._table(fields, fields)
        read = self._reader(table, fields)
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        for _ in range(3):
            read.to_arrow([QueryAuthSplit(object(), auth)], parallelism=1)
        self.assertEqual(table.catalog_environment.get_table_calls, 3)

    def test_a_read_without_rules_never_asks_the_catalog(self):
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        table = self._table(fields, fields)
        read = self._reader(table, fields)
        read.to_arrow([object()], parallelism=1)
        self.assertEqual(table.catalog_environment.get_table_calls, 0)

    def test_the_reader_stays_picklable_after_a_read(self):
        import pickle
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        table = self._table(fields, fields)
        read = self._reader(table, fields)
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        read.to_arrow([QueryAuthSplit(object(), auth)], parallelism=1)
        pickle.dumps(read._auth_schema)


class TestReaderAppliesTheRules(unittest.TestCase):

    def _read(self, fields, data, latest=None, projected=None):
        latest_schema = SimpleNamespace(fields=fields if latest is None else latest)
        table = SimpleNamespace(
            fields=fields, options=CoreOptions(Options({})), is_primary_key_table=False,
            schema_manager=SimpleNamespace(latest=lambda: latest_schema))
        read = TableRead(table, None, fields if projected is None else projected)

        def storage(split, **kwargs):
            requested = kwargs.get("read_type", read.read_type)
            schema = PyarrowFieldParser.from_paimon_schema(requested)
            batch = pa.RecordBatch.from_pydict(
                {f.name: data[f.name] for f in requested}, schema=schema)
            return SimpleNamespace(create_reader=lambda: _OneBatch(batch))

        read._create_split_read = storage
        return read

    def _rows(self, read, auth):
        return read.to_arrow([QueryAuthSplit(object(), auth)], parallelism=1).to_pydict()

    def test_the_reader_actually_masks(self):
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        read = self._read(fields, {"id": ["1"], "email": ["SECRET"]})
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        self.assertEqual(self._rows(read, auth), {"id": ["1"], "email": [None]})

    def test_the_reader_applies_the_row_filter(self):
        fields = [_string_field(0, "id"), _string_field(1, "dept")]
        read = self._read(fields, {"id": ["1", "2"], "dept": ["eng", "ops"]})
        auth = TableQueryAuthResult([_filter_json("dept", "eng")], None)
        self.assertEqual(self._rows(read, auth), {"id": ["1"], "dept": ["eng"]})

    def test_a_mask_on_a_column_added_after_the_snapshot_is_inert(self):
        fields = [_string_field(0, "id")]
        latest = [_string_field(0, "id"), _string_field(1, "new_email")]
        read = self._read(fields, {"id": ["1"]}, latest=latest)
        auth = TableQueryAuthResult(None, {"new_email": _null_mask()})
        self.assertEqual(self._rows(read, auth), {"id": ["1"]})

    def test_a_stale_rule_is_refused_at_the_reader(self):
        fields = [_string_field(0, "id"), _string_field(1, "mail")]
        read = self._read(fields, {"id": ["1"], "mail": ["SECRET"]})
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        with self.assertRaises(ValueError):
            self._rows(read, auth)

    def test_a_cross_mask_is_refused_at_the_reader(self):
        fields = [_string_field(0, "email"), _string_field(1, "alias")]
        read = self._read(fields, {"email": ["SECRET"], "alias": ["x"]})
        auth = TableQueryAuthResult(None, {
            "email": _null_mask(), "alias": _field_ref_mask("email", 0)})
        with self.assertRaises(ValueError):
            self._rows(read, auth)

    def test_an_unprojected_system_filter_operand_is_refused_at_the_reader(self):
        fields = [_string_field(0, "id")]
        read = self._read(fields, {"id": ["1"]})
        auth = TableQueryAuthResult([_filter_json("_ROW_ID")], None)
        with self.assertRaises(RuntimeError):
            self._rows(read, auth)

    def test_a_mask_on_an_unprojected_system_column_is_inert_at_the_reader(self):
        fields = [_string_field(0, "id")]
        read = self._read(fields, {"id": ["1"]})
        auth = TableQueryAuthResult(None, {"_ROW_ID": _null_mask()})
        self.assertEqual(self._rows(read, auth), {"id": ["1"]})

    def test_a_metadata_read_failure_is_propagated_at_the_reader(self):
        fields = [_string_field(0, "id"), _string_field(1, "email")]
        read = self._read(fields, {"id": ["1"], "email": ["SECRET"]})

        def boom():
            raise RuntimeError("no schema")
        read.table.schema_manager.latest = boom
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        with self.assertRaises(RuntimeError):
            self._rows(read, auth)

    def test_a_widened_column_is_read_and_masked(self):
        historical = [DataField(0, "n", AtomicType("INT"))]
        latest = [DataField(0, "n", AtomicType("BIGINT"))]
        read = self._read(historical, {"n": [7]}, latest=latest)
        auth = TableQueryAuthResult(None, {"n": _null_mask()})
        self.assertEqual(self._rows(read, auth), {"n": [None]})

    def test_a_generated_alias_colliding_with_a_new_column_is_refused(self):
        historical = [_string_field(0, "email")]
        latest = [_string_field(0, "email"), _string_field(3, "email__0")]
        projected = [_string_field(0, "email"), _string_field(0, "email__0")]
        read = self._read(historical, {"email": ["SECRET"], "email__0": ["SECRET"]},
                          latest=latest, projected=projected)
        auth = TableQueryAuthResult(None, {"email__0": _null_mask()})
        with self.assertRaises((ValueError, RuntimeError)):
            self._rows(read, auth)

    def test_a_swapped_identity_is_refused_at_the_reader(self):
        historical = [_string_field(1, "email"), _string_field(2, "alias")]
        latest = [_string_field(2, "email"), _string_field(1, "alias")]
        read = self._read(historical, {"email": ["SECRET"], "alias": ["x"]},
                          latest=latest)
        auth = TableQueryAuthResult(None, {"email": _null_mask()})
        with self.assertRaises(ValueError):
            self._rows(read, auth)
