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
import unittest

import pyarrow as pa

from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.catalog.table_query_auth import (
    TableQueryAuthResult,
)
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.read.query_auth_split import QueryAuthSplit


class _FakeField:
    def __init__(self, name):
        self.name = name


class _FakeSplit:
    def __init__(self, name="s1"):
        self.name = name

    @property
    def row_count(self):
        return 100

    @property
    def files(self):
        return []

    @property
    def partition(self):
        return {}

    @property
    def bucket(self):
        return 0

    def merged_row_count(self):
        return 100


class _FakePlan:
    def __init__(self, splits, snapshot_id=None):
        self._splits = splits
        self.snapshot_id = snapshot_id

    def splits(self):
        return self._splits


def _simple_filter_json(field_name="dept", value="eng"):
    return json.dumps({
        "kind": "LEAF",
        "transform": {
            "name": "FIELD_REF",
            "fieldRef": {"index": 0, "name": field_name, "type": "STRING"},
        },
        "function": "EQUAL",
        "literals": [value],
    })


class TestTableNoPermissionException(unittest.TestCase):

    def test_message_format(self):
        identifier = Identifier("db", "table")
        exc = TableNoPermissionException(identifier)
        self.assertIn("db.table", str(exc))
        self.assertIn("No permission", str(exc))
        self.assertEqual(exc.identifier, identifier)


class TestTableQueryAuthResultConvertPlan(unittest.TestCase):

    def test_no_auth_returns_original_plan(self):
        result = TableQueryAuthResult(None, None)
        plan = _FakePlan([_FakeSplit()])
        converted = result.convert_plan(plan)
        self.assertIs(converted, plan)

    def test_empty_filter_and_masking_returns_original(self):
        result = TableQueryAuthResult([], {})
        plan = _FakePlan([_FakeSplit()])
        converted = result.convert_plan(plan)
        self.assertIs(converted, plan)

    def test_blank_filter_entries_are_skipped(self):
        result = TableQueryAuthResult(["", None], None)
        self.assertFalse(result.filter)

    def test_mixed_blank_and_valid_filter_entries(self):
        valid = _simple_filter_json()
        result = TableQueryAuthResult(["", valid], None)
        self.assertEqual(len(result.filter), 1)
        self.assertEqual(result.filter[0], valid)

    def test_blank_filter_no_extra_fields(self):
        result = TableQueryAuthResult([""], None)
        extra = result.get_extra_fields_for_filter(
            [_FakeField("a")], [_FakeField("a"), _FakeField("b")])
        self.assertEqual(extra, [])

    def test_blank_filter_no_row_filter(self):
        result = TableQueryAuthResult(["", None], None)
        self.assertIsNone(result.extract_row_filter())

    def test_blank_column_masking_values_stripped(self):
        result = TableQueryAuthResult(None, {"col": "", "col3": '{"name":"NULL"}'})
        self.assertEqual(list(result.column_masking.keys()), ["col3"])

    def test_blank_column_masking_keys_stripped(self):
        result = TableQueryAuthResult(None, {"": '{"name":"NULL"}'})
        self.assertEqual(result.column_masking, {})

    def test_blank_masking_returns_original_plan(self):
        result = TableQueryAuthResult(None, {"col": ""})
        plan = _FakePlan([_FakeSplit()])
        converted = result.convert_plan(plan)
        self.assertIs(converted, plan)

    def test_wraps_splits_with_filter(self):
        result = TableQueryAuthResult([_simple_filter_json()], None)
        plan = _FakePlan([_FakeSplit("s1"), _FakeSplit("s2")])
        converted = result.convert_plan(plan)
        self.assertEqual(len(converted.splits()), 2)
        for qs in converted.splits():
            self.assertIsInstance(qs, QueryAuthSplit)

    def test_wraps_splits_with_masking(self):
        result = TableQueryAuthResult(None, {"col": '{"name":"NULL"}'})
        plan = _FakePlan([_FakeSplit()])
        converted = result.convert_plan(plan)
        self.assertEqual(len(converted.splits()), 1)
        self.assertIsInstance(converted.splits()[0], QueryAuthSplit)

    def test_inner_split_preserved(self):
        result = TableQueryAuthResult([_simple_filter_json()], None)
        plan = _FakePlan([_FakeSplit("original")])
        converted = result.convert_plan(plan)
        self.assertEqual(converted.splits()[0].split.name, "original")


class TestTableQueryAuthResultExtractRowFilter(unittest.TestCase):

    def test_no_filter_returns_none(self):
        result = TableQueryAuthResult(None, None)
        self.assertIsNone(result.extract_row_filter())

    def test_single_filter(self):
        result = TableQueryAuthResult([_simple_filter_json("dept", "eng")], None)
        fn = result.extract_row_filter()
        self.assertIsNotNone(fn)
        batch = pa.RecordBatch.from_pydict({"dept": ["eng", "sales", "eng"]})
        mask = fn(batch)
        self.assertEqual(mask.to_pylist(), [True, False, True])

    def test_multiple_filters_combined_with_and(self):
        f1 = _simple_filter_json("dept", "eng")
        f2 = json.dumps({
            "kind": "LEAF",
            "transform": {
                "name": "FIELD_REF",
                "fieldRef": {"index": 0, "name": "dept", "type": "STRING"},
            },
            "function": "NOT_EQUAL",
            "literals": ["eng"],
        })
        result = TableQueryAuthResult([f1, f2], None)
        fn = result.extract_row_filter()
        batch = pa.RecordBatch.from_pydict({"dept": ["eng", "sales", "eng"]})
        mask = fn(batch)
        self.assertEqual(mask.to_pylist(), [False, False, False])


class TestTableQueryAuthResultExtraFields(unittest.TestCase):

    def test_detects_unprojected_field(self):
        read_fields = [_FakeField("name"), _FakeField("age")]
        table_fields = [
            _FakeField("name"),
            _FakeField("age"),
            _FakeField("dept"),
        ]
        result = TableQueryAuthResult([_simple_filter_json("dept")], None)
        extra = result.get_extra_fields_for_filter(read_fields, table_fields)
        self.assertEqual(len(extra), 1)
        self.assertEqual(extra[0].name, "dept")

    def test_no_extra_when_already_projected(self):
        read_fields = [_FakeField("name"), _FakeField("dept")]
        table_fields = read_fields + [_FakeField("age")]
        result = TableQueryAuthResult([_simple_filter_json("dept")], None)
        extra = result.get_extra_fields_for_filter(read_fields, table_fields)
        self.assertEqual(len(extra), 0)

    def test_no_extra_when_no_filter(self):
        result = TableQueryAuthResult(None, None)
        extra = result.get_extra_fields_for_filter(
            [_FakeField("a")], [_FakeField("a"), _FakeField("b")]
        )
        self.assertEqual(len(extra), 0)

    def test_deduplicates_extra_fields(self):
        f1 = _simple_filter_json("dept", "eng")
        f2 = _simple_filter_json("dept", "sales")
        read_fields = [_FakeField("name")]
        table_fields = [_FakeField("name"), _FakeField("dept")]
        result = TableQueryAuthResult([f1, f2], None)
        extra = result.get_extra_fields_for_filter(read_fields, table_fields)
        self.assertEqual(len(extra), 1)


class TestQueryAuthSplit(unittest.TestCase):

    def test_delegates_properties(self):
        auth = TableQueryAuthResult([_simple_filter_json()], None)
        split = _FakeSplit()
        qs = QueryAuthSplit(split, auth)
        self.assertEqual(qs.row_count, 100)
        self.assertEqual(qs.bucket, 0)
        self.assertEqual(qs.files, [])
        self.assertEqual(qs.partition, {})

    def test_merged_row_count_none_with_filter(self):
        auth = TableQueryAuthResult([_simple_filter_json()], None)
        qs = QueryAuthSplit(_FakeSplit(), auth)
        self.assertIsNone(qs.merged_row_count())

    def test_merged_row_count_delegates_without_filter(self):
        auth = TableQueryAuthResult(None, {"col": '{"name":"NULL"}'})
        qs = QueryAuthSplit(_FakeSplit(), auth)
        self.assertEqual(qs.merged_row_count(), 100)

    def test_exposes_inner_split(self):
        split = _FakeSplit("inner")
        qs = QueryAuthSplit(split, TableQueryAuthResult(None, None))
        self.assertIs(qs.split, split)

    def test_exposes_auth_result(self):
        auth = TableQueryAuthResult(None, None)
        qs = QueryAuthSplit(_FakeSplit(), auth)
        self.assertIs(qs.auth_result, auth)


class TestQueryAuthSplitRawConvertible(unittest.TestCase):

    def test_raw_convertible_proxy_true(self):
        inner = _FakeSplit()
        inner.raw_convertible = True
        auth_split = QueryAuthSplit(inner, None)
        self.assertTrue(auth_split.raw_convertible)

    def test_raw_convertible_proxy_false(self):
        inner = _FakeSplit()
        inner.raw_convertible = False
        auth_split = QueryAuthSplit(inner, None)
        self.assertFalse(auth_split.raw_convertible)


class TestQueryAuthSplitAttributeDelegation(unittest.TestCase):

    def test_file_size_delegated(self):
        inner = _FakeSplit()
        inner.file_size = 12345
        auth_split = QueryAuthSplit(inner, TableQueryAuthResult(None, None))
        self.assertEqual(auth_split.file_size, 12345)

    def test_file_paths_delegated(self):
        inner = _FakeSplit()
        inner.file_paths = ["/data/file1.parquet", "/data/file2.parquet"]
        auth_split = QueryAuthSplit(inner, TableQueryAuthResult(None, None))
        self.assertEqual(auth_split.file_paths, ["/data/file1.parquet", "/data/file2.parquet"])

    def test_data_deletion_files_delegated(self):
        inner = _FakeSplit()
        inner.data_deletion_files = [None, "dv-1"]
        auth_split = QueryAuthSplit(inner, TableQueryAuthResult(None, None))
        self.assertEqual(auth_split.data_deletion_files, [None, "dv-1"])

    def test_unknown_attr_raises(self):
        inner = _FakeSplit()
        auth_split = QueryAuthSplit(inner, TableQueryAuthResult(None, None))
        with self.assertRaises(AttributeError):
            _ = auth_split.completely_nonexistent_attr

    def test_pickle_roundtrip(self):
        import pickle
        inner = _FakeSplit()
        inner.file_size = 999
        auth_split = QueryAuthSplit(inner, TableQueryAuthResult(None, None))
        restored = pickle.loads(pickle.dumps(auth_split))
        self.assertEqual(restored.file_size, 999)
        self.assertEqual(restored.row_count, 100)


class TestCoreOptionsQueryAuth(unittest.TestCase):

    def test_disabled_by_default(self):
        opts = CoreOptions(Options({}))
        self.assertFalse(opts.query_auth_enabled)

    def test_enabled_when_set(self):
        opts = CoreOptions(Options({"query-auth.enabled": True}))
        self.assertTrue(opts.query_auth_enabled)

    def test_disabled_when_false(self):
        opts = CoreOptions(Options({"query-auth.enabled": False}))
        self.assertFalse(opts.query_auth_enabled)


if __name__ == "__main__":
    unittest.main()


class TestConvertPlanPreservesSnapshotId(unittest.TestCase):

    def test_convert_plan_preserves_snapshot_id(self):
        from pypaimon.catalog.table_query_auth import TableQueryAuthResult
        from pypaimon.read.plan import Plan
        from unittest.mock import MagicMock

        split = MagicMock()
        original_plan = Plan([split], snapshot_id=42)
        predicate_json = (
            '{"kind":"LEAF","transform":{"name":"FIELD_REF",'
            '"fieldRef":{"name":"id"}},"function":"EQUAL","literals":[1]}'
        )
        auth_result = TableQueryAuthResult(
            filter=[predicate_json],
            column_masking=None)
        converted = auth_result.convert_plan(original_plan)
        assert converted.snapshot_id == 42

    def test_convert_plan_preserves_none_snapshot_id(self):
        from pypaimon.catalog.table_query_auth import TableQueryAuthResult
        from pypaimon.read.plan import Plan
        from unittest.mock import MagicMock

        split = MagicMock()
        original_plan = Plan([split], snapshot_id=None)
        predicate_json = (
            '{"kind":"LEAF","transform":{"name":"FIELD_REF",'
            '"fieldRef":{"name":"id"}},"function":"EQUAL","literals":[1]}'
        )
        auth_result = TableQueryAuthResult(
            filter=[predicate_json],
            column_masking=None)
        converted = auth_result.convert_plan(original_plan)
        assert converted.snapshot_id is None
