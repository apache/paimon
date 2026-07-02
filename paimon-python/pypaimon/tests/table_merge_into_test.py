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

import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon.table.data_evolution_merge_into import (
    WhenMatched,
    WhenNotMatched,
    lit,
    source_col,
    target_col,
)
from pypaimon.tests.data_evolution_test_helpers import (
    BatchModeMixin,
    DataEvolutionTestBase,
    StreamModeMixin,
)

try:
    import datafusion  # noqa: F401

    _HAS_DATAFUSION = True
except ImportError:
    _HAS_DATAFUSION = False

_SKIP_CONDITION = not _HAS_DATAFUSION
_SKIP_REASON = "pypaimon[sql] is required for condition expressions"


class TableMergeIntoTest(BatchModeMixin, DataEvolutionTestBase, unittest.TestCase):

    def _read_sorted(self, table):
        return self._read_all(table).sort_by("id").to_pydict()

    def _read_projected_sorted(self, table, projection):
        rb = table.new_read_builder().with_projection(projection)
        return rb.new_read().to_arrow(rb.new_scan().plan().splits()).sort_by(
            "id"
        ).to_pydict()

    def _row_ids_by_id(self, table):
        rows = self._read_projected_sorted(table, ["id", "_ROW_ID"])
        return dict(zip(rows["id"], rows["_ROW_ID"]))

    def _merge_and_commit(self, table, source, **kwargs):
        wb = table.new_batch_write_builder()
        table_update = wb.new_update()
        msgs = table_update.merge_into(source, **kwargs)
        commit = wb.new_commit()
        commit.commit(msgs)
        commit.close()
        return msgs

    def test_when_matched_action_constructors(self):
        update = WhenMatched.update(
            {"age": source_col("age")},
            condition="s.age > t.age",
        )
        delete = WhenMatched.delete(condition="s.deleted = TRUE")

        self.assertEqual({"age": source_col("age")}, update.update)
        self.assertEqual("s.age > t.age", update.condition)
        self.assertFalse(update.delete)
        self.assertIsNone(delete.update)
        self.assertEqual("s.deleted = TRUE", delete.condition)
        self.assertTrue(delete.delete)

    def test_ray_merge_rejects_mixed_update_delete_duplicate_row_ids(self):
        import pypaimon.ray.data_evolution_merge_into as ray_merge

        with self.assertRaisesRegex(ValueError, "multiple source rows"):
            ray_merge._validate_disjoint_action_row_ids([7], [7])

    def test_ray_execute_validates_mixed_update_delete_duplicate_row_ids(self):
        import pypaimon.ray.data_evolution_merge_into as ray_merge

        with patch.object(
                ray_merge,
                "distributed_update_apply",
                return_value=([], 1, [7])
        ), patch.object(
                ray_merge,
                "distributed_delete_apply",
                return_value=([], 1, [7])
        ):
            with self.assertRaisesRegex(ValueError, "multiple source rows"):
                ray_merge._execute_and_commit(
                    table=object(),
                    update_ds=object(),
                    delete_ds=object(),
                    insert_ds=None,
                    update_cols_union=["age"],
                    base_snapshot=None,
                    num_partitions=1,
                    ray_remote_args=None,
                    concurrency=None,
                )

    def test_table_merge_into_updates_and_inserts(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": ["Alice", "Bob"],
            "age": pa.array([25, 30], type=pa.int32()),
            "city": ["NYC", "LA"],
        }, schema=self.pa_schema))

        source = pa.Table.from_pydict({
            "id": pa.array([2, 3], type=pa.int32()),
            "name": ["Bobby", "Cindy"],
            "age": pa.array([31, 22], type=pa.int32()),
            "city": ["LA2", "SF"],
        }, schema=self.pa_schema)

        msgs = self._merge_and_commit(
            target,
            source,
            on=["id"],
            when_matched=[WhenMatched.update("*")],
            when_not_matched=[WhenNotMatched(insert="*")],
        )

        self.assertTrue(msgs)
        self.assertEqual(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bobby", "Cindy"],
                "age": [25, 31, 22],
                "city": ["NYC", "LA2", "SF"],
            },
            self._read_sorted(target),
        )

    def test_table_merge_into_insert_only_on_empty_table(self):
        target = self._create_table()
        source = pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": ["Alice", "Bob"],
            "age": pa.array([25, 30], type=pa.int32()),
            "city": ["NYC", "LA"],
        }, schema=self.pa_schema)

        msgs = self._merge_and_commit(
            target,
            source,
            on=["id"],
            when_not_matched=[WhenNotMatched(insert="*")],
        )

        self.assertTrue(msgs)
        self.assertEqual(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "age": [25, 30],
                "city": ["NYC", "LA"],
            },
            self._read_sorted(target),
        )

    def test_table_merge_into_with_renamed_on_key_fills_insert_key(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alice"],
            "age": pa.array([25], type=pa.int32()),
            "city": ["NYC"],
        }, schema=self.pa_schema))
        source = pa.Table.from_pydict({
            "source_id": pa.array([1, 2], type=pa.int32()),
            "new_name": ["Alicia", "Bob"],
            "new_age": pa.array([26, 30], type=pa.int32()),
            "new_city": ["SF", "LA"],
        })

        self._merge_and_commit(
            target,
            source,
            on={"id": "source_id"},
            when_matched=[
                WhenMatched.update({
                    "name": source_col("new_name"),
                    "age": source_col("new_age"),
                    "city": source_col("new_city"),
                }),
            ],
            when_not_matched=[
                WhenNotMatched(insert={
                    "name": source_col("new_name"),
                    "age": source_col("new_age"),
                    "city": source_col("new_city"),
                }),
            ],
        )

        self.assertEqual(
            {
                "id": [1, 2],
                "name": ["Alicia", "Bob"],
                "age": [26, 30],
                "city": ["SF", "LA"],
            },
            self._read_sorted(target),
        )

    def test_table_merge_into_ignores_target_self_assignment(self):
        for assignment in [target_col("name"), "t.name"]:
            with self.subTest(assignment=assignment):
                target = self._create_table()
                self._write_arrow(target, pa.Table.from_pydict({
                    "id": pa.array([1], type=pa.int32()),
                    "name": ["Alice"],
                    "age": pa.array([25], type=pa.int32()),
                    "city": ["NYC"],
                }, schema=self.pa_schema))
                row_ids_before = self._row_ids_by_id(target)
                source = pa.Table.from_pydict({
                    "id": pa.array([1], type=pa.int32()),
                    "name": ["Alicia"],
                    "age": pa.array([26], type=pa.int32()),
                    "city": ["SF"],
                }, schema=self.pa_schema)

                msgs = self._merge_and_commit(
                    target,
                    source,
                    on=["id"],
                    when_matched=[WhenMatched.update({"name": assignment})],
                )

                self.assertEqual([], msgs)
                self.assertEqual(
                    {
                        "id": [1],
                        "name": ["Alice"],
                        "age": [25],
                        "city": ["NYC"],
                    },
                    self._read_sorted(target),
                )
                self.assertEqual(row_ids_before, self._row_ids_by_id(target))

    def test_table_merge_into_source_assignment_same_name_is_modified(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alice"],
            "age": pa.array([25], type=pa.int32()),
            "city": ["NYC"],
        }, schema=self.pa_schema))
        source = pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alicia"],
            "age": pa.array([26], type=pa.int32()),
            "city": ["SF"],
        }, schema=self.pa_schema)

        msgs = self._merge_and_commit(
            target,
            source,
            on=["id"],
            when_matched=[WhenMatched.update({"name": source_col("name")})],
        )

        self.assertTrue(msgs)
        self.assertEqual(["Alicia"], self._read_sorted(target)["name"])
        self.assertEqual([25], self._read_sorted(target)["age"])

    def test_table_merge_into_rejects_nested_assignment_key(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alice"],
            "age": pa.array([25], type=pa.int32()),
            "city": ["NYC"],
        }, schema=self.pa_schema))
        source = pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alicia"],
            "age": pa.array([26], type=pa.int32()),
            "city": ["SF"],
        }, schema=self.pa_schema)

        with self.assertRaisesRegex(ValueError, "unknown target column"):
            target.new_batch_write_builder().new_update().merge_into(
                source,
                on=["id"],
                when_matched=[WhenMatched.update({"name.first": lit("A")})],
            )

    def test_table_merge_into_rejects_partition_column_update(self):
        target = self._create_table(partition_keys=["city"])
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alice"],
            "age": pa.array([25], type=pa.int32()),
            "city": ["NYC"],
        }, schema=self.pa_schema))
        source = pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alice"],
            "age": pa.array([26], type=pa.int32()),
            "city": ["SF"],
        }, schema=self.pa_schema)

        with self.assertRaisesRegex(ValueError, "partition columns"):
            target.new_batch_write_builder().new_update().merge_into(
                source,
                on=["id"],
                when_matched=[WhenMatched.update({"city": source_col("city")})],
            )

    def test_table_merge_into_preserves_row_ids_for_matched_rows(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": ["Alice", "Bob"],
            "age": pa.array([25, 30], type=pa.int32()),
            "city": ["NYC", "LA"],
        }, schema=self.pa_schema))
        row_ids_before = self._row_ids_by_id(target)

        self._merge_and_commit(
            target,
            pa.Table.from_pydict({
                "id": pa.array([2, 3], type=pa.int32()),
                "name": ["Bobby", "Cindy"],
                "age": pa.array([31, 22], type=pa.int32()),
                "city": ["LA2", "SF"],
            }, schema=self.pa_schema),
            on=["id"],
            when_matched=[WhenMatched.update("*")],
            when_not_matched=[WhenNotMatched(insert="*")],
        )

        row_ids_after = self._row_ids_by_id(target)
        self.assertEqual(row_ids_before[1], row_ids_after[1])
        self.assertEqual(row_ids_before[2], row_ids_after[2])
        self.assertGreater(row_ids_after[3], max(row_ids_before.values()))

    def test_table_merge_into_delete_matched_rows(self):
        options = dict(self.table_options)
        options["deletion-vectors.enabled"] = "true"
        target = self._create_table(options=options)
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": ["Alice", "Bob", "Charlie"],
            "age": pa.array([25, 30, 35], type=pa.int32()),
            "city": ["NYC", "LA", "Chicago"],
        }, schema=self.pa_schema))

        msgs = self._merge_and_commit(
            target,
            pa.Table.from_pydict({
                "id": pa.array([2, 3], type=pa.int32()),
                "name": ["ignored", "ignored"],
                "age": pa.array([99, 99], type=pa.int32()),
                "city": ["ignored", "ignored"],
            }, schema=self.pa_schema),
            on=["id"],
            when_matched=[WhenMatched.delete()],
        )

        self.assertEqual(1, sum(len(m.index_adds) for m in msgs))
        self.assertEqual(
            {
                "id": [1],
                "name": ["Alice"],
                "age": [25],
                "city": ["NYC"],
            },
            self._read_sorted(target),
        )

    def test_table_merge_into_second_round_updates_first_round_insert(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alice"],
            "age": pa.array([25], type=pa.int32()),
            "city": ["NYC"],
        }, schema=self.pa_schema))

        self._merge_and_commit(
            target,
            pa.Table.from_pydict({
                "id": pa.array([2], type=pa.int32()),
                "name": ["Bob"],
                "age": pa.array([30], type=pa.int32()),
                "city": ["LA"],
            }, schema=self.pa_schema),
            on=["id"],
            when_not_matched=[WhenNotMatched(insert="*")],
        )
        row_ids_after_insert = self._row_ids_by_id(target)

        self._merge_and_commit(
            target,
            pa.Table.from_pydict({
                "id": pa.array([2], type=pa.int32()),
                "name": ["Bobby"],
                "age": pa.array([31], type=pa.int32()),
                "city": ["LA2"],
            }, schema=self.pa_schema),
            on=["id"],
            when_matched=[WhenMatched.update("*")],
        )

        self.assertEqual(
            {
                "id": [1, 2],
                "name": ["Alice", "Bobby"],
                "age": [25, 31],
                "city": ["NYC", "LA2"],
            },
            self._read_sorted(target),
        )
        self.assertEqual(row_ids_after_insert, self._row_ids_by_id(target))

    def test_table_merge_into_accepts_paimon_source_table(self):
        target = self._create_table()
        source = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alice"],
            "age": pa.array([25], type=pa.int32()),
            "city": ["NYC"],
        }, schema=self.pa_schema))
        self._write_arrow(source, pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": ["Alicia", "Bob"],
            "age": pa.array([26, 30], type=pa.int32()),
            "city": ["SF", "LA"],
        }, schema=self.pa_schema))

        msgs = self._merge_and_commit(
            target,
            source,
            on=["id"],
            when_matched=[WhenMatched.update("*")],
            when_not_matched=[WhenNotMatched(insert="*")],
        )

        self.assertTrue(msgs)
        self.assertEqual(
            {
                "id": [1, 2],
                "name": ["Alicia", "Bob"],
                "age": [26, 30],
                "city": ["SF", "LA"],
            },
            self._read_sorted(target),
        )

    def test_table_merge_into_updates_and_inserts_blob_column(self):
        blob_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("payload", pa.large_binary()),
        ])
        target = self._create_table(pa_schema=blob_schema)
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": ["Alice", "Bob"],
            "payload": pa.array(
                [b"blob-1", b"blob-2"], type=pa.large_binary()
            ),
        }, schema=blob_schema))

        msgs = self._merge_and_commit(
            target,
            pa.Table.from_pydict({
                "id": pa.array([2, 3], type=pa.int32()),
                "name": ["Bobby", "Cindy"],
                "payload": pa.array(
                    [b"blob-2-updated", b"blob-3"], type=pa.large_binary()
                ),
            }, schema=blob_schema),
            on=["id"],
            when_matched=[WhenMatched.update("*")],
            when_not_matched=[WhenNotMatched(insert="*")],
        )

        self.assertTrue(msgs)
        self.assertEqual(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bobby", "Cindy"],
                "payload": [b"blob-1", b"blob-2-updated", b"blob-3"],
            },
            self._read_projected_sorted(target, ["id", "name", "payload"]),
        )

    def test_table_merge_into_inserts_null_for_unspecified_blob_column(self):
        blob_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("payload", pa.large_binary()),
        ])
        source_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
        ])
        target = self._create_table(pa_schema=blob_schema)

        msgs = self._merge_and_commit(
            target,
            pa.Table.from_pydict({
                "id": pa.array([1], type=pa.int32()),
                "name": ["Alice"],
            }, schema=source_schema),
            on=["id"],
            when_not_matched=[WhenNotMatched(insert={
                "id": source_col("id"),
                "name": source_col("name"),
            })],
        )

        self.assertTrue(msgs)
        self.assertEqual(
            {
                "id": [1],
                "name": ["Alice"],
                "payload": [None],
            },
            self._read_projected_sorted(target, ["id", "name", "payload"]),
        )

    @unittest.skipIf(_SKIP_CONDITION, _SKIP_REASON)
    def test_table_merge_into_condition_clauses(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": ["old_1", "old_2"],
            "age": pa.array([10, 40], type=pa.int32()),
            "city": ["NYC", "LA"],
        }, schema=self.pa_schema))

        source = pa.Table.from_pydict({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": ["new_1", "new_2", "new_3"],
            "age": pa.array([15, 35, 50], type=pa.int32()),
            "city": ["SF", "SEA", "DAL"],
        }, schema=self.pa_schema)

        msgs = self._merge_and_commit(
            target,
            source,
            on=["id"],
            when_matched=[
                WhenMatched.update({"age": lit(99)}, condition="s.age > t.age"),
                WhenMatched.update({"name": lit("kept")}),
            ],
            when_not_matched=[
                WhenNotMatched(insert="*", condition="s.age > 45"),
            ],
        )

        self.assertTrue(msgs)
        self.assertEqual(
            {
                "id": [1, 2, 3],
                "name": ["old_1", "kept", "new_3"],
                "age": [99, 40, 50],
                "city": ["NYC", "LA", "DAL"],
            },
            self._read_sorted(target),
        )

    def test_table_merge_into_duplicate_source_match_rejected(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alice"],
            "age": pa.array([25], type=pa.int32()),
            "city": ["NYC"],
        }, schema=self.pa_schema))

        source = pa.Table.from_pydict({
            "id": pa.array([1, 1], type=pa.int32()),
            "name": ["A1", "A2"],
            "age": pa.array([26, 27], type=pa.int32()),
            "city": ["SF", "LA"],
        }, schema=self.pa_schema)

        with self.assertRaisesRegex(ValueError, "multiple source rows"):
            target.new_batch_write_builder().new_update().merge_into(
                source,
                on=["id"],
                when_matched=[WhenMatched.update({"age": "s.age"})],
            )

    def test_table_merge_into_self_merge_by_row_id(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": ["Alice", "Bob"],
            "age": pa.array([25, 30], type=pa.int32()),
            "city": ["NYC", "LA"],
        }, schema=self.pa_schema))

        msgs = self._merge_and_commit(
            target,
            target,
            on=["_ROW_ID"],
            when_matched=[WhenMatched.update({"name": lit("updated")})],
        )

        self.assertTrue(msgs)
        self.assertEqual(["updated", "updated"], self._read_sorted(target)["name"])


class StreamTableMergeIntoTest(StreamModeMixin, DataEvolutionTestBase, unittest.TestCase):

    def _read_sorted(self, table):
        return self._read_all(table).sort_by("id").to_pydict()

    def test_stream_table_update_merge_into(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alice"],
            "age": pa.array([25], type=pa.int32()),
            "city": ["NYC"],
        }, schema=self.pa_schema))

        source = pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": ["Alicia", "Bob"],
            "age": pa.array([26, 30], type=pa.int32()),
            "city": ["SF", "LA"],
        }, schema=self.pa_schema)

        base_snapshot_id = self._latest_snapshot_id(target)
        wb = target.new_stream_write_builder()
        cid = self._next_commit_id()
        msgs = wb.new_update().merge_into(
            source,
            on=["id"],
            when_matched=[WhenMatched.update("*")],
            when_not_matched=[WhenNotMatched(insert="*")],
            commit_identifier=cid,
        )
        commit = wb.new_commit()
        commit.commit(msgs, cid)
        commit.close()

        self._assert_stream_builder_snapshots(
            target, wb, base_snapshot_id, [cid]
        )
        self.assertEqual(
            {
                "id": [1, 2],
                "name": ["Alicia", "Bob"],
                "age": [26, 30],
                "city": ["SF", "LA"],
            },
            self._read_sorted(target),
        )

    def test_stream_table_update_merge_into_multiple_rounds(self):
        target = self._create_table()
        self._write_arrow(target, pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": ["Alice"],
            "age": pa.array([25], type=pa.int32()),
            "city": ["NYC"],
        }, schema=self.pa_schema))

        base_snapshot_id = self._latest_snapshot_id(target)
        wb = target.new_stream_write_builder()
        commit = wb.new_commit()

        cid1 = self._next_commit_id()
        msgs1 = wb.new_update().merge_into(
            pa.Table.from_pydict({
                "id": pa.array([2], type=pa.int32()),
                "name": ["Bob"],
                "age": pa.array([30], type=pa.int32()),
                "city": ["LA"],
            }, schema=self.pa_schema),
            on=["id"],
            when_not_matched=[WhenNotMatched(insert="*")],
            commit_identifier=cid1,
        )
        commit.commit(msgs1, cid1)

        cid2 = self._next_commit_id()
        msgs2 = wb.new_update().merge_into(
            pa.Table.from_pydict({
                "id": pa.array([2], type=pa.int32()),
                "name": ["Bobby"],
                "age": pa.array([31], type=pa.int32()),
                "city": ["LA2"],
            }, schema=self.pa_schema),
            on=["id"],
            when_matched=[WhenMatched.update("*")],
            commit_identifier=cid2,
        )
        commit.commit(msgs2, cid2)
        commit.close()

        self._assert_stream_builder_snapshots(
            target, wb, base_snapshot_id, [cid1, cid2]
        )
        self.assertEqual(
            {
                "id": [1, 2],
                "name": ["Alice", "Bobby"],
                "age": [25, 31],
                "city": ["NYC", "LA2"],
            },
            self._read_sorted(target),
        )


if __name__ == "__main__":
    unittest.main()
