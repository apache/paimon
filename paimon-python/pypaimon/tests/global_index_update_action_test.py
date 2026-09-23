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

import unittest
from unittest import mock

from pypaimon.common.options.core_options import (
    CoreOptions,
    GlobalIndexColumnUpdateAction,
)
from pypaimon.common.options.options import Options
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.global_index_update_checker import (
    apply_global_index_update_action,
)


def _field(field_id, name, field_type="STRING"):
    return DataField(field_id, name, AtomicType(field_type))


def _partition_row(values):
    fields = [_field(i, "p{}".format(i)) for i in range(len(values))]
    return GenericRow(list(values), fields)


def _index_entry(file_name, partition, field_id, extra_field_ids=None):
    index_file = IndexFileMeta(
        index_type="BTREE",
        file_name=file_name,
        file_size=1,
        row_count=1,
        global_index_meta=GlobalIndexMeta(
            row_range_start=0,
            row_range_end=0,
            index_field_id=field_id,
            extra_field_ids=extra_field_ids,
            index_meta=b"",
        ),
    )
    return IndexManifestEntry(
        kind=0,
        partition=_partition_row(partition),
        bucket=0,
        index_file=index_file,
    )


class _Table:
    fields = [_field(1, "name"), _field(2, "age", "INT")]

    def __init__(self, options):
        self.options = options


class GlobalIndexUpdateActionTest(unittest.TestCase):

    def test_column_update_action_reads_java_style_string(self):
        options = CoreOptions.from_dict({
            CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(): "DROP_PARTITION_INDEX",
        })

        self.assertEqual(
            GlobalIndexColumnUpdateAction.DROP_PARTITION_INDEX,
            options.global_index_column_update_action(),
        )

    def test_column_update_action_round_trips_python_enum(self):
        options = CoreOptions(Options({}))
        options.set(
            CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION,
            GlobalIndexColumnUpdateAction.DROP_PARTITION_INDEX,
        )

        self.assertEqual(
            "DROP_PARTITION_INDEX",
            options.options.to_map()[
                CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key()
            ],
        )
        self.assertEqual(
            GlobalIndexColumnUpdateAction.DROP_PARTITION_INDEX,
            options.global_index_column_update_action(),
        )

    def test_default_action_rejects_updates_to_global_index_columns(self):
        table = _Table(CoreOptions.from_dict({}))
        entries = [_index_entry("idx-name", (), 1)]

        with mock.patch(
                "pypaimon.write.global_index_update_checker."
                "scan_global_index_entries",
                return_value=entries):
            with self.assertRaisesRegex(RuntimeError, "Conflicted columns"):
                apply_global_index_update_action(table, object(), ["name"], {()})

    def test_default_action_rejects_extra_global_index_columns(self):
        table = _Table(CoreOptions.from_dict({}))
        entries = [_index_entry("idx-name-age", (), 1, extra_field_ids=[2])]

        with mock.patch(
                "pypaimon.write.global_index_update_checker."
                "scan_global_index_entries",
                return_value=entries):
            with self.assertRaises(RuntimeError) as ctx:
                apply_global_index_update_action(table, object(), ["age"], {()})

        self.assertIn("'age'", str(ctx.exception))
        self.assertIn("Conflicted columns: ['age']", str(ctx.exception))

    def test_ignore_action_skips_global_index_handling(self):
        options = CoreOptions.from_dict({
            CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(): "IGNORE",
        })
        table = _Table(options)

        with mock.patch(
                "pypaimon.write.global_index_update_checker."
                "scan_global_index_entries") as scan:
            messages = apply_global_index_update_action(
                table,
                object(),
                ["name"],
                {()},
            )

        self.assertEqual([], messages)
        scan.assert_not_called()

    def test_drop_partition_index_builds_deletes_for_affected_partition(self):
        options = CoreOptions.from_dict({
            CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(): "DROP_PARTITION_INDEX",
        })
        table = _Table(options)
        entries = [
            _index_entry("idx-name-p0", ("2026-06-17",), 1),
            _index_entry("idx-age-p0", ("2026-06-17",), 2),
            _index_entry("idx-name-p1", ("2026-06-18",), 1),
        ]

        with mock.patch(
                "pypaimon.write.global_index_update_checker."
                "scan_global_index_entries",
                return_value=entries):
            messages = apply_global_index_update_action(
                table,
                object(),
                ["name"],
                {("2026-06-17",)},
            )

        self.assertEqual(1, len(messages))
        self.assertEqual(("2026-06-17",), messages[0].partition)
        self.assertEqual(
            ["idx-name-p0"],
            [d.index_file.file_name for d in messages[0].index_deletes],
        )
        self.assertEqual([1], [d.kind for d in messages[0].index_deletes])

    def test_drop_partition_index_builds_deletes_for_extra_column_update(self):
        options = CoreOptions.from_dict({
            CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(): "DROP_PARTITION_INDEX",
        })
        table = _Table(options)
        entries = [
            _index_entry(
                "idx-name-age-p0",
                ("2026-06-17",),
                1,
                extra_field_ids=[2],
            ),
        ]

        with mock.patch(
                "pypaimon.write.global_index_update_checker."
                "scan_global_index_entries",
                return_value=entries):
            messages = apply_global_index_update_action(
                table,
                object(),
                ["age"],
                {("2026-06-17",)},
            )

        self.assertEqual(
            ["idx-name-age-p0"],
            [d.index_file.file_name for d in messages[0].index_deletes],
        )


if __name__ == "__main__":
    unittest.main()
