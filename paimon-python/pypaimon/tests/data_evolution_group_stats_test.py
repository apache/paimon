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

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.manifest.simple_stats_evolutions import SimpleStatsEvolutions
from pypaimon.read.scanner.data_evolution_split_generator import \
    DataEvolutionSplitGenerator
from pypaimon.read.scanner.data_evolution_stats import \
    DataEvolutionGroupStatsFilter
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow


def _empty_stats():
    return SimpleStats(GenericRow([], []), GenericRow([], []), [])


def _file(
    name,
    first_row_id,
    row_count,
    fields,
    min_values,
    max_values,
    null_counts=None,
    sequence=0,
    schema_id=0,
    write_cols=None,
    value_stats_cols=None,
):
    stats_fields = fields
    if value_stats_cols is not None:
        by_name = {field.name: field for field in fields}
        stats_fields = [by_name[name] for name in value_stats_cols]
    elif write_cols is not None:
        by_name = {field.name: field for field in fields}
        stats_fields = [by_name[name] for name in write_cols
                        if name in by_name]
    return DataFileMeta.create(
        file_name=name,
        file_size=100,
        row_count=row_count,
        min_key=GenericRow([], []),
        max_key=GenericRow([], []),
        key_stats=_empty_stats(),
        value_stats=SimpleStats(
            GenericRow(min_values, stats_fields),
            GenericRow(max_values, stats_fields),
            ([0] * len(stats_fields)
             if null_counts is None else null_counts),
        ),
        min_sequence_number=sequence,
        max_sequence_number=sequence,
        schema_id=schema_id,
        level=0,
        extra_files=[],
        first_row_id=first_row_id,
        write_cols=write_cols,
        value_stats_cols=value_stats_cols,
    )


class DataEvolutionGroupStatsFilterTest(unittest.TestCase):

    @staticmethod
    def _filter(predicate, schemas, current_schema_id):
        current_fields = schemas[current_schema_id]
        return DataEvolutionGroupStatsFilter(
            predicate,
            current_fields,
            lambda schema_id: schemas[schema_id],
            SimpleStatsEvolutions(
                lambda schema_id: schemas[schema_id], current_schema_id),
        )

    def test_merges_stats_from_latest_file_for_each_column(self):
        fields = [
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'left_value', AtomicType('INT')),
            DataField(2, 'right_value', AtomicType('INT')),
        ]
        base = _file(
            'base.parquet', 0, 10, fields, [0, 10], [9, 19],
            write_cols=['id', 'left_value'])
        delta = _file(
            'delta.parquet', 0, 10, fields, [100], [109], sequence=1,
            write_cols=['right_value'])
        builder = PredicateBuilder(fields)

        self.assertTrue(self._filter(
            builder.and_predicates([
                builder.equal('left_value', 15),
                builder.equal('right_value', 105),
            ]), {0: fields}, 0).may_match([base, delta]))
        self.assertFalse(self._filter(
            builder.equal('right_value', 500),
            {0: fields}, 0).may_match([base, delta]))

    def test_missing_or_corrupt_stats_fail_open(self):
        fields = [DataField(0, 'value', AtomicType('INT'))]
        builder = PredicateBuilder(fields)
        without_stats = _file(
            'no-stats.parquet', 0, 10, fields, [], [],
            write_cols=['value'], value_stats_cols=[])
        corrupt = _file(
            'corrupt.parquet', 10, 10, fields, [10], [20])
        corrupt.value_stats = _empty_stats()

        for file in [without_stats, corrupt]:
            with self.subTest(file=file.file_name):
                self.assertTrue(self._filter(
                    builder.equal('value', 1000),
                    {0: fields}, 0).may_match([file]))

    def test_value_stats_cols_controls_covered_fields(self):
        fields = [
            DataField(0, 'without_stats', AtomicType('INT')),
            DataField(1, 'with_stats', AtomicType('INT')),
        ]
        file = _file(
            'data.parquet', 0, 10, fields, [20], [29],
            write_cols=['without_stats', 'with_stats'],
            value_stats_cols=['with_stats'])
        builder = PredicateBuilder(fields)

        self.assertFalse(self._filter(
            builder.equal('with_stats', 1000),
            {0: fields}, 0).may_match([file]))
        self.assertTrue(self._filter(
            builder.equal('without_stats', 1000),
            {0: fields}, 0).may_match([file]))

    def test_add_column_uses_implicit_null_stats(self):
        old_fields = [DataField(0, 'id', AtomicType('INT'))]
        current_fields = old_fields + [
            DataField(1, 'added', AtomicType('STRING'))]
        old_file = _file(
            'old.parquet', 0, 10, old_fields, [0], [9], schema_id=0)
        builder = PredicateBuilder(current_fields)
        schemas = {0: old_fields, 1: current_fields}

        self.assertTrue(self._filter(
            builder.is_null('added'), schemas, 1).may_match([old_file]))
        self.assertFalse(self._filter(
            builder.is_not_null('added'), schemas, 1).may_match([old_file]))
        self.assertFalse(self._filter(
            builder.equal('added', 'x'), schemas, 1).may_match([old_file]))

    def test_schema_rename_uses_field_id_and_type_change_fails_open(self):
        old_fields = [DataField(0, 'old_name', AtomicType('INT'))]
        renamed_fields = [DataField(0, 'new_name', AtomicType('INT'))]
        changed_fields = [DataField(0, 'new_name', AtomicType('BIGINT'))]
        old_file = _file(
            'old.parquet', 0, 10, old_fields, [0], [9], schema_id=0)

        renamed_builder = PredicateBuilder(renamed_fields)
        self.assertFalse(self._filter(
            renamed_builder.equal('new_name', 50),
            {0: old_fields, 1: renamed_fields}, 1).may_match([old_file]))

        changed_builder = PredicateBuilder(changed_fields)
        self.assertTrue(self._filter(
            changed_builder.equal('new_name', 50),
            {0: old_fields, 2: changed_fields}, 2).may_match([old_file]))

    def test_blob_and_vector_files_do_not_supply_predicate_stats(self):
        fields = [
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('BYTES')),
        ]
        base = _file(
            'base.parquet', 0, 10, fields, [0], [9],
            write_cols=['id'])
        builder = PredicateBuilder(fields)
        for name in ['payload.blob', 'payload.vector.parquet']:
            special = _file(
                name, 0, 10, fields, [b'a'], [b'z'], sequence=1,
                write_cols=['payload'])
            with self.subTest(name=name):
                stats_filter = self._filter(
                    builder.equal('id', 50), {0: fields}, 0)
                self.assertFalse(stats_filter.may_match([base, special]))
                stats_filter = self._filter(
                    builder.equal('payload', b'not-present'),
                    {0: fields}, 0)
                self.assertTrue(stats_filter.may_match([base, special]))

    def test_partial_newer_file_fails_open(self):
        fields = [DataField(0, 'value', AtomicType('INT'))]
        base = _file('base.parquet', 0, 10, fields, [0], [9])
        partial_delta = _file(
            'delta.parquet', 3, 3, fields, [100], [102], sequence=1,
            write_cols=['value'])
        stats_filter = self._filter(
            PredicateBuilder(fields).equal('value', 5), {0: fields}, 0)

        self.assertTrue(stats_filter.may_match([base, partial_delta]))


class DataEvolutionGroupStatsPlanningTest(unittest.TestCase):

    def test_prunes_groups_before_split_packing(self):
        fields = [DataField(0, 'id', AtomicType('INT'))]
        files = [
            _file('match.parquet', 0, 10, fields, [0], [9]),
            _file('fallback-1.parquet', 10, 10, fields, [20], [29]),
            _file('fallback-2.parquet', 20, 10, fields, [40], [49]),
        ]
        entries = [ManifestEntry(
            kind=0,
            partition=GenericRow([], []),
            bucket=0,
            total_buckets=1,
            file=file,
        ) for file in files]

        class _Options:
            options = {}

        class _Table:
            table_path = '/tmp/table'
            options = _Options()

        predicate = PredicateBuilder(fields).equal('id', 5)
        group_filter = DataEvolutionGroupStatsFilter(
            predicate,
            fields,
            lambda schema_id: fields,
            SimpleStatsEvolutions(lambda schema_id: fields, 0),
        )
        without_pruning = DataEvolutionSplitGenerator(
            _Table(), 1024 * 1024, 0).create_splits(entries)
        with_pruning = DataEvolutionSplitGenerator(
            _Table(), 1024 * 1024, 0,
            group_stats_filter=group_filter).create_splits(entries)

        self.assertEqual(3, sum(len(split.files) for split in without_pruning))
        self.assertEqual(1, sum(len(split.files) for split in with_pruning))
        self.assertEqual(
            ['match.parquet'],
            [file.file_name for split in with_pruning for file in split.files],
        )


if __name__ == '__main__':
    unittest.main()
