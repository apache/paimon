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

"""Floating-point key bounds preserve overlapping split groups."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.interval_partition import IntervalPartition, default_key_comparator
from pypaimon.read.scanner.primary_key_table_split_generator import PrimaryKeyTableSplitGenerator
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow, GenericRowDeserializer, GenericRowSerializer


def _key_fields(type_name):
    return [DataField(0, 'f', AtomicType(type_name)), DataField(1, 'i', AtomicType('INT'))]


def _file(name, minimum, maximum, fields):
    def key(values):
        row = GenericRow(list(values), fields)
        return GenericRowDeserializer.from_bytes(GenericRowSerializer.to_bytes(row), fields)

    return DataFileMeta(
        file_name=name, file_size=100, row_count=3,
        min_key=key(minimum), max_key=key(maximum),
        key_stats=SimpleStats.empty_stats(), value_stats=SimpleStats.empty_stats(),
        min_sequence_number=0, max_sequence_number=0, schema_id=0, level=0, extra_files=[])


@pytest.mark.parametrize('type_name', ['FLOAT', 'DOUBLE'])
def test_signed_zero_key_ranges_keep_versions_in_one_split(type_name):
    fields = _key_fields(type_name)
    files = [
        _file('broad', (-0.0, 0), (0.0, 10), fields),
        _file('point', (-0.0, 100), (-0.0, 100), fields),
    ]
    sections = IntervalPartition(files).partition()
    assert len(sections) == 1
    assert sorted([f.file_name for f in run.files] for run in sections[0]) == [['broad'], ['point']]

    table = SimpleNamespace(table_path='/tmp/interval-test', options=CoreOptions(Options({})))
    entries = [ManifestEntry(0, GenericRow([], []), 0, 1, file) for file in files]
    splits = PrimaryKeyTableSplitGenerator(table, 1, 1).create_splits(entries)
    assert len(splits) == 1
    assert sorted(file.file_name for file in splits[0].files) == ['broad', 'point']
    assert not splits[0].raw_convertible


def test_decimal_keys_keep_numeric_equality():
    fields = _key_fields('DECIMAL(10, 2)')
    left = GenericRow([Decimal('-0'), 1], fields)
    right = GenericRow([Decimal('0'), 1], fields)
    assert default_key_comparator(left, right) == 0


@pytest.mark.parametrize('type_name', ['FLOAT', 'DOUBLE'])
def test_disjoint_finite_ranges_still_form_separate_sections(type_name):
    fields = _key_fields(type_name)
    sections = IntervalPartition([
        _file('high', (3.0, 0), (4.0, 0), fields),
        _file('low', (1.0, 0), (2.0, 0), fields),
    ]).partition()
    assert [[f.file_name for run in section for f in run.files] for section in sections] == [
        ['low'], ['high']]
