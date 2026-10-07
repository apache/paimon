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

"""Golden Java shard fixtures also guard the non-native planner."""

from types import SimpleNamespace

import pytest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.read.scan_distribution import java_file_name_shard
from pypaimon.read.scanner.primary_key_table_split_generator import PrimaryKeyTableSplitGenerator


@pytest.mark.parametrize('name,count,expected', [
    ('Aa', 3, 0), ('BB', 3, 0),
    ('a', 3, 1), ('😀', 3, 1),
    # Java hashes these strings to Integer.MIN_VALUE and a negative value.
    ('polygenelubricants', 3, 2), ('polygenelubricants', 7, 2),
    ('data-0.parquet', 3, 2),
    ('', 5, 0), ('a', 1, 0),
])
def test_file_name_shards_match_java_golden_values(name, count, expected):
    assert java_file_name_shard(name, count) == expected


@pytest.mark.parametrize('engine,dv,by_file', [
    ('deduplicate', False, False),
    ('partial-update', False, False),
    ('aggregation', False, False),
    ('first-row', False, True),
    ('deduplicate', True, True),
])
def test_primary_key_shard_rule_matches_java_raw_convertibility(engine, dv, by_file):
    table = SimpleNamespace(options=CoreOptions(Options({
        'merge-engine': engine, 'deletion-vectors.enabled': str(dv).lower(),
    })))
    generator = PrimaryKeyTableSplitGenerator(table, 100, 1)
    names = ['Aa', 'BB', 'a', '😀', 'polygenelubricants']
    entries = [SimpleNamespace(bucket=0, file=SimpleNamespace(file_name=name)) for name in names]
    expected = [['Aa', 'BB'], ['a', '😀'], ['polygenelubricants']] if by_file else [names, [], []]
    covered = []
    for shard in range(3):
        generator.with_shard(shard, 3)
        selected = generator._filter_by_shard(entries)
        assert [entry.file.file_name for entry in selected] == expected[shard]
        covered.extend(selected)
    assert len(covered) == len(entries)
    assert {id(entry) for entry in covered} == {id(entry) for entry in entries}
