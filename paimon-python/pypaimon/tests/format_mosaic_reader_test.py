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

import struct
from datetime import datetime

from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.reader.format_mosaic_reader import _convert_stats_value
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow


def test_timestamp9_stats_rounds_max_up_for_row_group_skipping():
    fields = [
        DataField(0, "ts", AtomicType("TIMESTAMP(9)")),
    ]
    stats_bytes = struct.pack(">qi", 123, 456789)
    min_value = _convert_stats_value(stats_bytes, fields[0].type)
    max_value = _convert_stats_value(
        stats_bytes, fields[0].type, round_up_timestamp=True)
    stats = SimpleStats(
        GenericRow([min_value], fields),
        GenericRow([max_value], fields),
        [0])

    predicate = PredicateBuilder(fields).greater_than(
        "ts", datetime(1970, 1, 1, 0, 0, 0, 123456))

    assert min_value == datetime(1970, 1, 1, 0, 0, 0, 123456)
    assert max_value == datetime(1970, 1, 1, 0, 0, 0, 123457)
    assert predicate.test_by_simple_stats(stats, 1)


def test_timestamp9_stats_keeps_exact_microsecond_max():
    value = _convert_stats_value(
        struct.pack(">qi", 123, 456000),
        AtomicType("TIMESTAMP(9)"),
        round_up_timestamp=True)

    assert value == datetime(1970, 1, 1, 0, 0, 0, 123456)
