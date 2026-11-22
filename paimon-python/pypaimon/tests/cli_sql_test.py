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

import json
from datetime import datetime

import pyarrow as pa
import pytest

from pypaimon.cli import cli_sql


@pytest.mark.parametrize('unit,value,timezone,expected', [
    ('us', 1123456, None, '1970-01-01T00:00:01.123456'),
    ('ns', 1123456789, 'Asia/Shanghai', '1970-01-01T08:00:01.123456789+08:00'),
    ('ns', -(2 ** 63), None, '1677-09-21T00:12:43.145224192'),
    ('ns', -(2 ** 63), '+08:00', '1677-09-21T08:12:43.145224192+08:00'),
    ('ns', -(2 ** 63), '-05:00', '1677-09-20T19:12:43.145224192-05:00'),
])
def test_json_timestamp_precision_and_timezone(capsys, unit, value, timezone, expected):
    table = pa.table({'ts': pa.array([value, None], type=pa.timestamp(unit, tz=timezone))})

    cli_sql._print_table(table, 'json')

    assert json.loads(capsys.readouterr().out) == [
        {'ts': expected},
        {'ts': None},
    ]


def test_json_nullable_bigint(capsys):
    table = pa.table({
        'id': pa.array([9007199254740993, None], type=pa.int64()),
        'name': ['时间戳', None],
    })

    cli_sql._print_table(table, 'json')

    output = capsys.readouterr().out
    assert '时间戳' in output
    assert json.loads(output) == [
        {'id': 9007199254740993, 'name': '时间戳'},
        {'id': None, 'name': None},
    ]


def test_json_minimum_timestamp_in_containers(capsys):
    timestamp_type = pa.timestamp('ns', tz='UTC')
    minimum = -(2 ** 63)
    timestamps = pa.array([minimum, None], type=timestamp_type)
    table = pa.table({
        'struct': pa.StructArray.from_arrays([timestamps], ['ts']),
        'list': pa.array([[minimum, None], None], type=pa.list_(timestamp_type)),
        'map': pa.array([[('ts', minimum), ('null', None)], None],
                        type=pa.map_(pa.string(), timestamp_type)),
        'dictionary': pa.DictionaryArray.from_arrays(pa.array([0, None]), timestamps.slice(0, 1)),
    })

    cli_sql._print_table(table, 'json')

    expected = '1677-09-21T00:12:43.145224192+00:00'
    assert json.loads(capsys.readouterr().out) == [
        {'struct': {'ts': expected}, 'list': [expected, None],
         'map': [['ts', expected], ['null', None]], 'dictionary': expected},
        {'struct': {'ts': None}, 'list': None, 'map': None, 'dictionary': None},
    ]


def test_table_timestamp_output(capsys):
    table = pa.table({'ts': [datetime(2026, 11, 11, 16, 13, 47)]})

    cli_sql._print_table(table, 'table', elapsed=0.25)

    assert [line.strip() for line in capsys.readouterr().out.splitlines()] == [
        'ts', '2026-11-11 16:13:47', '(1 row in 0.25s)',
    ]
