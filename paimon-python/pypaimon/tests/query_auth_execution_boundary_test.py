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

"""Authorization metadata validation does not select a read executor."""

import subprocess
import sys


def test_authorization_fields_validate_without_loading_either_execution_backend():
    source = r'''
import importlib.abc
import json
import sys

class RejectExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(('pypaimon.execution', 'pypaimon_rust')):
            raise AssertionError('Authorization metadata loaded an executor: ' + fullname)

sys.meta_path.insert(0, RejectExecution())

from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.common.predicate_json_parser import extract_referenced_fields
from pypaimon.schema.data_types import AtomicType, DataField

fields = [DataField(0, 'event_time', AtomicType('TIMESTAMP(3)')),
          DataField(1, 'secret', AtomicType('STRING'))]

def year_filter(name):
    return json.dumps({
        'kind': 'LEAF', 'function': 'EQUAL', 'literals': [2026],
        'transform': {'name': 'YEAR', 'fieldRef': {'name': name, 'type': 'TIMESTAMP(3)'}},
    })

rule = year_filter('event_time')
mask = json.dumps({'name': 'UPPER', 'inputs': [{'name': 'secret', 'type': 'STRING'}]})
result = TableQueryAuthResult([rule], {'secret': mask})
assert extract_referenced_fields(rule) == {'event_time'}
assert result.filter_field_names() == {'event_time'}
assert result.rule_field_names() == {'event_time', 'secret'}
result.validate_against_schema(fields)
result.validate_readable_without_rename(fields, fields)
result.validate_read_type(fields, fields)
assert result.get_extra_fields([fields[1]], fields) == [fields[0]]
assert TableQueryAuthResult(None, None).extract_row_filter() is None
assert TableQueryAuthResult([], None).extract_row_filter() is None

try:
    TableQueryAuthResult([year_filter('missing')], None).validate_against_schema(fields)
except ValueError as error:
    assert "references column 'missing'" in str(error)
else:
    raise AssertionError('A stale date-transform field was accepted')

assert not any(name.startswith(('pypaimon.execution', 'pypaimon_rust'))
               for name in sys.modules)
'''
    result = subprocess.run([sys.executable, '-c', source], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
