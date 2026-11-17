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

"""Schema cast validation describes capabilities without loading execution."""

from pathlib import Path
import subprocess
import sys
import textwrap

import pyarrow as pa
import pytest

from pypaimon.schema.data_type_casts import can_execute_cast
from pypaimon.schema.data_types import AtomicType, PyarrowFieldParser


_LEAF_TYPES = [
    'CHAR(3)', 'VARCHAR(5)', 'BOOLEAN', 'BINARY(3)', 'VARBINARY(5)',
    'DECIMAL(4,2)', 'DECIMAL(38,18)', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
    'FLOAT', 'DOUBLE', 'DATE', 'TIME', 'TIME(3)',
    'TIMESTAMP(0)', 'TIMESTAMP(3)', 'TIMESTAMP(6)', 'TIMESTAMP(9)',
    'TIMESTAMP_LTZ(0)', 'TIMESTAMP_LTZ(3)', 'TIMESTAMP_LTZ(6)', 'TIMESTAMP_LTZ(9)',
    'TIMESTAMP(6) WITH LOCAL TIME ZONE', 'BLOB', 'VARIANT',
]


@pytest.mark.parametrize('source', _LEAF_TYPES)
def test_declared_cast_capabilities_match_existing_read_materialization(source):
    from pypaimon.execution.python.casting.schema_evolution import cast_array_for_schema_evolution

    source_type = AtomicType(source)
    values = pa.nulls(1, type=PyarrowFieldParser.from_paimon_type(source_type))
    for target in _LEAF_TYPES:
        target_type = AtomicType(target)
        try:
            cast_array_for_schema_evolution(
                values, PyarrowFieldParser.from_paimon_type(target_type), source_type, target_type)
            executable = True
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid, pa.ArrowTypeError,
                ValueError, NotImplementedError):
            executable = False
        # TIME precision is not represented by its Arrow storage unit, so
        # changing it is rejected independently of Arrow's identity kernel.
        if (source, target) in (('TIME', 'TIME(3)'), ('TIME(3)', 'TIME')):
            executable = False
        assert can_execute_cast(source_type, target_type) == executable, '{} -> {}'.format(source, target)


@pytest.mark.skipif(sys.version_info < (3, 7), reason='Python 3.6 retains eager package exports')
def test_schema_type_changes_do_not_import_or_execute_either_backend():
    script = r"""
import importlib.abc
import sys
from unittest.mock import patch

class RejectExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pypaimon.execution' or fullname.startswith('pypaimon.execution.'):
            raise AssertionError('Schema validation imported execution: ' + fullname)

sys.meta_path.insert(0, RejectExecution())
from pypaimon.schema.data_type_casts import can_execute_cast
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.schema.table_schema import TableSchema

manager = SchemaManager(None, '/unused-schema-capabilities')
with patch('pyarrow.nulls', side_effect=AssertionError('Schema validation executed a cast probe')):
    for source, target, supported in [
        ('INT', 'BIGINT', True), ('INT', 'TIMESTAMP(3)', True),
        ('BIGINT', 'TIMESTAMP_LTZ(6)', True), ('INT', 'DECIMAL(4,2)', True),
        ('TIMESTAMP_LTZ(9)', 'TIMESTAMP(3)', True),
        ('TIMESTAMP(3)', 'DECIMAL(10,2)', False), ('BOOLEAN', 'DECIMAL(4,2)', False),
        ('TIME', 'TIMESTAMP(6)', False), ('TIME(3)', 'TIME(1)', False),
    ]:
        source_type, target_type = AtomicType(source), AtomicType(target)
        assert can_execute_cast(source_type, target_type) == supported, (source, target)
        schema = TableSchema(fields=[DataField(0, 'v', source_type)])
        change = SchemaChange.update_column_type('v', target_type)
        if supported:
            changed = manager._generate_table_schema(schema, [change])
            assert changed.fields[0].id == 0 and changed.fields[0].type == target_type
            assert schema.fields[0].type == source_type
        else:
            try:
                manager._generate_table_schema(schema, [change])
            except ValueError as error:
                assert 'no executable cast' in str(error)
            else:
                raise AssertionError('Unsupported schema cast was accepted: ' + str((source, target)))
assert not any(name == 'pypaimon.execution' or name.startswith('pypaimon.execution.')
               for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
