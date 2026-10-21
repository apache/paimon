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

"""Public format writes preserve behavior while backend I/O stays in execution."""

import inspect
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.format import Format, FormatTable, FormatTableCommit, FormatTableWrite


def _table(tmp_path, options=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    schema = Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), ('pt', pa.string())]),
        partition_keys=['pt'], options=options or {})
    catalog.create_table('default.data', schema, False)
    table = catalog.get_table('default.data')
    return FormatTable(table.file_io, table.identifier, table.table_schema,
                       str(tmp_path / 'format'), Format.PARQUET)


def _rows(paths):
    return sorted(
        row['id']
        for path in paths
        for row in pq.ParquetFile(path).read().to_pylist())


def test_public_format_import_does_not_import_python_execution():
    code = """
import sys
from pypaimon.table.format import FormatTableWrite, FormatTableCommit
assert FormatTableWrite.__module__ == 'pypaimon.table.format.format_table_write'
assert FormatTableCommit.__module__ == 'pypaimon.table.format.format_table_commit'
for name in ('format_table_write', 'format_table_commit'):
    assert 'pypaimon.execution.python.write.' + name not in sys.modules
"""
    subprocess.run([sys.executable, '-c', code], check=True)


def test_format_write_public_types_and_partitioned_rolling(tmp_path):
    table = _table(tmp_path, {'target-file-row-num': '2'})
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    commit = builder.new_commit()
    assert type(writer) is FormatTableWrite
    assert type(commit) is FormatTableCommit
    assert writer.table is table
    assert commit.table is table

    writer.write_arrow(pa.table({'id': [1, 2, 3, 4, 5], 'pt': ['a', 'a', 'b', 'b', 'b']}))
    messages = writer.prepare_commit()
    paths = messages[0].written_paths
    assert sorted(pq.ParquetFile(path).metadata.num_rows for path in paths) == [1, 2, 2]
    assert _rows(paths) == [1, 2, 3, 4, 5]
    assert writer.prepare_commit() == messages
    commit.commit(messages)
    with pytest.raises(RuntimeError, match='only one commit'):
        commit.commit(messages)
    writer.close()
    commit.close()


def test_format_overwrite_and_explicit_abort_keep_other_files(tmp_path):
    table = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    writer.write_pandas(pd.DataFrame({'id': [1, 2], 'pt': ['a', 'b']}))
    old_paths = writer.prepare_commit()[0].written_paths
    writer.close()
    partition_a = Path(old_paths[0]).parent
    marker = partition_a / '_retained_metadata'
    marker.write_text('keep')

    builder = table.new_batch_write_builder().overwrite({'pt': 'a'})
    overwrite = builder.new_write()
    overwrite.write_arrow_batch(pa.record_batch([[3], ['a']], names=['id', 'pt']))
    overwrite.write_arrow_batch(pa.record_batch([[4], ['a']], names=['id', 'pt']))
    messages = overwrite.prepare_commit()
    assert marker.read_text() == 'keep'
    assert not Path(old_paths[0]).exists()
    assert Path(old_paths[1]).exists()
    assert _rows(messages[0].written_paths) == [3, 4]
    overwrite.close()

    commit = builder.new_commit()
    commit.abort(messages)
    assert all(not Path(path).exists() for path in messages[0].written_paths)
    assert marker.read_text() == 'keep'
    assert Path(old_paths[1]).exists()
    commit.close()


@pytest.mark.parametrize('value', ['0', '-1', str(2 ** 63)])
def test_format_writer_retains_eager_row_limit_validation(tmp_path, value):
    table = _table(tmp_path)
    table.options()['target-file-row-num'] = value
    with pytest.raises(ValueError, match='target-file-row-num'):
        FormatTableWrite(table)
    assert not Path(table.location()).exists()


def test_format_writer_checks_parquet_options_at_construction(tmp_path):
    table = _table(tmp_path, {'parquet.write-page-index.enabled': 'true'})
    with patch.object(pa, '__version__', '12.0.1'):
        with pytest.raises(ValueError, match='PyArrow >= 13'):
            FormatTableWrite(table, overwrite=True)
    assert not Path(table.location()).exists()


def test_format_public_method_parameter_defaults_are_unchanged():
    parameters = inspect.signature(FormatTableWrite.__init__).parameters
    assert list(parameters) == ['self', 'table', 'overwrite', 'static_partitions']
    assert parameters['table'].default is inspect.Parameter.empty
    assert parameters['overwrite'].default is False
    assert parameters['static_partitions'].default is None
    assert list(inspect.signature(FormatTableCommit.commit).parameters) == ['self', 'commit_messages']
    assert list(inspect.signature(FormatTableCommit.abort).parameters) == ['self', 'commit_messages']


@pytest.mark.parametrize('input_method', ['write_arrow', 'write_pandas'])
@pytest.mark.parametrize('override', ['subclass', 'instance'])
def test_format_batch_write_public_overrides_are_honored(tmp_path, input_method, override):
    table = _table(tmp_path)
    batches = []

    class ObservedWriter(FormatTableWrite):
        def write_arrow_batch(self, data):
            batches.append(data)
            return super().write_arrow_batch(data)

    if override == 'subclass':
        writer = ObservedWriter(table)
    else:
        writer = FormatTableWrite(table)
        original = writer.write_arrow_batch

        def observed(data):
            batches.append(data)
            return original(data)

        writer.write_arrow_batch = observed
    data = pa.table({'id': [1, 2], 'pt': ['a', 'b']})
    value = data if input_method == 'write_arrow' else data.to_pandas()
    getattr(writer, input_method)(value)
    assert len(batches) == 1
    assert batches[0].to_pydict() == data.to_pydict()
    assert _rows(writer.prepare_commit()[0].written_paths) == [1, 2]
    writer.close()
