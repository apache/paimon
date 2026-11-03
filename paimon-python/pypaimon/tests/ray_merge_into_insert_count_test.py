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

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.manifest.schema.data_file_meta import DataFileMeta


@pytest.mark.parametrize('batch_sizes', [(5,), (2, 3)],
                         ids=['single-write', 'multiple-writes'])
@pytest.mark.parametrize('with_blob', [False, True], ids=['no-blob', 'blob'])
@pytest.mark.parametrize('vector_format', [None, 'parquet'],
                         ids=['inline', 'parquet'])
def test_merge_into_counts_logical_inserted_rows(
        tmp_path, batch_sizes, with_blob, vector_format):
    pytest.importorskip('ray')

    from pypaimon.ray.data_evolution_merge_into import _execute_and_commit

    options = {
        'bucket': '-1',
        'data-evolution.enabled': 'true',
        'row-tracking.enabled': 'true',
        'file.format': 'parquet',
        'vector-field': 'embedding',
        'field.embedding.vector-dim': '3',
        'write.native.enabled': 'false',
        'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'false',
        'read.native.enabled': 'false',
    }
    if vector_format is not None:
        options['vector.file.format'] = vector_format
    data = pa.table({
        'id': pa.array(list(range(5)), type=pa.int64()),
        'embedding': pa.array(
            [[float(index), 1.0, 2.0] for index in range(5)],
            type=pa.list_(pa.float32(), 3)),
    })
    if with_blob:
        options['blob-field'] = 'payload'
        data = data.append_column('payload', pa.array(
            [('payload-%d' % index).encode() for index in range(5)],
            type=pa.large_binary()))

    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', False)
    catalog.create_table('default.vectors', Schema.from_pyarrow_schema(
        data.schema, options=options), False)
    table = catalog.get_table('default.vectors')
    writers = []
    messages = []
    try:
        offset = 0
        for batch_size in batch_sizes:
            writer = table.new_batch_write_builder().new_write()
            writers.append(writer)
            writer.write_arrow(data.slice(offset, batch_size))
            messages.extend(writer.prepare_commit())
            offset += batch_size

        files = [file for message in messages for file in message.new_files]
        normal_files = [file for file in files
                        if not DataFileMeta.is_blob_file(file.file_name)
                        and not DataFileMeta.is_vector_file(file.file_name)]
        vector_files = [file for file in files
                        if DataFileMeta.is_vector_file(file.file_name)]
        blob_files = [file for file in files
                      if DataFileMeta.is_blob_file(file.file_name)]
        assert len(messages) == len(batch_sizes)
        assert sum(file.row_count for file in normal_files) == data.num_rows
        assert sum(file.row_count for file in vector_files) == (
            data.num_rows if vector_format is not None else 0)
        assert sum(file.row_count for file in blob_files) == (
            data.num_rows if with_blob else 0)

        with patch(
                'pypaimon.ray.data_evolution_merge_into.'
                'distributed_write_collect_msgs', return_value=messages):
            result = _execute_and_commit(
                table=table, update_ds=None, delete_ds=None,
                insert_ds=object(), update_cols_union=[], base_snapshot=None,
                num_partitions=1, ray_remote_args=None, concurrency=None)

        read_builder = table.new_read_builder()
        actual = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits()).sort_by('id')
        assert actual.to_pydict() == data.to_pydict()
        assert result == {
            'num_matched': 0,
            'num_inserted': data.num_rows,
            'num_unchanged': 0,
        }
    finally:
        for writer in writers:
            writer.close()
