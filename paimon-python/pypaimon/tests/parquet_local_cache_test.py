# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from unittest.mock import patch

import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq
import pytest

from pypaimon.common.options import Options
from pypaimon.filesystem.caching_file_io import CachingFileIO
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader.format_pyarrow_reader import (
    FormatPyArrowReader, _reset_file_format_dataset_cache,
)
from pypaimon.schema.data_types import AtomicType, DataField


def _read(file_io, path, file_format, rows, file_size=None):
    reader = FormatPyArrowReader(
        file_io, file_format, path, [DataField(0, 'v', AtomicType('INT'))],
        None, row_indices=rows, batch_size=2, file_size=file_size)
    values = []
    try:
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            values.extend(batch.column(0).to_pylist())
    finally:
        reader.close()
    return values


@pytest.mark.parametrize('disk', [False, True])
@pytest.mark.parametrize('metadata_cache', ['0 b', '1 mb'])
@pytest.mark.parametrize('file_format,rows', [('parquet', None), ('parquet', [1]), ('orc', None)])
def test_format_reader_uses_block_cache(tmp_path, disk, metadata_cache, file_format, rows):
    path = tmp_path / ('data.' + file_format)
    table = pa.table({'v': pa.array([1, 2, 3], type=pa.int32())})
    if file_format == 'parquet':
        pq.write_table(table, path, row_group_size=1)
    else:
        orc.write_table(table, path)
    opts = Options({'local-cache.enabled': 'true', 'local-cache.block-size': '64 b',
                    'local-cache.whitelist': 'data',
                    'local-cache.exclude-extensions': 'blob',
                    'file-format.metadata-cache.max-size': metadata_cache})
    if disk:
        opts.data['local-cache.dir'] = str(tmp_path / 'cache')
    delegate = LocalFileIO(catalog_options=opts)
    cache = CachingFileIO.create_cache_manager(opts)
    file_io = CachingFileIO.wrap_with_caching_if_needed(delegate, opts, cache)
    expected = [2] if rows is not None else [1, 2, 3]
    _reset_file_format_dataset_cache()
    try:
        opened = []
        original_open = delegate.new_input_stream

        def open_source(location):
            stream = original_open(location)
            opened.append(stream)
            return stream

        with patch.object(delegate, 'new_input_stream', side_effect=open_source) as opens:
            assert _read(file_io, path.as_uri(), file_format, rows) == expected
            assert opens.call_count > 0
        assert all(stream.closed for stream in opened)
        if file_format == 'parquet':
            path.unlink()
        # Drop the parsed-footer cache, proving that raw cached blocks are sufficient.
        _reset_file_format_dataset_cache()
        with patch.object(delegate, 'new_input_stream', side_effect=AssertionError('source read')):
            with patch.object(delegate, 'get_file_size', side_effect=AssertionError('source stat')):
                assert _read(file_io, path.as_uri(), file_format, rows) == expected
    finally:
        _reset_file_format_dataset_cache()


def test_known_parquet_size_avoids_source_stat(tmp_path):
    path = tmp_path / 'data.parquet'
    pq.write_table(pa.table({'v': pa.array([1, 2, 3], type=pa.int32())}), path)
    opts = Options({'local-cache.enabled': 'true', 'local-cache.whitelist': 'data',
                    'local-cache.exclude-extensions': 'blob',
                    'file-format.metadata-cache.max-size': '0 b'})
    delegate = LocalFileIO(catalog_options=opts)
    file_io = CachingFileIO.wrap_with_caching_if_needed(
        delegate, opts, CachingFileIO.create_cache_manager(opts))
    with patch.object(delegate, 'get_file_size', side_effect=AssertionError('source stat')):
        assert _read(file_io, str(path), 'parquet', None, path.stat().st_size) == [1, 2, 3]
