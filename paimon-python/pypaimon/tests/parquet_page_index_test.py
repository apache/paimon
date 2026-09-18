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

import hashlib
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

from pypaimon.common.options import Options
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader import format_pyarrow_reader as reader_module
from pypaimon.read.reader import parquet_page_index_reader as page_module
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.tests.parquet_metadata_cache_test import _CountingLocalFileSystem


pytestmark = pytest.mark.skipif(
    int(pa.__version__.split('.')[0]) < 13,
    reason='Writing page indexes requires PyArrow 13 or later')
N = 16384
RUNS = [(0, 2), (125, 132), (4500, 4540), (N - 2, N - 1)]
FIELDS = [DataField(0, 'id', AtomicType('BIGINT')),
          DataField(1, 'payload', AtomicType('STRING'))]
PAGE_INDEX_OPTIONS = CoreOptions(Options({'read.parquet.page-index.enabled': 'true'}))


@pytest.fixture
def fixture(tmp_path):
    path = str(tmp_path / 'indexed.parquet')
    table = pa.table({'id': range(N), 'payload': [
        hashlib.sha256(str(i).encode()).hexdigest() for i in range(N)]})
    pq.write_table(table, path, write_page_index=True,
                   data_page_size=4096, write_batch_size=128,
                   dictionary_pagesize_limit=16 * 1024,
                   row_group_size=N // 2)
    counter = _CountingLocalFileSystem(skip_instance_cache=True)
    file_io = LocalFileIO(str(tmp_path), Options({}))
    file_io.filesystem = pafs.PyFileSystem(pafs.FSSpecHandler(counter))
    reader_module._reset_file_format_dataset_cache()
    yield path, table, file_io, counter
    reader_module._reset_file_format_dataset_cache()


def _expected(table, runs):
    return table.take(pa.array(sorted({
        i for lower, upper in runs for i in range(max(0, lower), min(len(table), upper + 1))
    }), type=pa.int64()))


def _read(fixture, baseline=False, fields=FIELDS, **kwargs):
    path, table, file_io, counter = fixture
    counter.reset_counts()
    kwargs.setdefault('row_ranges', RUNS)
    kwargs.setdefault('options', None if baseline else PAGE_INDEX_OPTIONS)
    reader = reader_module.FormatPyArrowReader(
        file_io, 'parquet', path, fields, None, batch_size=71, **kwargs)
    try:
        batches = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            assert batch.num_rows <= 71
            batches.append(batch)
        result = pa.Table.from_batches(batches) if batches else None
        return result, list(counter.reads)
    finally:
        reader.close()


@pytest.mark.parametrize('cache_size', ['0 b', '50 mb'])
def test_sparse_reads_skip_bytes_and_preserve_results(fixture, cache_size):
    path, table, file_io, counter = fixture
    file_io.properties = Options({'file-format.metadata-cache.max-size': cache_size})
    runs = [(4500, 4540)]
    baseline, baseline_reads = _read(fixture, baseline=True, row_ranges=runs)
    reader_module._reset_file_format_dataset_cache()
    for _ in range(2):  # Cold and warm metadata cache.
        result, reads = _read(fixture, row_ranges=runs)
        assert result.equals(baseline)
        assert result.equals(_expected(table, runs))
        assert sum(size for _, size in reads) < sum(size for _, size in baseline_reads) / 2


def test_ranges_projection_missing_fields_and_fallback_in_same_file(fixture):
    # First row group is read in full; the second uses selected pages.
    runs = [(-10, N // 2 - 1), (N - 2, N + 10)]
    fields = [FIELDS[1], DataField(2, 'added', AtomicType('INT')), FIELDS[0]]
    baseline, _ = _read(fixture, baseline=True, fields=fields, row_ranges=runs)
    result, _ = _read(fixture, fields=fields, row_ranges=runs)
    assert result.equals(baseline)
    assert result.column('id').to_pylist() == _expected(fixture[1], runs)['id'].to_pylist()
    assert result.column('added').null_count == len(result)


@pytest.mark.parametrize('mode', ['full', 'no_index', 'nested', 'budget', 'cache', 'scattered'])
def test_unsupported_or_expensive_reads_fall_back(fixture, mode):
    path, table, file_io, counter = fixture
    kwargs = {}
    if mode == 'full':
        kwargs['row_ranges'] = [(0, N - 1)]
    elif mode == 'scattered':
        kwargs['row_ranges'] = [(5, 6), (4500, 4540)]
    elif mode == 'no_index':
        pq.write_table(table, path)
    elif mode == 'nested':
        pq.write_table(table.append_column('nested', pa.array([[i] for i in range(N)])),
                       path, write_page_index=True)
    elif mode == 'cache':
        kwargs['row_group_cache'] = reader_module._DecodedRowGroupCache(4 * 1024 * 1024)
    with patch.object(page_module, '_MAX_PAGE_BYTES', 1 if mode == 'budget' else 32 * 1024 * 1024), \
            patch.object(page_module.ParquetPageIndexReader, '_batches',
                         side_effect=AssertionError('must fall back')):
        result, _ = _read(fixture, **kwargs)
    assert result.equals(_expected(table, kwargs.get('row_ranges', RUNS)))


@pytest.mark.parametrize('version', ['1.0', '2.0'])
@pytest.mark.parametrize('dictionary', [False, True])
@pytest.mark.parametrize('compression', ['NONE', 'snappy', 'zstd', 'gzip'])
def test_page_encodings_nulls_and_different_column_boundaries(tmp_path, version, dictionary, compression):
    path = str(tmp_path / 'types.parquet')
    count = 8192
    table = pa.table({
        'text': pa.array([None if i % 3 else 'v-%d' % i for i in range(count)]),
        'all_null': pa.nulls(count, type=pa.int32()),
        'flag': pa.array([i % 3 == 0 for i in range(count)]),
        'decimal': pa.array([Decimal(i).scaleb(-2) for i in range(count)], pa.decimal128(20, 2)),
        'timestamp': pa.array(range(count), pa.timestamp('ns', 'Asia/Shanghai')),
        'large': pa.array([bytes([i % 256]) * (i % 101) for i in range(count)], pa.large_binary()),
    })
    pq.write_table(table, path, data_page_version=version, use_dictionary=dictionary,
                   compression=compression, write_page_index=True,
                   data_page_size=1024, write_batch_size=64, dictionary_pagesize_limit=1024)
    runs = [(62, 1050)]
    with pa.OSFile(path, 'rb') as source:
        parquet = pq.ParquetFile(source)
        reader = page_module.ParquetPageIndexReader.create(
            source, parquet, table.column_names, [0], 37)
        batches = reader.read_row_group(0, runs)
        assert batches is not None
        assert pa.Table.from_batches(list(batches)).equals(_expected(table, runs))


def test_sparse_row_indices_are_normalized(fixture):
    rows = [N - 1, 1000, 125, 125, 126, -1, N + 1]
    result, _ = _read(fixture, row_ranges=None, row_indices=rows)
    assert result.equals(_expected(fixture[1], [(i, i) for i in rows]))
    assert _read(fixture, row_ranges=[])[0] is None
    assert _read(fixture, row_ranges=[(N, N + 10)])[0] is None


def test_concurrent_readers_and_early_close(fixture):
    path, table, file_io, _ = fixture

    def read(_):
        reader = reader_module.FormatPyArrowReader(
            file_io, 'parquet', path, FIELDS, None, row_ranges=[(0, 2)], batch_size=71,
            options=PAGE_INDEX_OPTIONS)
        source = reader._parquet_source
        try:
            return reader.read_arrow_batch().column(0).to_pylist()
        finally:
            reader.close()
            assert source.closed
            reader.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(read, range(8))) == [[0, 1, 2]] * 8


def test_corrupt_offset_index_is_not_silently_ignored(fixture):
    path = fixture[0]
    with pa.OSFile(path, 'rb') as source:
        parquet = pq.ParquetFile(source)
        reader = page_module.ParquetPageIndexReader.create(source, parquet, ['id'], [0], 71)
        chunk = page_module._get(page_module._get(reader.footer, 4)[1][0], 1)[1][0]
        offset, size = page_module._get(chunk, 4), page_module._get(chunk, 5)
        raw = source.read_at(size, offset)
    index = page_module._Compact(raw).value(12)
    page_module._get(index, 1)[1][0][3] = (6, 1)  # First page must start at row zero.
    modified = page_module._encode(12, index)
    assert len(modified) == size
    with open(path, 'r+b') as output:
        output.seek(offset)
        output.write(modified)
    with pytest.raises(ValueError, match='OffsetIndex row boundaries'):
        _read(fixture, row_ranges=[(0, 2)])


def test_index_io_errors_propagate_and_release_source(fixture):
    path, _, file_io, _ = fixture
    reader = reader_module.FormatPyArrowReader(
        file_io, 'parquet', path, FIELDS, None, row_ranges=[(0, 2)], options=PAGE_INDEX_OPTIONS)
    source = reader._parquet_source
    try:
        with patch.object(page_module, '_read_exact', side_effect=OSError('injected I/O failure')):
            with pytest.raises(OSError, match='injected I/O failure'):
                reader.read_arrow_batch()
    finally:
        reader.close()
    assert source.closed


@pytest.mark.parametrize('encoding,kind', [
    ('DELTA_BINARY_PACKED', pa.int64()),
    ('DELTA_LENGTH_BYTE_ARRAY', pa.string()),
    ('DELTA_BYTE_ARRAY', pa.string()),
    ('BYTE_STREAM_SPLIT', pa.float64()),
])
def test_non_dictionary_encodings(tmp_path, encoding, kind):
    path = str(tmp_path / 'encoding.parquet')
    values = ['common-prefix-%05d' % i for i in range(N)] if pa.types.is_string(kind) else range(N)
    if pa.types.is_int64(kind):
        values = [i ** 3 for i in range(N)]
    table = pa.table({'value': pa.array(values, type=kind)})
    pq.write_table(table, path, column_encoding=encoding, use_dictionary=False,
                   write_page_index=True, data_page_size=1024, write_batch_size=64)
    with pa.OSFile(path, 'rb') as source:
        reader = page_module.ParquetPageIndexReader.create(
            source, pq.ParquetFile(source), ['value'], [0], 71)
        actual = pa.Table.from_batches(list(reader.read_row_group(0, [(8000, 8100)])))
        assert actual.equals(table.slice(8000, 101))


def test_page_header_row_count_must_agree_with_index(fixture):
    path = fixture[0]
    with pa.OSFile(path, 'rb') as source:
        reader = page_module.ParquetPageIndexReader.create(source, pq.ParquetFile(source), ['id'], [0], 71)
        chunk = page_module._get(page_module._get(reader.footer, 4)[1][0], 1)[1][0]
        offset, size = page_module._get(chunk, 4), page_module._get(chunk, 5)
        index = page_module._Compact(source.read_at(size, offset)).value(12)
    second = page_module._get(index, 1)[1][1]
    second[3] = (6, page_module._get(second, 3) + 1)
    modified = page_module._encode(12, index)
    assert len(modified) == size
    with open(path, 'r+b') as output:
        output.seek(offset)
        output.write(modified)
    with pytest.raises(ValueError, match='page rows disagree'):
        _read(fixture, row_ranges=[(0, 2)])


def test_scattered_ranges_do_not_even_read_indexes(fixture):
    with patch.object(page_module, '_read_exact', side_effect=AssertionError('index read')):
        actual, _ = _read(fixture, row_ranges=[(5, 6), (4500, 4501)])
    assert actual.equals(_expected(fixture[1], [(5, 6), (4500, 4501)]))


@pytest.mark.parametrize('missing', [False, True])
def test_missing_or_corrupt_parquet_still_raises(fixture, missing):
    path = fixture[0]
    if missing:
        import os
        os.remove(path)
    else:
        with open(path, 'wb') as output:
            output.write(b'not a parquet file')
    with pytest.raises((OSError, pa.ArrowInvalid)):
        _read(fixture, row_ranges=[(0, 2)])


@pytest.mark.parametrize('values,enabled', [
    (None, False), ({}, False),
    ({'read.parquet.page-index.enabled': 'false'}, False),
    ({'read.parquet.page-index.enabled': False}, False),
    ({'read.parquet.page-index.enabled': 'true'}, True),
    ({'read.parquet.page-index.enabled': True}, True),
])
def test_page_index_switch_bypasses_metadata_processing_when_disabled(fixture, values, enabled):
    options = CoreOptions(Options(values)) if values is not None else None
    with patch.object(page_module.ParquetPageIndexReader, 'create',
                      wraps=page_module.ParquetPageIndexReader.create) as create:
        actual, _ = _read(fixture, options=options, row_ranges=[(4500, 4540)])
    assert create.called == enabled
    assert actual.equals(_expected(fixture[1], [(4500, 4540)]))


def test_table_copy_can_enable_and_disable_page_index_reads(tmp_path):
    from pypaimon import CatalogFactory, Schema

    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', False)
    data = pa.table({'id': range(N)})
    catalog.create_table('default.indexed', Schema.from_pyarrow_schema(
        data.schema, options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}), False)
    table = catalog.get_table('default.indexed')
    write_parquet = table.file_io.write_parquet

    def write_indexed(path, arrow, **kwargs):
        kwargs.update(write_page_index=True, data_page_size=1024, write_batch_size=64,
                      use_dictionary=False)
        return write_parquet(path, arrow, **kwargs)

    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        with patch.object(table.file_io, 'write_parquet', side_effect=write_indexed):
            writer.write_arrow(data)
            commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    table = catalog.get_table('default.indexed')
    for value in ('true', 'false', 'true'):
        copied = table.copy({'read.parquet.page-index.enabled': value})
        builder = copied.new_read_builder().with_projection(['id', '_ROW_ID'])
        builder.with_filter(builder.new_predicate_builder().between('_ROW_ID', 4500, 4540))
        with patch.object(page_module.ParquetPageIndexReader, 'create',
                          wraps=page_module.ParquetPageIndexReader.create) as create:
            actual = builder.new_read().to_arrow(builder.new_scan().plan().splits())
        assert create.called == (value == 'true')
        assert actual.to_pydict() == {'id': list(range(4500, 4541)), '_ROW_ID': list(range(4500, 4541))}
    assert not table.options.read_parquet_page_index_enabled()
    assert not catalog.get_table('default.indexed').options.read_parquet_page_index_enabled()
