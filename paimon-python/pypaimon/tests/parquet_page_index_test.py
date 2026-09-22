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
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser
from pypaimon.tests.parquet_metadata_cache_test import _CountingLocalFileSystem


pytestmark = pytest.mark.skipif(
    int(pa.__version__.split('.')[0]) < 13,
    reason='Writing page indexes requires PyArrow 13 or later')
N = 16384
RUNS = [(0, 2), (125, 132), (4500, 4540), (N - 2, N - 1)]
FIELDS = [DataField(0, 'id', AtomicType('BIGINT')),
          DataField(1, 'payload', AtomicType('STRING'))]
PAGE_INDEX_OPTIONS = CoreOptions(Options({'parquet.filter.columnindex.enabled': 'true'}))


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


@pytest.mark.parametrize(
    'mode', ['full', 'no_index', 'budget', 'location_budget', 'footer_bytes',
             'footer_chunks', 'footer_items', 'cache', 'scattered'])
def test_unsupported_or_expensive_reads_fall_back(fixture, mode):
    path, table, file_io, counter = fixture
    kwargs = {}
    if mode == 'full':
        kwargs['row_ranges'] = [(0, N - 1)]
    elif mode == 'scattered':
        kwargs['row_ranges'] = [(5, 6), (4500, 4540)]
    elif mode == 'no_index':
        pq.write_table(table, path)
    elif mode == 'cache':
        kwargs['row_group_cache'] = reader_module._DecodedRowGroupCache(4 * 1024 * 1024)
    with patch.object(page_module, '_MAX_PAGE_BYTES', 1 if mode == 'budget' else 32 * 1024 * 1024), \
            patch.object(page_module, '_MAX_PAGE_LOCATIONS',
                         1 if mode == 'location_budget' else 128 * 1024), \
            patch.object(page_module, '_MAX_FOOTER_BYTES',
                         1 if mode == 'footer_bytes' else 1024 * 1024), \
            patch.object(page_module, '_MAX_FOOTER_COLUMN_CHUNKS',
                         1 if mode == 'footer_chunks' else 1024), \
            patch.object(page_module, '_MAX_FOOTER_ITEMS',
                         1 if mode == 'footer_items' else 64 * 1024), \
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


def test_offset_index_page_locations_are_bounded_before_decoding():
    count = page_module._MAX_PAGE_LOCATIONS + 1
    encoded = b'\x19\xfc' + page_module._unsigned(count) + b'\x00' * count + b'\x00'
    with pytest.raises(page_module._PageIndexBudgetExceeded,
                       match='page-location budget'):
        page_module._decode_offset_index(encoded, page_module._MAX_PAGE_LOCATIONS)


def test_offset_index_unknown_struct_fields_are_bounded():
    location = {1: (6, 4), 2: (5, 1), 3: (6, 0)}
    unknown = {field: (1, True) for field in range(1, 33)}
    encoded = page_module._encode(
        12, {1: (9, (12, [location])), 2: (12, unknown)})
    with pytest.raises(page_module._PageIndexBudgetExceeded,
                       match='object budget'):
        page_module._decode_offset_index(encoded, 1)


def test_page_header_unknown_struct_fields_are_bounded():
    unknown = {field: (1, True) for field in range(1, 4097)}
    encoded = page_module._encode(
        12, {1: (5, 0), 2: (5, 1), 3: (5, 1), 9: (12, unknown)})
    with pytest.raises(page_module._PageIndexBudgetExceeded,
                       match='object budget'):
        page_module._decode_page_header(encoded)


def test_page_header_decoder_skips_unknown_fields_and_rejects_missing_fields():
    encoded = page_module._encode(
        12, {1: (5, 0), 2: (5, 11), 3: (5, 7),
             5: (12, {1: (5, 3), 9: (9, (5, [1, 2]))}),
             9: (12, {1: (1, True)})})
    header, size = page_module._decode_page_header(encoded)
    assert size == len(encoded)
    assert page_module._get(header, 1) == 0
    assert page_module._get(header, 2) == 11
    assert page_module._get(header, 3) == 7
    assert page_module._get(page_module._get(header, 5), 1) == 3
    with pytest.raises(ValueError, match='Missing Parquet PageHeader field'):
        page_module._decode_page_header(
            page_module._encode(12, {1: (5, 0), 2: (5, 1)}))


def test_page_header_budget_falls_back(fixture):
    runs = [(4500, 4540)]
    baseline, _ = _read(fixture, baseline=True, row_ranges=runs)
    reader_module._reset_file_format_dataset_cache()
    with patch.object(
            page_module, '_decode_page_header',
            side_effect=page_module._PageIndexBudgetExceeded('test budget')):
        result, _ = _read(fixture, row_ranges=runs)
    assert result.equals(baseline)


def test_fragmented_footer_falls_back_before_generic_decoding(fixture):
    path = fixture[0]
    with pa.OSFile(path, 'rb') as source:
        parquet = pq.ParquetFile(source)
        with patch.object(page_module, '_MAX_FOOTER_COLUMN_CHUNKS', 1), \
                patch.object(page_module._Compact, 'value',
                             side_effect=AssertionError('must not decode footer')):
            assert page_module.ParquetPageIndexReader.create(
                source, parquet, ['id'], [0], 71) is None


def test_wide_fallback_coalesces_offset_index_reads(tmp_path):
    path = str(tmp_path / 'wide.parquet')
    columns = ['column_%03d' % i for i in range(200)]
    table = pa.table({name: range(16) for name in columns})
    pq.write_table(table, path, write_page_index=True, use_dictionary=False,
                   data_page_size=1024 * 1024)
    with pa.OSFile(path, 'rb') as source:
        reader = page_module.ParquetPageIndexReader.create(
            source, pq.ParquetFile(source), columns, [0], 71)
        with patch.object(page_module, '_read_exact', wraps=page_module._read_exact) as reads:
            assert reader.read_row_group(0, [(0, 1)]) is None
    assert reads.call_count == 1


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
    runs = [(8000, 8100)]
    with pa.OSFile(path, 'rb') as source:
        page_reader = page_module.ParquetPageIndexReader.create(
            source, pq.ParquetFile(source), ['value'], [0], 71)
        use_pages = page_reader.read_row_group(0, runs) is not None
    fields = PyarrowFieldParser.to_paimon_schema(table.schema)
    reader = reader_module.FormatPyArrowReader(
        LocalFileIO(str(tmp_path), Options({})), 'parquet', path, fields, None,
        row_ranges=runs, batch_size=71, options=PAGE_INDEX_OPTIONS)
    try:
        with patch.object(page_module.ParquetPageIndexReader, '_column_payload',
                          autospec=True,
                          side_effect=page_module.ParquetPageIndexReader._column_payload
                          ) as read_pages:
            batches = []
            while True:
                batch = reader.read_arrow_batch()
                if batch is None:
                    break
                batches.append(batch)
        assert pa.Table.from_batches(batches).equals(table.slice(8000, 101))
        if use_pages:
            assert read_pages.called
    finally:
        reader.close()


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
    (None, False), ({}, True),
    ({'parquet.filter.columnindex.enabled': 'false'}, False),
    ({'parquet.filter.columnindex.enabled': False}, False),
    ({'parquet.filter.columnindex.enabled': 'true'}, True),
    ({'parquet.filter.columnindex.enabled': True}, True),
])
def test_page_index_switch_bypasses_metadata_processing_when_disabled(fixture, values, enabled):
    options = CoreOptions(Options(values)) if values is not None else None
    with patch.object(page_module.ParquetPageIndexReader, 'create',
                      wraps=page_module.ParquetPageIndexReader.create) as create:
        actual, _ = _read(fixture, options=options, row_ranges=[(4500, 4540)])
    assert create.called == enabled
    assert actual.equals(_expected(fixture[1], [(4500, 4540)]))


@pytest.mark.parametrize('nested', [False, True])
@pytest.mark.python_plan
def test_table_option_and_copy_control_page_index_reads(tmp_path, nested):
    from pypaimon import CatalogFactory, Schema

    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', False)
    data = pa.table({'id': range(N)})
    if nested:
        data = data.append_column('record', pa.array([{'value': i} for i in range(N)]))
    catalog.create_table('default.indexed', Schema.from_pyarrow_schema(
        data.schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'parquet.filter.columnindex.enabled': 'true',
        }), False)
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
    for candidate, enabled in (
            (table, True),
            (table.copy({'parquet.filter.columnindex.enabled': 'false'}), False),
            (table.copy({'parquet.filter.columnindex.enabled': 'true'}), True)):
        builder = candidate.new_read_builder().with_projection(['id', '_ROW_ID'])
        builder.with_filter(builder.new_predicate_builder().between('_ROW_ID', 4500, 4540))
        with patch.object(page_module.ParquetPageIndexReader, 'create',
                          wraps=page_module.ParquetPageIndexReader.create) as create:
            actual = builder.new_read().to_arrow(builder.new_scan().plan().splits())
        assert create.called == enabled
        assert actual.to_pydict() == {'id': list(range(4500, 4541)), '_ROW_ID': list(range(4500, 4541))}
    assert table.options.parquet_column_index_enabled()
    assert catalog.get_table('default.indexed').options.parquet_column_index_enabled()


@pytest.fixture
def nested_fixture(fixture):
    path, original, file_io, counter = fixture
    count = len(original)
    child_type = pa.struct([('number', pa.int64()), ('text', pa.string())])
    records = [None if i % 13 == 0 else {
        'number': None if i % 11 == 0 else i,
        'text': None if i % 7 == 0 else hashlib.sha256(str(i).encode()).hexdigest()
    } for i in range(count)]
    table = pa.table({
        'record': pa.array(records, child_type),
        'items': pa.array([None if i % 9 == 0 else
                           [records[i]] * (4097 if i == N // 2 + 1 else i % 5)
                           for i in range(count)], pa.list_(child_type)),
        'mapping': pa.array([None if i % 9 == 0 else
                             [('key-%d' % j, None if j == 1 else list(range(j)))
                              for j in range(i % 4)] for i in range(count)],
                            pa.map_(pa.string(), pa.list_(pa.int32()))),
        'matrix': pa.array([None if i % 9 == 0 else
                            [None, [], [None, i]] * (i % 3) for i in range(count)],
                           pa.list_(pa.list_(pa.int64()))),
        # Place flat columns after multiple nested physical leaves.
        'id': original['id'],
        'payload': original['payload'],
    })
    return path, table, file_io, counter


@pytest.mark.parametrize('version', ['1.0', '2.0'])
@pytest.mark.parametrize('dictionary', [False, True])
@pytest.mark.parametrize('projection', ['flat', 'nested'])
def test_nested_page_reads_preserve_structure_and_skip_bytes(
        nested_fixture, version, dictionary, projection):
    path, table, _, _ = nested_fixture
    pq.write_table(table, path, write_page_index=True, data_page_version=version,
                   use_dictionary=dictionary, dictionary_pagesize_limit=1024,
                   data_page_size=2048, write_batch_size=64, row_group_size=N // 2)
    names = ['payload', 'id'] if projection == 'flat' else list(reversed(table.column_names))
    fields = PyarrowFieldParser.to_paimon_schema(table.select(names).schema)
    # Cross a row-group boundary, including null parents, empty lists/maps,
    # null elements, and multiple leaves with different page boundaries.
    runs = [(N // 2 - 17, N // 2 + 83)]
    baseline, baseline_reads = _read(nested_fixture, baseline=True, fields=fields, row_ranges=runs)
    reader_module._reset_file_format_dataset_cache()
    for _ in range(2):
        with patch.object(page_module.ParquetPageIndexReader, '_column_payload',
                          autospec=True, side_effect=page_module.ParquetPageIndexReader._column_payload
                          ) as read_pages:
            actual, reads = _read(nested_fixture, fields=fields, row_ranges=runs)
        assert read_pages.called
        assert actual.equals(baseline)
        assert actual.equals(_expected(table.select(names), runs))
        assert sum(size for _, size in reads) < sum(size for _, size in baseline_reads)


def test_nested_child_projection_with_page_index(nested_fixture):
    path, table, _, _ = nested_fixture
    pq.write_table(table, path, write_page_index=True, use_dictionary=False,
                   data_page_size=2048, write_batch_size=64)
    fields = [DataField(0, 'text', AtomicType('STRING')),
              DataField(1, 'missing', AtomicType('INT')),
              DataField(2, 'id', AtomicType('BIGINT'))]
    kwargs = {'fields': fields, 'nested_name_paths': [['record', 'text'], ['record', 'absent'], ['id']],
              'row_ranges': [(4500, 4540)]}
    baseline, _ = _read(nested_fixture, baseline=True, **kwargs)
    with patch.object(page_module.ParquetPageIndexReader, '_column_payload',
                      autospec=True, side_effect=page_module.ParquetPageIndexReader._column_payload
                      ) as read_pages:
        actual, _ = _read(nested_fixture, **kwargs)
    assert read_pages.called
    assert actual.equals(baseline)
    assert actual.column('text').to_pylist() == [
        None if value is None else value['text'] for value in table['record'].slice(4500, 41).to_pylist()]
    assert actual.column('missing').null_count == 41


@pytest.mark.parametrize('version', ['1.0', '2.0'])
def test_repeated_page_row_count_corruption_is_not_hidden(nested_fixture, version):
    path, table, _, _ = nested_fixture
    # A single-leaf nested field isolates V1's value count from its row count.
    table = table.select(['matrix'])
    pq.write_table(table, path, write_page_index=True, data_page_version=version,
                   use_dictionary=False, data_page_size=1024, write_batch_size=64)
    with pa.OSFile(path, 'rb') as source:
        reader = page_module.ParquetPageIndexReader.create(
            source, pq.ParquetFile(source), ['matrix'], [0], 71)
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
    fields = PyarrowFieldParser.to_paimon_schema(table.schema)
    with pytest.raises((ValueError, pa.ArrowInvalid), match='rows|row'):
        _read(nested_fixture, fields=fields, row_ranges=[(0, 2)])


def test_nested_alignment_can_fall_back_when_no_pages_can_be_skipped(nested_fixture):
    path, table, _, _ = nested_fixture
    # One leaf has a single page, forcing the field's common span to the full group.
    table = pa.table({'record': pa.StructArray.from_arrays(
        [pa.array([True] * N), table['payload'].combine_chunks()], names=['flag', 'text'])})
    pq.write_table(table, path, write_page_index=True, use_dictionary=False,
                   data_page_size=4096, write_batch_size=64)
    fields = PyarrowFieldParser.to_paimon_schema(table.schema)
    with patch.object(page_module.ParquetPageIndexReader, '_column_payload',
                      side_effect=AssertionError('must fall back before reading pages')):
        actual, _ = _read(nested_fixture, fields=fields, row_ranges=[(4500, 4540)])
    assert actual.equals(table.slice(4500, 41))
