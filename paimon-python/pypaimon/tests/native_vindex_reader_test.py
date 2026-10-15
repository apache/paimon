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

"""Python positional I/O delegates vector-index queries and scores to Rust core."""

import io
from unittest.mock import patch

import numpy as np
import pytest

from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import (
    PaimonVindexInput, VINDEX_IDENTIFIERS, VindexVectorGlobalIndexReader,
)
from pypaimon.read.native_plan import native_method_available
from pypaimon.utils.roaring_bitmap import RoaringBitmap64

pytestmark = pytest.mark.skipif(
    not native_method_available('VindexVectorGlobalIndexReader', 'search'),
    reason='Rust low-level vector-index reader required')


def _native_type():
    from pypaimon_rust.datafusion import VindexVectorGlobalIndexReader
    return VindexVectorGlobalIndexReader


def _bitmap(ids):
    bitmap = RoaringBitmap64()
    for row_id in ids:
        bitmap.add(row_id)
    return bitmap


def _options(kind):
    return {'diskann.l_search': '64'} if kind == 'diskann' else {'ivf.nprobe': '64'}


def _reference_scores(kind, data, queries):
    from paimon_vindex import SearchParams, VectorIndexReader
    params = SearchParams.diskann(4, 64) if kind == 'diskann' else SearchParams.ivf(4, 64)
    input_ = PaimonVindexInput(io.BytesIO(data))
    try:
        with VectorIndexReader(input_) as reader:
            reader.optimize_for_search()
            if len(queries) == 1:
                ids, distances = reader.search(queries[0], params)
                ids, distances = [ids], [distances]
            else:
                ids, distances = reader.search_batch(queries, params)
            # Query the same encoded index without a sparse filter to avoid the
            # FFI length bug. Quantizers may approximate the original distances;
            # Java converts their estimates using float32 arithmetic.
            return [{int(row_id): float(np.float32(1.) / (np.float32(1.) + distance))
                     for row_id, distance in zip(query_ids, query_distances) if row_id >= 0}
                    for query_ids, query_distances in zip(ids, distances)]
    finally:
        input_.close()


@pytest.fixture(params=VINDEX_IDENTIFIERS)
def index_bytes(request):
    vindex = pytest.importorskip('paimon_vindex')
    kind = request.param
    options = {'index.type': kind.replace('-', '_'), 'dimension': '2', 'metric': 'l2'}
    if kind != 'diskann':
        options['nlist'] = '1'
    if kind == 'ivf-pq':
        options['pq.m'] = '1'
    if kind == 'diskann':
        options['pq.bits'] = '4'
    vectors = np.array([[1., 0.], [0., 1.], [1., 1.], [2., 2.]], dtype=np.float32)
    output = io.BytesIO()
    with vindex.VectorIndexTrainer.train(options, vectors) as training:
        with vindex.VectorIndexWriter(training) as writer:
            writer.add_vectors(np.array([0, 1, 3, 4], dtype=np.int64), vectors)
            writer.write(output)
    return kind, output.getvalue()


@pytest.mark.parametrize('batch', [False, True])
def test_sparse_filter_allows_fewer_hits_than_limit(index_bytes, batch):
    kind, data = index_bytes
    stream = io.BytesIO(data)
    input_ = PaimonVindexInput(stream)
    reader = _native_type()(input_, 'index', len(data))
    try:
        assert reader.metric() == 'l2'
        queries = [[1., 1.], [0., 1.]] if batch else [[1., 1.]]
        # Row 2 is a live NULL vector. It counts toward the filter's cardinality,
        # but cannot yield a hit; deleted rows 0 and 3 must never be returned.
        results = reader.search(queries, 64, _bitmap([1, 2, 4]).serialize(), _options(kind))
        reference = _reference_scores(kind, data, queries)
        assert [sorted(result) for result in results] == [[1, 4]] * len(queries)
        for result, scores in zip(results, reference):
            assert result == pytest.approx({row_id: scores[row_id] for row_id in [1, 4]})
        for selected in ([], [2]):
            assert reader.search(queries, 64, _bitmap(selected).serialize(), _options(kind)) == [
                None] * len(queries)
        # Filters and options belong to a call, rather than leaking across calls.
        assert sorted(reader.search([[1., 1.]], 64, options=_options(kind))[0]) == [0, 1, 3, 4]
        wrong_family = {'ivf.nprobe': '1'} if kind == 'diskann' else {'diskann.l_search': '64'}
        with pytest.raises(ValueError, match='cannot be used'):
            reader.search([[1., 1.]], 1, options=wrong_family)
    finally:
        reader.close()
        reader.close()
        input_.close()
    # The adapter's owner closes its stream, as with the existing Python reader.
    assert not stream.closed
    stream.close()
    with pytest.raises(ValueError, match='closed'):
        reader.search([[1., 1.]], 1)


@pytest.mark.parametrize('batch', [False, True])
def test_python_reader_uses_core_without_third_party_search(index_bytes, batch):
    kind, data = index_bytes
    reference = _reference_scores(kind, data, [[1., 1.]])[0]

    class FileIO:
        def __init__(self):
            self.stream = io.BytesIO(data)

        def new_input_stream(self, path):
            assert path == '/indexes/file'
            return self.stream

    file_io = FileIO()
    reader = VindexVectorGlobalIndexReader(
        file_io, '/indexes', [GlobalIndexIOMeta('file', len(data))])
    with patch('paimon_vindex.VectorIndexReader', side_effect=AssertionError('third-party FFI used')):
        try:
            assert reader.vector_metric() == 'l2'
            include = _bitmap([1, 2, 4])
            if batch:
                query = BatchVectorSearch(
                    [[1., 1.], [0., 1.]], 64, 'embedding', options=_options(kind))
                results = reader.visit_batch_vector_search(query.with_include_row_ids(include)).result()
            else:
                query = VectorSearch([1., 1.], 64, 'embedding', options=_options(kind))
                results = [reader.visit_vector_search(query.with_include_row_ids(include)).result()]
            assert [sorted(result.results()) for result in results] == [[1, 4]] * len(results)
            assert results[0].score_getter()(1) == pytest.approx(reference[1])
        finally:
            reader.close()
    assert file_io.stream.closed
    assert reader._reader is None
    assert reader._index_input is None


def test_core_errors_and_query_option_overrides(index_bytes):
    kind, data = index_bytes
    input_ = PaimonVindexInput(io.BytesIO(data))
    # An invalid reader default must be superseded by a per-query value.
    reader = _native_type()(input_, 'index', len(data), {
        'diskann.l_search' if kind == 'diskann' else 'ivf.nprobe': 'invalid'})
    try:
        assert sorted(reader.search([[1., 1.]], 64, options=_options(kind))[0]) == [0, 1, 3, 4]
        with pytest.raises(ValueError, match='integer'):
            reader.search([[1., 1.]], 1)
        with pytest.raises(ValueError, match='dimension'):
            reader.search([[1.]], 1, options=_options(kind))
        with pytest.raises(ValueError, match='Limit'):
            reader.search([[1., 1.]], 0, options=_options(kind))
        with pytest.raises(ValueError, match='Cannot set both'):
            reader.search([[1., 1.]], 1, options={'ivf.nprobe': '1', 'diskann.l_search': '64'})
        with pytest.raises(ValueError):
            reader.search([[1., 1.]], 1, filter_bytes=b'invalid')
    finally:
        reader.close()
        input_.close()


def test_callback_exception_preserves_python_identity_and_short_reads_fail():
    failure = OSError('index input unavailable')

    class FailingInput:
        def pread_many(self, ranges):
            raise failure

    with pytest.raises(OSError) as raised:
        _native_type()(FailingInput(), 'index', 100)
    assert raised.value is failure

    class ShortInput:
        def pread_many(self, ranges):
            return [b'' for _ in ranges]

    with pytest.raises(ValueError, match='short read'):
        _native_type()(ShortInput(), 'index', 100)

    class MissingRange:
        def pread_many(self, ranges):
            return []

    with pytest.raises(ValueError, match='number of ranges'):
        _native_type()(MissingRange(), 'index', 100)
