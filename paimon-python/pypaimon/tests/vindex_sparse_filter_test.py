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

"""Python Vindex searches allow fewer valid hits than the requested limit."""

import io

import numpy as np
import pytest

from pypaimon.execution.python.read.search.vector_search_read import _create_vector_reader
from pypaimon.globalindex.batch_vector_search import BatchVectorSearch
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


@pytest.fixture(params=['l2', 'cosine', 'inner_product'])
def index_bytes(request):
    vindex = pytest.importorskip('paimon_vindex')
    vectors = np.array([[1., 0.], [0., 1.], [1., 1.], [2., 2.]], dtype=np.float32)
    options = {'index.type': 'ivf_pq', 'dimension': '2', 'nlist': '1',
               'pq.m': '1', 'metric': request.param}
    output = io.BytesIO()
    with vindex.VectorIndexTrainer.train(options, vectors) as training:
        with vindex.VectorIndexWriter(training) as writer:
            writer.add_vectors(np.array([0, 1, 3, 4], dtype=np.int64), vectors)
            writer.write(output)
    return output.getvalue()


@pytest.mark.parametrize('batch_size', [None, 1, 2], ids=['single', 'batch-one', 'batch-two'])
@pytest.mark.parametrize('selected, expected', [([1, 2, 4], [1, 4]), ([2], []), ([], [])],
                         ids=['live-null', 'null-only', 'empty'])
def test_python_ivfpq_sparse_filter(index_bytes, batch_size, selected, expected):
    class FileIO:
        def new_input_stream(self, path):
            self.stream = io.BytesIO(index_bytes)
            return self.stream

    file_io = FileIO()
    include = RoaringBitmap64()
    for row_id in selected:
        include.add(row_id)
    # Row 2 is a live NULL vector; rows 0 and 3 are deleted by the query filter.
    query_type = BatchVectorSearch if batch_size else VectorSearch
    vectors = [[1., 1.], [0., 1.]][:batch_size] if batch_size else [1., 1.]
    query = query_type(vectors, 64, 'embedding', options={'ivf.nprobe': '1'})
    reader = _create_vector_reader('ivf-pq', file_io, '/indexes', [GlobalIndexIOMeta('file', len(index_bytes))])
    with reader:
        search = reader.visit_batch_vector_search if batch_size else reader.visit_vector_search
        reference = search(query).result()
        results = search(query.with_include_row_ids(include)).result()
        if not batch_size:
            reference, results = [reference], [results]
        assert len(results) == len(reference) == (batch_size or 1)
        for result, unfiltered in zip(results, reference):
            assert ([] if result is None else sorted(result.results())) == expected
            if result is not None:
                assert {row_id: result.score_getter()(row_id) for row_id in expected} == pytest.approx(
                    {row_id: unfiltered.score_getter()(row_id) for row_id in expected})
    assert file_io.stream.closed
