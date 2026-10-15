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

from io import BytesIO
from unittest.mock import patch

import pytest

from pypaimon.globalindex.full_text.native_full_text_global_index_reader import (
    NativeFullTextIndexOptions, PaimonFullTextInput, _search_top_k,
)
from pypaimon.globalindex.global_index_reader import _completed_future
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.source.full_text_read import DataEvolutionFullTextRead, _search_raw_full_text
from pypaimon.table.source.full_text_search_split import IndexFullTextSearchSplit
from pypaimon.tests.vector_search_filter_test import _StubTable, _entry, _field, _bitmap, match_query


def _index(rows):
    native = pytest.importorskip("paimon_ftindex")
    writer = native.FullTextIndexWriter(NativeFullTextIndexOptions().to_native_options())
    try:
        for row_id, text in rows:
            writer.add_document(row_id, text)
        output = BytesIO()
        writer.write(output)
        return output.getvalue()
    finally:
        writer.close()


@pytest.mark.parametrize("filtered", [False, True])
def test_top_k_preserves_row_id_ties_and_raw_offsets(filtered):
    data = _index([(row_id, "paimon") for row_id in [100, 50, 30, 1, 2, 3]])
    include = _bitmap(150, 130, 102) if filtered else None
    result = _search_raw_full_text(data, 100, match_query("paimon"), 1, include)
    assert list(result.results()) == ([102] if filtered else [101])


def test_top_k_does_not_fetch_the_whole_index_without_boundary_ties():
    native = pytest.importorskip("paimon_ftindex")
    data = _index([(i, "paimon " + "lake " * i) for i in range(128)])
    reader = native.FullTextIndexReader(PaimonFullTextInput(BytesIO(data)))
    calls = []

    class RecordingReader:
        def search(self, query, **kwargs):
            calls.append(kwargs["limit"])
            return reader.search(query, **kwargs)

    try:
        ids, scores = reader.search(match_query("paimon"), limit=128)
        expected = DictBasedScoredIndexResult(dict(zip(ids, scores))).top_k(3)
        actual = _search_top_k(RecordingReader(), match_query("paimon"), 3)
        assert list(actual.results()) == list(expected.results())
        assert {i: actual.score_getter()(i) for i in actual.results()} == {
            i: expected.score_getter()(i) for i in expected.results()}
        assert calls[0] == 4
        assert max(calls) < 128
    finally:
        reader.close()


@pytest.mark.parametrize("overlap", [False, True])
def test_per_shard_top_k_preserves_global_ranking_and_duplicate_precedence(overlap):
    field = _field(0, "text", "STRING")
    starts = [0, 0] if overlap else [0, 3]
    entries = [_entry(None, 0, "full-text", str(i), start, start + 2)
               for i, start in enumerate(starts)]
    splits = [IndexFullTextSearchSplit("text", start, start + 2, [entry.index_file])
              for start, entry in zip(starts, entries)]
    scores = [{0: 1., 1: 3., 2: 2.}, {0: 10., 1: 4., 2: 2.}]
    limits = []

    class Reader:
        def __init__(self, index):
            self.index = index

        def visit_full_text_search(self, search):
            limits.append(search.limit)
            return _completed_future(DictBasedScoredIndexResult(scores[self.index]).top_k(search.limit))

        def close(self):
            pass

    with patch("pypaimon.table.source.full_text_read._create_full_text_reader",
               side_effect=lambda kind, io, path, metas: Reader(int(metas[0].file_name))):
        read = DataEvolutionFullTextRead(_StubTable([field], entries), 1, field, match_query("paimon"))
        result = read._eval_column_query({"text": splits}, None)
    assert list(result.results()) == ([1] if overlap else [3])
    assert limits == ([3, 3] if overlap else [1, 1])
