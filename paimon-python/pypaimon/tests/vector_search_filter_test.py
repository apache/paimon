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

"""Tests for VectorSearch + scalar predicate pre-filter wiring in pypaimon.

Each test protects a distinct behavior introduced by this feature; no
redundancy.
"""

import io
import json
import struct
import sys
import types
import unittest
from typing import List
from unittest import mock

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.btree.btree_index_meta import BTreeIndexMeta
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta, GlobalIndexMeta
from pypaimon.globalindex.global_index_reader import _completed_future
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.full_text_query import MatchQuery
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.vector_search_result import ScoredGlobalIndexResult
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl
from pypaimon.utils.roaring_bitmap import RoaringBitmap64
from pypaimon.utils.range import Range


# ----------------------------- table stubs ---------------------------------


class _StubSchema:
    def __init__(self):
        self.options = {}


class _StubTable:
    """Minimal FileStoreTable stand-in."""

    def __init__(self, fields, entries, partition_fields=None):
        self.fields = fields
        self.partition_keys_fields = partition_fields or []
        self.partition_keys: List[str] = [
            f.name for f in self.partition_keys_fields]
        self.table_schema = _StubSchema()
        self.file_io = object()
        self._entries = entries

    def tag_manager(self):
        return None

    def snapshot_manager(self):
        return None

    def path_factory(self):
        class _P:
            def global_index_path_factory(self_inner):
                class _G:
                    def index_path(self_g):
                        return "/tmp/unused"
                return _G()
        return _P()

    def new_vector_search_builder(self):
        from pypaimon.table.source.vector_search_builder import (
            VectorSearchBuilderImpl,
        )
        return VectorSearchBuilderImpl(self)

    def new_full_text_search_builder(self):
        from pypaimon.table.source.full_text_search_builder import (
            FullTextSearchBuilderImpl,
        )
        return FullTextSearchBuilderImpl(self)


def _field(fid, name, dtype="INT"):
    return DataField(id=fid, name=name,
                     type=AtomicType(dtype), description="")


def _entry(partition_row, field_id, index_type, file_name,
           row_range_start, row_range_end, external_path=None):
    meta = GlobalIndexMeta(
        row_range_start=row_range_start,
        row_range_end=row_range_end,
        index_field_id=field_id,
        index_meta=b"",
    )
    index_file = IndexFileMeta(
        index_type=index_type,
        file_name=file_name,
        file_size=1,
        row_count=row_range_end - row_range_start + 1,
        global_index_meta=meta,
        external_path=external_path,
    )
    return IndexManifestEntry(kind=0, partition=partition_row, bucket=0,
                              index_file=index_file)


def _install_raw_vector_read_builder(table, vector_column_name, row_id_to_vector,
                                     calls=None):
    """Install a fake raw read builder which honors GlobalIndexResult ranges."""
    import pyarrow as pa

    calls = calls if calls is not None else {}

    class _Plan:
        def __init__(self, row_ids):
            self._row_ids = row_ids

        def splits(self):
            return list(self._row_ids)

    class _Scan:
        def __init__(self):
            self._row_ids = []

        def with_global_index_result(self, result):
            ranges = result.results().to_range_list()
            calls["global_index_ranges"] = ranges
            self._row_ids = [
                row_id
                for row_id in sorted(row_id_to_vector)
                if any(r.contains(row_id) for r in ranges)
            ]
            calls["candidate_ids"] = list(self._row_ids)
            return self

        def plan(self):
            return _Plan(self._row_ids)

    class _Read:
        def to_arrow(self, splits):
            row_ids = list(splits)
            return pa.table({
                vector_column_name: pa.array(
                    [row_id_to_vector[row_id] for row_id in row_ids]),
                "_ROW_ID": pa.array(row_ids, type=pa.int64()),
            })

    class _Builder:
        def with_partition_filter(self, predicate):
            calls["partition_filter"] = predicate
            return self

        def with_filter(self, predicate):
            calls["filter"] = predicate
            return self

        def with_projection(self, projection):
            calls["projection"] = list(projection)
            return self

        def new_scan(self):
            return _Scan()

        def new_read(self):
            return _Read()

    table.new_read_builder = lambda: _Builder()
    return calls


def _patch_snapshot(testcase, entries, snapshot=None):
    """Stub IndexFileHandler.scan + snapshot resolution."""

    mock.patch.stopall()
    for attr in ("_scan_patch", "_travel_patch"):
        patcher = getattr(testcase, attr, None)
        if patcher is not None:
            try:
                patcher.stop()
            except RuntimeError:
                pass

    def _scan(snapshot, entry_filter=None):
        if entry_filter is None:
            return list(entries)
        return [e for e in entries if entry_filter(e)]

    testcase._scan_patch = mock.patch(
        "pypaimon.index.index_file_handler.IndexFileHandler.scan",
        autospec=True, side_effect=lambda self_, s, f=None: _scan(s, f))
    testcase._scan_patch.start()
    testcase._travel_patch = mock.patch(
        "pypaimon.snapshot.time_travel_util.TimeTravelUtil.try_travel_to_snapshot",
        return_value=snapshot if snapshot is not None else object())
    testcase._travel_patch.start()


def _java_tantivy_meta(tokenizer="ngram", min_gram=2, max_gram=2,
                       prefix_only=False, lower_case=True,
                       max_token_length=40, ascii_folding=False,
                       stem=False, language="english",
                       remove_stop_words=False, stop_words="",
                       with_position=True):
    config = {}
    if tokenizer != "default":
        config["tokenizer"] = tokenizer
    if min_gram != 2:
        config["ngram.min-gram"] = min_gram
    if max_gram != 2:
        config["ngram.max-gram"] = max_gram
    if prefix_only:
        config["ngram.prefix-only"] = prefix_only
    if not lower_case:
        config["lower-case"] = lower_case
    if max_token_length != 40:
        config["max-token-length"] = max_token_length
    if ascii_folding:
        config["ascii-folding"] = ascii_folding
    if stem:
        config["stem"] = stem
    if language != "english":
        config["language"] = language
    if remove_stop_words:
        config["remove-stop-words"] = remove_stop_words
    if stop_words:
        config["stop-words"] = stop_words
    if not with_position:
        config["with-position"] = with_position
    return json.dumps(config, separators=(",", ":")).encode("utf-8")


class _FakeFileIO:
    def new_input_stream(self, path):
        buf = io.BytesIO()
        buf.write(struct.pack(">i", 1))
        name = b"meta.json"
        buf.write(struct.pack(">i", len(name)))
        buf.write(name)
        data = b"{}"
        buf.write(struct.pack(">q", len(data)))
        buf.write(data)
        buf.seek(0)
        return buf


class _FakeSchemaBuilder:
    def __init__(self):
        self.fields = {}

    def add_unsigned_field(self, name, stored=False, indexed=True, fast=False):
        self.fields[name] = {"fast": fast, "stored": stored}

    def add_text_field(self, name, stored=False, tokenizer_name=None, **kwargs):
        if "index_option" in kwargs and kwargs["index_option"] is None:
            raise TypeError("index_option must not be None")
        self.fields[name] = {
            "stored": stored,
            "tokenizer_name": tokenizer_name or "default",
        }
        if "index_option" in kwargs:
            self.fields[name]["index_option"] = kwargs["index_option"]

    def build(self):
        return types.SimpleNamespace(fields=self.fields)


class _FakeTokenizer:
    @staticmethod
    def ngram(min_gram=2, max_gram=3, prefix_only=False):
        return ("ngram", min_gram, max_gram, prefix_only)

    @staticmethod
    def simple():
        return ("simple",)

    @staticmethod
    def whitespace():
        return ("whitespace",)

    @staticmethod
    def raw():
        return ("raw",)


class _FakeFilter:
    @staticmethod
    def lowercase():
        return "lowercase"

    @staticmethod
    def remove_long(length_limit):
        return ("remove_long", length_limit)

    @staticmethod
    def ascii_fold():
        return "ascii_fold"

    @staticmethod
    def stemmer(language):
        return ("stemmer", language)

    @staticmethod
    def stopword(language):
        return ("stopword", language)

    @staticmethod
    def custom_stopword(stopwords):
        return ("custom_stopword", tuple(stopwords))


class _FakeTextAnalyzerBuilder:
    def __init__(self, tokenizer):
        self._tokenizer = tokenizer
        self._filters = []

    def filter(self, filter_):
        result = _FakeTextAnalyzerBuilder(self._tokenizer)
        result._filters = self._filters + [filter_]
        return result

    def build(self):
        return self._tokenizer + (tuple(self._filters),)


class _FakeQuery:
    @staticmethod
    def empty_query():
        return ("empty",)

    @staticmethod
    def boost_query(query, boost):
        return ("boost", query, boost)

    @staticmethod
    def fuzzy_term_query(
            schema, field_name, text, distance=1,
            transposition_cost_one=True, prefix=False):
        return (
            "fuzzy",
            schema,
            field_name,
            text,
            distance,
            transposition_cost_one,
            prefix,
        )

    @staticmethod
    def term_query(schema, field_name, field_value, index_option="position"):
        return ("term", schema, field_name, field_value, index_option)

    @staticmethod
    def boolean_query(subqueries, minimum_number_should_match=None):
        return ("boolean", tuple(subqueries), minimum_number_should_match)


class _FakeOccur:
    Should = "should"
    Must = "must"
    MustNot = "must_not"


class _FakeSearchResults:
    def __init__(self, hits=None):
        self.hits = hits if hits is not None else [(2.0, "addr")]


class _FakeSearcher:
    def __init__(self):
        self.query = None
        self.queries = []

    def search(self, query, limit):
        self.query = query
        self.queries.append(query)
        query_text = _fake_query_text(query)
        if query_text == "positive":
            return _FakeSearchResults([(10.0, ("addr", 1)), (5.0, ("addr", 2))])
        if query_text == "negative":
            return _FakeSearchResults([(7.0, ("addr", 2))])
        return _FakeSearchResults()

    def fast_field_values(self, name, addresses):
        return [
            address[1] if isinstance(address, tuple) else 7
            for address in addresses
        ]


def _fake_query_text(query):
    if isinstance(query, tuple) and query:
        if query[0] == "boost":
            return _fake_query_text(query[1])
        if isinstance(query[0], str):
            return query[0]
    return None


class _FakeIndex:
    def __init__(self, schema, directory=None):
        self.schema = schema
        self.directory = directory
        self.registered_tokenizer = None

    def register_tokenizer(self, name, analyzer):
        self.registered_tokenizer = (name, analyzer)

    def reload(self):
        pass

    def searcher(self):
        self.searcher_instance = _FakeSearcher()
        return self.searcher_instance

    def parse_query(self, query_text, fields, **kwargs):
        return (query_text, tuple(fields), kwargs)


class _FakeTantivy(types.SimpleNamespace):
    def __init__(self):
        super().__init__()
        self.Tokenizer = _FakeTokenizer
        self.Filter = _FakeFilter
        self.TextAnalyzerBuilder = _FakeTextAnalyzerBuilder
        self.Query = _FakeQuery
        self.Occur = _FakeOccur
        self.last_schema = None
        self.last_index = None
        parent = self

        class SchemaBuilder(_FakeSchemaBuilder):
            def build(self_inner):
                parent.last_schema = super().build()
                return parent.last_schema

        class Index(_FakeIndex):
            def __init__(self_inner, schema, directory=None):
                super().__init__(schema, directory=directory)
                parent.last_index = self_inner

        self.SchemaBuilder = SchemaBuilder
        self.Index = Index


# ----------------------------- tests ---------------------------------------


class VectorReaderFactoryTest(unittest.TestCase):
    """Vector reader factory compatibility."""

    def test_lumina_reader_accepts_new_and_legacy_identifiers(self):
        from pypaimon.globalindex.lumina.lumina_vector_global_index_reader import (
            LUMINA_IDENTIFIERS,
            LuminaVectorGlobalIndexReader,
        )
        from pypaimon.table.source.vector_search_read import _create_vector_reader

        io_meta = GlobalIndexIOMeta(file_name="vec.index", file_size=1)
        for index_type in LUMINA_IDENTIFIERS:
            reader = _create_vector_reader(
                index_type, object(), "/tmp/unused", [io_meta], {})
            try:
                self.assertIsInstance(reader, LuminaVectorGlobalIndexReader)
            finally:
                reader.close()


class VectorOptionsTest(unittest.TestCase):
    """VectorSearch options compatibility."""

    def test_offset_range_preserves_options(self):
        search = VectorSearch(
            vector=[1.0, 0.0],
            limit=1,
            field_name="embedding",
            options={"ivf.nprobe": "16", "hnsw.ef_search": "64"},
        )
        include_row_ids = RoaringBitmap64()
        include_row_ids.add_range(100, 200)

        offset = search.with_include_row_ids(include_row_ids).offset_range(60, 150)

        self.assertEqual(
            {"ivf.nprobe": "16", "hnsw.ef_search": "64"},
            offset.options,
        )


class LuminaOptionsTest(unittest.TestCase):
    """Lumina query-time option compatibility."""

    def test_query_options_override_index_options(self):
        from pypaimon.globalindex.lumina.lumina_vector_global_index_reader import (
            _merge_options,
        )

        merged = _merge_options(
            {"diskann.search.list_size": "16", "search.parallel_number": "2"},
            {"diskann.search.list_size": "32", "index.dimension": "4"},
            {"diskann.search.list_size": "64", "hnsw.ef_search": "128"},
        )

        self.assertEqual("64", merged["diskann.search.list_size"])
        self.assertEqual("2", merged["search.parallel_number"])
        self.assertEqual("4", merged["index.dimension"])
        self.assertEqual("128", merged["hnsw.ef_search"])


class TantivyFullTextIndexOptionsTest(unittest.TestCase):
    """Tantivy full-text tokenizer metadata compatibility."""

    def test_empty_metadata_uses_default_tokenizer(self):
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextIndexOptions,
        )

        options = TantivyFullTextIndexOptions.deserialize(b"")

        self.assertEqual("default", options.tokenizer)
        self.assertEqual(2, options.ngram_min_gram)
        self.assertEqual(2, options.ngram_max_gram)
        self.assertFalse(options.ngram_prefix_only)
        self.assertTrue(options.lower_case)
        self.assertEqual(40, options.max_token_length)
        self.assertFalse(options.ascii_folding)
        self.assertFalse(options.stem)
        self.assertEqual("english", options.language)
        self.assertFalse(options.remove_stop_words)
        self.assertEqual("", options.stop_words)
        self.assertTrue(options.with_position)
        self.assertEqual("default", options.tokenizer_name())

    def test_empty_json_metadata_uses_default_tokenizer(self):
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextIndexOptions,
        )

        options = TantivyFullTextIndexOptions.deserialize(b"{}")

        self.assertEqual("default", options.tokenizer)
        self.assertEqual(2, options.ngram_min_gram)
        self.assertEqual(2, options.ngram_max_gram)
        self.assertFalse(options.ngram_prefix_only)
        self.assertTrue(options.lower_case)
        self.assertEqual(40, options.max_token_length)
        self.assertFalse(options.ascii_folding)
        self.assertFalse(options.stem)
        self.assertEqual("english", options.language)
        self.assertFalse(options.remove_stop_words)
        self.assertEqual("", options.stop_words)
        self.assertTrue(options.with_position)
        self.assertEqual("default", options.tokenizer_name())

    def test_deserializes_java_ngram_metadata(self):
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TANTIVY_NGRAM_TOKENIZER,
            TantivyFullTextIndexOptions,
        )

        options = TantivyFullTextIndexOptions.deserialize(
            _java_tantivy_meta(
                tokenizer=" NGRAM ", min_gram=2, max_gram=3,
                prefix_only=True, lower_case=False))

        self.assertEqual("ngram", options.tokenizer)
        self.assertEqual(2, options.ngram_min_gram)
        self.assertEqual(3, options.ngram_max_gram)
        self.assertTrue(options.ngram_prefix_only)
        self.assertFalse(options.lower_case)
        self.assertEqual(TANTIVY_NGRAM_TOKENIZER, options.tokenizer_name())

    def test_deserializes_java_json_ngram_metadata(self):
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TANTIVY_NGRAM_TOKENIZER,
            TantivyFullTextIndexOptions,
        )

        options = TantivyFullTextIndexOptions.deserialize(
            _java_tantivy_meta(
                tokenizer=" NGRAM ", min_gram=2, max_gram=3,
                prefix_only=True, lower_case=False))

        self.assertEqual("ngram", options.tokenizer)
        self.assertEqual(2, options.ngram_min_gram)
        self.assertEqual(3, options.ngram_max_gram)
        self.assertTrue(options.ngram_prefix_only)
        self.assertFalse(options.lower_case)
        self.assertEqual(TANTIVY_NGRAM_TOKENIZER, options.tokenizer_name())

    def test_deserializes_extended_analyzer_metadata(self):
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextIndexOptions,
        )

        options = TantivyFullTextIndexOptions.deserialize(
            _java_tantivy_meta(
                tokenizer=" WHITESPACE ", lower_case=False, max_token_length=12,
                ascii_folding=True, stem=True, language="English",
                remove_stop_words=True, stop_words="paimon;lake",
                with_position=False))

        self.assertEqual("whitespace", options.tokenizer)
        self.assertFalse(options.lower_case)
        self.assertEqual(12, options.max_token_length)
        self.assertTrue(options.ascii_folding)
        self.assertTrue(options.stem)
        self.assertEqual("english", options.language)
        self.assertTrue(options.remove_stop_words)
        self.assertEqual("paimon;lake", options.stop_words)
        self.assertEqual(["paimon", "lake"], options.stop_word_list())
        self.assertFalse(options.with_position)
        self.assertEqual("paimon_custom", options.tokenizer_name())

    def test_deserializes_java_json_analyzer_metadata(self):
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextIndexOptions,
        )

        options = TantivyFullTextIndexOptions.deserialize(
            _java_tantivy_meta(
                tokenizer=" WHITESPACE ", lower_case=False, max_token_length=12,
                ascii_folding=True, stem=True, language="English",
                remove_stop_words=True, stop_words=["paimon", "lake"],
                with_position=False))

        self.assertEqual("whitespace", options.tokenizer)
        self.assertFalse(options.lower_case)
        self.assertEqual(12, options.max_token_length)
        self.assertTrue(options.ascii_folding)
        self.assertTrue(options.stem)
        self.assertEqual("english", options.language)
        self.assertTrue(options.remove_stop_words)
        self.assertEqual("paimon;lake", options.stop_words)
        self.assertEqual(["paimon", "lake"], options.stop_word_list())
        self.assertFalse(options.with_position)
        self.assertEqual("paimon_custom", options.tokenizer_name())

    def test_deserializes_java_jieba_metadata(self):
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TANTIVY_JIEBA_TOKENIZER,
            TantivyFullTextIndexOptions,
        )

        options = TantivyFullTextIndexOptions.deserialize(
            _java_tantivy_meta(tokenizer=" JIEBA "))

        self.assertEqual("jieba", options.tokenizer)
        self.assertEqual(2, options.ngram_min_gram)
        self.assertEqual(2, options.ngram_max_gram)
        self.assertFalse(options.ngram_prefix_only)
        self.assertTrue(options.lower_case)
        self.assertEqual(TANTIVY_JIEBA_TOKENIZER, options.tokenizer_name())

    def test_ngram_reader_registers_matching_tantivy_analyzer(self):
        from pypaimon.globalindex.full_text_search import FullTextSearch
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TANTIVY_NGRAM_TOKENIZER,
            TantivyFullTextGlobalIndexReader,
        )

        tantivy = _FakeTantivy()
        old_tantivy = sys.modules.get("tantivy")
        sys.modules["tantivy"] = tantivy
        try:
            reader = TantivyFullTextGlobalIndexReader(
                _FakeFileIO(),
                "/unused",
                [GlobalIndexIOMeta(
                    file_name="ft.index",
                    file_size=1,
                    metadata=_java_tantivy_meta(
                        min_gram=2, max_gram=3,
                        prefix_only=True, lower_case=True))])
            try:
                result = reader.visit_full_text_search(
                    FullTextSearch(MatchQuery("中文", "content"), 10)).result()
            finally:
                reader.close()
        finally:
            if old_tantivy is None:
                sys.modules.pop("tantivy", None)
            else:
                sys.modules["tantivy"] = old_tantivy

        self.assertEqual({"row_id": {"fast": True, "stored": False},
                          "text": {"stored": False,
                                   "tokenizer_name": TANTIVY_NGRAM_TOKENIZER}},
                         tantivy.last_schema.fields)
        self.assertEqual(
            (TANTIVY_NGRAM_TOKENIZER,
             ("ngram", 2, 3, True, (("remove_long", 40), "lowercase"))),
            tantivy.last_index.registered_tokenizer)
        self.assertEqual([7], sorted(list(result.results())))

        query = tantivy.last_index.searcher_instance.query
        self.assertEqual(("中文", ("text",), {}), query)

    def test_schema_fallback_for_pre_7670_indexes(self):
        from pypaimon.globalindex.full_text_search import FullTextSearch
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextGlobalIndexReader,
        )

        call_count = [0]

        class _FakeTantivyWithSchemaFallback(_FakeTantivy):
            def __init__(self_outer):
                super().__init__()
                parent = self_outer

                class SchemaBuilder(_FakeSchemaBuilder):
                    def build(self_inner):
                        parent.last_schema = super().build()
                        return parent.last_schema

                class Index(_FakeIndex):
                    def __init__(self_inner, schema, directory=None):
                        call_count[0] += 1
                        row_id_opts = schema.fields.get("row_id", {})
                        if not row_id_opts.get("stored", False):
                            raise ValueError(
                                "Schema error: 'An index exists but "
                                "the schema does not match.'"
                            )
                        super().__init__(schema, directory=directory)
                        parent.last_index = self_inner

                self_outer.SchemaBuilder = SchemaBuilder
                self_outer.Index = Index

        tantivy = _FakeTantivyWithSchemaFallback()
        old_tantivy = sys.modules.get("tantivy")
        sys.modules["tantivy"] = tantivy
        try:
            reader = TantivyFullTextGlobalIndexReader(
                _FakeFileIO(), "/unused",
                [GlobalIndexIOMeta(file_name="ft.index", file_size=1)])
            try:
                reader.visit_full_text_search(
                    FullTextSearch(MatchQuery("hello", "content"), 5)).result()
            finally:
                reader.close()
        finally:
            if old_tantivy is None:
                sys.modules.pop("tantivy", None)
            else:
                sys.modules["tantivy"] = old_tantivy

        self.assertEqual(2, call_count[0])
        self.assertTrue(
            tantivy.last_schema.fields["row_id"].get("stored", False))

    def test_custom_analyzer_reader_registers_matching_tantivy_analyzer(self):
        from pypaimon.globalindex.full_text_search import FullTextSearch
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TANTIVY_CUSTOM_TOKENIZER,
            TantivyFullTextGlobalIndexReader,
        )

        tantivy = _FakeTantivy()
        old_tantivy = sys.modules.get("tantivy")
        sys.modules["tantivy"] = tantivy
        try:
            reader = TantivyFullTextGlobalIndexReader(
                _FakeFileIO(),
                "/unused",
                [GlobalIndexIOMeta(
                    file_name="ft.index",
                    file_size=1,
                    metadata=_java_tantivy_meta(
                        tokenizer="simple", max_token_length=16,
                        ascii_folding=True, stem=True, language="english",
                        remove_stop_words=True, stop_words="paimon;lake",
                        with_position=False))])
            try:
                reader.visit_full_text_search(
                    FullTextSearch(MatchQuery("running", "content"), 10)).result()
            finally:
                reader.close()
        finally:
            if old_tantivy is None:
                sys.modules.pop("tantivy", None)
            else:
                sys.modules["tantivy"] = old_tantivy

        self.assertEqual({"row_id": {"fast": True, "stored": False},
                          "text": {"stored": False,
                                   "tokenizer_name": TANTIVY_CUSTOM_TOKENIZER,
                                   "index_option": "freq"}},
                         tantivy.last_schema.fields)
        self.assertEqual(
            (TANTIVY_CUSTOM_TOKENIZER,
             ("simple",
              (("remove_long", 16), "lowercase", "ascii_fold", ("stemmer", "english"),
               ("stopword", "english"), ("custom_stopword", ("paimon", "lake"))))),
            tantivy.last_index.registered_tokenizer)

    def test_match_reader_aligns_java_boost_and_fuzziness(self):
        from pypaimon.globalindex.full_text_search import FullTextSearch
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextGlobalIndexReader,
        )

        tantivy = _FakeTantivy()
        old_tantivy = sys.modules.get("tantivy")
        sys.modules["tantivy"] = tantivy
        try:
            reader = TantivyFullTextGlobalIndexReader(
                _FakeFileIO(),
                "/unused",
                [GlobalIndexIOMeta(file_name="ft.index", file_size=1)])
            try:
                reader.visit_full_text_search(
                    FullTextSearch(
                        MatchQuery(
                            "paimon",
                            "content",
                            boost=2.0,
                            fuzziness=1,
                            operator="and",
                        ),
                        10,
                    )
                ).result()
            finally:
                reader.close()
        finally:
            if old_tantivy is None:
                sys.modules.pop("tantivy", None)
            else:
                sys.modules["tantivy"] = old_tantivy

        self.assertEqual(
            ("boost",
             ("paimon",
              ("text",),
              {"conjunction_by_default": True,
               "fuzzy_fields": {"text": (False, 1, True)}}),
             2.0),
            tantivy.last_index.searcher_instance.query,
        )

    def test_boost_reader_demotes_negative_matches_like_java(self):
        from pypaimon.globalindex.full_text_query import BoostQuery
        from pypaimon.globalindex.full_text_search import FullTextSearch
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextGlobalIndexReader,
        )

        tantivy = _FakeTantivy()
        old_tantivy = sys.modules.get("tantivy")
        sys.modules["tantivy"] = tantivy
        try:
            reader = TantivyFullTextGlobalIndexReader(
                _FakeFileIO(),
                "/unused",
                [GlobalIndexIOMeta(file_name="ft.index", file_size=1)])
            try:
                result = reader.visit_full_text_search(
                    FullTextSearch(
                        BoostQuery(
                            MatchQuery("positive", "content"),
                            MatchQuery("negative", "content"),
                            negative_boost=0.2,
                        ),
                        10,
                    )
                ).result()
            finally:
                reader.close()
        finally:
            if old_tantivy is None:
                sys.modules.pop("tantivy", None)
            else:
                sys.modules["tantivy"] = old_tantivy

        self.assertEqual([1, 2], sorted(list(result.results())))
        score_getter = result.score_getter()
        self.assertEqual(10.0, score_getter(1))
        self.assertEqual(1.0, score_getter(2))
        self.assertEqual(
            ["positive", "negative"],
            [_fake_query_text(q) for q in tantivy.last_index.searcher_instance.queries],
        )

    def test_reader_rejects_java_unsupported_match_options(self):
        from pypaimon.globalindex.full_text_search import FullTextSearch
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextGlobalIndexReader,
        )

        tantivy = _FakeTantivy()
        old_tantivy = sys.modules.get("tantivy")
        sys.modules["tantivy"] = tantivy
        try:
            reader = TantivyFullTextGlobalIndexReader(
                _FakeFileIO(),
                "/unused",
                [GlobalIndexIOMeta(file_name="ft.index", file_size=1)])
            try:
                with self.assertRaisesRegex(ValueError, "max_expansions"):
                    reader.visit_full_text_search(
                        FullTextSearch(
                            MatchQuery(
                                "paimon",
                                "content",
                                max_expansions=10,
                            ),
                            10,
                        )
                    ).result()
                with self.assertRaisesRegex(ValueError, "prefix_length"):
                    reader.visit_full_text_search(
                        FullTextSearch(
                            MatchQuery(
                                "paimon",
                                "content",
                                prefix_length=1,
                            ),
                            10,
                        )
                    ).result()
            finally:
                reader.close()
        finally:
            if old_tantivy is None:
                sys.modules.pop("tantivy", None)
            else:
                sys.modules["tantivy"] = old_tantivy

    def test_reader_rejects_single_column_multi_match_like_java(self):
        from pypaimon.globalindex.full_text_query import MultiMatchQuery
        from pypaimon.globalindex.full_text_search import FullTextSearch
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextGlobalIndexReader,
        )

        tantivy = _FakeTantivy()
        old_tantivy = sys.modules.get("tantivy")
        sys.modules["tantivy"] = tantivy
        try:
            reader = TantivyFullTextGlobalIndexReader(
                _FakeFileIO(),
                "/unused",
                [GlobalIndexIOMeta(file_name="ft.index", file_size=1)])
            try:
                with self.assertRaisesRegex(ValueError, "multi_match"):
                    reader.visit_full_text_search(
                        FullTextSearch(
                            MultiMatchQuery("paimon", ["title", "content"]),
                            10,
                        )
                    ).result()
            finally:
                reader.close()
        finally:
            if old_tantivy is None:
                sys.modules.pop("tantivy", None)
            else:
                sys.modules["tantivy"] = old_tantivy

    def test_ngram_reader_requires_custom_tokenizer_api(self):
        from pypaimon.globalindex.full_text_search import FullTextSearch
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextGlobalIndexReader,
        )

        old_tantivy = sys.modules.get("tantivy")
        sys.modules["tantivy"] = types.SimpleNamespace(SchemaBuilder=object)
        try:
            reader = TantivyFullTextGlobalIndexReader(
                _FakeFileIO(),
                "/unused",
                [GlobalIndexIOMeta(
                    file_name="ft.index",
                    file_size=1,
                    metadata=_java_tantivy_meta())])
            with self.assertRaisesRegex(
                    RuntimeError, "ngram tokenizer support"):
                reader.visit_full_text_search(
                    FullTextSearch(MatchQuery("中文", "content"), 10)).result()
        finally:
            if old_tantivy is None:
                sys.modules.pop("tantivy", None)
            else:
                sys.modules["tantivy"] = old_tantivy

    def test_jieba_reader_builds_token_query(self):
        from pypaimon.globalindex.full_text_search import FullTextSearch
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TANTIVY_JIEBA_TOKENIZER,
            TantivyFullTextGlobalIndexReader,
        )

        tantivy = _FakeTantivy()
        jieba = types.SimpleNamespace(
            tokenize=lambda text, mode, HMM: [
                ("售货", 0, 2),
                ("货员", 1, 3),
                ("售货员", 0, 3),
                ("售货员", 0, 3)])
        old_tantivy = sys.modules.get("tantivy")
        old_jieba = sys.modules.get("jieba")
        sys.modules["tantivy"] = tantivy
        sys.modules["jieba"] = jieba
        try:
            reader = TantivyFullTextGlobalIndexReader(
                _FakeFileIO(),
                "/unused",
                [GlobalIndexIOMeta(
                    file_name="ft.index",
                    file_size=1,
                    metadata=_java_tantivy_meta(tokenizer="jieba"))])
            try:
                result = reader.visit_full_text_search(
                    FullTextSearch(MatchQuery("售货员", "content", operator="and"), 10)).result()
            finally:
                reader.close()
        finally:
            if old_tantivy is None:
                sys.modules.pop("tantivy", None)
            else:
                sys.modules["tantivy"] = old_tantivy
            if old_jieba is None:
                sys.modules.pop("jieba", None)
            else:
                sys.modules["jieba"] = old_jieba

        self.assertEqual({"row_id": {"fast": True, "stored": False},
                          "text": {"stored": False,
                                   "tokenizer_name": TANTIVY_JIEBA_TOKENIZER}},
                         tantivy.last_schema.fields)
        self.assertIsNone(tantivy.last_index.registered_tokenizer)
        self.assertEqual([7], sorted(list(result.results())))

        query = tantivy.last_index.searcher_instance.query
        self.assertEqual("boolean", query[0])
        self.assertEqual(
            ("售货", "货员", "售货员"),
            tuple(sub_query[1][3] for sub_query in query[1]))
        self.assertEqual(
            ("must", "must", "must"),
            tuple(sub_query[0] for sub_query in query[1]))

    def test_jieba_reader_requires_jieba_package(self):
        from pypaimon.globalindex.full_text_search import FullTextSearch
        from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
            TantivyFullTextGlobalIndexReader,
        )

        tantivy = _FakeTantivy()
        old_tantivy = sys.modules.get("tantivy")
        old_jieba = sys.modules.get("jieba")
        sys.modules["tantivy"] = tantivy
        sys.modules["jieba"] = None
        try:
            reader = TantivyFullTextGlobalIndexReader(
                _FakeFileIO(),
                "/unused",
                [GlobalIndexIOMeta(
                    file_name="ft.index",
                    file_size=1,
                    metadata=_java_tantivy_meta(tokenizer="jieba"))])
            try:
                with self.assertRaisesRegex(RuntimeError, "pip install jieba"):
                    reader.visit_full_text_search(
                        FullTextSearch(MatchQuery("售货员", "content"), 10)).result()
            finally:
                reader.close()
        finally:
            if old_tantivy is None:
                sys.modules.pop("tantivy", None)
            else:
                sys.modules["tantivy"] = old_tantivy
            if old_jieba is None:
                sys.modules.pop("jieba", None)
            else:
                sys.modules["jieba"] = old_jieba


class FullTextQueryDslTest(unittest.TestCase):

    def test_lancedb_style_query_json_round_trip(self):
        from pypaimon.globalindex.full_text_query import (
            BooleanQuery,
            BoostQuery,
            FullTextQuery,
            FullTextOperator,
            MatchQuery,
            MultiMatchQuery,
            Occur,
            PhraseQuery,
        )

        query = FullTextQuery.from_json(
            '{"match":{"column":"content","query":"paimon",'
            '"maxExpansions":10,"prefixLength":1,"fuzziness":"auto"}}')
        self.assertIsInstance(query, MatchQuery)
        self.assertEqual("paimon", query.query)
        self.assertIsNone(query.fuzziness)
        self.assertEqual(10, query.max_expansions)
        self.assertEqual(1, query.prefix_length)

        phrase = PhraseQuery("paimon lake", "content", slop=1)
        self.assertEqual(
            '{"match_phrase":{"column":"content","terms":"paimon lake","slop":1}}',
            phrase.to_json())
        self.assertEqual("match_phrase", phrase.query_type().value)

        boost = BoostQuery(
            MatchQuery("paimon", "content"),
            MatchQuery("vector", "content"),
            negative_boost=0.2)
        self.assertEqual(0.2, boost.negative_boost)
        self.assertIn('"negative_boost":0.2', boost.to_json())

        multi_match = MultiMatchQuery(
            "paimon", ["title", "content"], boosts=[2.0, 1.0],
            operator=FullTextOperator.AND)
        self.assertEqual(["title", "content"], multi_match.columns)
        self.assertEqual(
            '{"multi_match":{"query":"paimon","columns":["title","content"],'
            '"boost":[2.0,1.0],"operator":"And"}}',
            multi_match.to_json())

        boolean = BooleanQuery([
            (Occur.MUST, MatchQuery("paimon", "content")),
            ("must_not", MatchQuery("vector", "content")),
        ])
        self.assertEqual(["content"], boolean.referenced_columns())
        self.assertEqual(1, len(boolean.must()))
        self.assertEqual(1, len(boolean.must_not()))

    def test_lancedb_boolean_queries_alias(self):
        from pypaimon.globalindex.full_text_query import FullTextQuery

        query = FullTextQuery.from_json(
            '{"boolean":{"queries":[{"occur":"must","query":{"match":'
            '{"column":"content","terms":"paimon"}}},["must_not",{"match":'
            '{"column":"content","terms":"vector"}}]]}}')

        self.assertEqual(1, len(query.must()))
        self.assertEqual(1, len(query.must_not()))
        self.assertEqual("content", query.single_column())


class FullTextSearchBuilderDslTest(unittest.TestCase):

    def tearDown(self):
        mock.patch.stopall()

    def test_full_text_builder_threads_structured_query_to_reader(self):
        from pypaimon.table.source.full_text_search_builder import (
            FullTextSearchBuilderImpl,
        )
        from pypaimon.table.source.full_text_search_split import (
            FullTextSearchSplit,
        )

        text_field = _field(1, "content", "STRING")
        entry = _entry(
            None, field_id=1, index_type="tantivy-fulltext",
            file_name="ft.index", row_range_start=0, row_range_end=9)
        table = _StubTable(fields=[text_field], entries=[entry])
        _patch_snapshot(self, [entry])
        captured_searches = []

        def _fake_create(index_type, file_io, index_path, index_io_meta_list):
            class _FakeReader:
                def visit_full_text_search(self_inner, fts):
                    captured_searches.append(fts)
                    return _completed_future(None)

                def close(self_inner):
                    pass
            return _FakeReader()

        split = FullTextSearchSplit(
            column_name="content",
            row_range_start=0, row_range_end=9,
            full_text_index_files=[entry.index_file])

        with mock.patch(
                "pypaimon.table.source.full_text_read._create_full_text_reader",
                side_effect=_fake_create):
            (
                FullTextSearchBuilderImpl(table)
                .with_query(MatchQuery("paimon", "content", operator="and"))
                .with_limit(10)
                .new_full_text_read()
                .read([split])
            )

        self.assertEqual(1, len(captured_searches))
        self.assertEqual("content", captured_searches[0].field_name)
        self.assertEqual(10, captured_searches[0].limit)
        self.assertEqual(
            '{"match":{"column":"content","terms":"paimon","boost":1.0,'
            '"fuzziness":0,"max_expansions":50,"operator":"And",'
            '"prefix_length":0}}',
            captured_searches[0].query_json())

    def test_full_text_builder_sends_leaf_candidate_limit_for_compound_queries(self):
        from pypaimon.globalindex.full_text_query import BoostQuery
        from pypaimon.table.source.full_text_search_builder import (
            FullTextSearchBuilderImpl,
        )
        from pypaimon.table.source.full_text_search_split import (
            FullTextSearchSplit,
        )

        text_field = _field(1, "content", "STRING")
        entry = _entry(
            None, field_id=1, index_type="tantivy-fulltext",
            file_name="ft.index", row_range_start=0, row_range_end=99)
        table = _StubTable(fields=[text_field], entries=[entry])
        _patch_snapshot(self, [entry])
        captured_limits = []

        def _fake_create(index_type, file_io, index_path, index_io_meta_list):
            class _FakeReader:
                def visit_full_text_search(self_inner, fts):
                    captured_limits.append(fts.limit)
                    return _completed_future(None)

                def close(self_inner):
                    pass
            return _FakeReader()

        split = FullTextSearchSplit(
            column_name="content",
            row_range_start=0,
            row_range_end=99,
            full_text_index_files=[entry.index_file])

        with mock.patch(
                "pypaimon.table.source.full_text_read._create_full_text_reader",
                side_effect=_fake_create):
            (
                FullTextSearchBuilderImpl(table)
                .with_query(
                    BoostQuery(
                        MatchQuery("paimon", "content"),
                        MatchQuery("vector", "content"),
                        negative_boost=0.1))
                .with_limit(3)
                .new_full_text_read()
                .read([split])
            )

        self.assertEqual([100, 100], captured_limits)

    def test_full_text_builder_supports_multi_match_across_columns(self):
        from pypaimon.globalindex.full_text_query import MultiMatchQuery
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )
        from pypaimon.table.source.full_text_search_builder import (
            FullTextSearchBuilderImpl,
        )

        title_field = _field(1, "title", "STRING")
        body_field = _field(2, "body", "STRING")
        title_entry = _entry(
            None, field_id=1, index_type="tantivy-fulltext",
            file_name="title.index", row_range_start=0, row_range_end=9)
        body_entry = _entry(
            None, field_id=2, index_type="tantivy-fulltext",
            file_name="body.index", row_range_start=0, row_range_end=9)
        table = _StubTable(
            fields=[title_field, body_field], entries=[title_entry, body_entry])
        _patch_snapshot(self, [title_entry, body_entry])
        captured = []

        def _fake_create(index_type, file_io, index_path, index_io_meta_list):
            file_name = index_io_meta_list[0].file_name

            class _FakeReader:
                def visit_full_text_search(self_inner, fts):
                    captured.append(fts.query_json())
                    if file_name == "title.index":
                        return _completed_future(DictBasedScoredIndexResult({1: 2.0}))
                    return _completed_future(DictBasedScoredIndexResult({2: 1.0}))

                def close(self_inner):
                    pass
            return _FakeReader()

        with mock.patch(
                "pypaimon.table.source.full_text_read._create_full_text_reader",
                side_effect=_fake_create):
            result = (
                FullTextSearchBuilderImpl(table)
                .with_query(MultiMatchQuery("paimon", ["title", "body"]))
                .with_limit(10)
                .execute_local()
            )

        self.assertEqual({1, 2}, set(result.results().to_list()))
        self.assertEqual(2, len(captured))

    def test_full_text_multi_match_adds_column_scores(self):
        from pypaimon.globalindex.full_text_query import MultiMatchQuery
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )
        from pypaimon.table.source.full_text_search_builder import (
            FullTextSearchBuilderImpl,
        )

        title_field = _field(1, "title", "STRING")
        body_field = _field(2, "body", "STRING")
        title_entry = _entry(
            None, field_id=1, index_type="tantivy-fulltext",
            file_name="title.index", row_range_start=0, row_range_end=9)
        body_entry = _entry(
            None, field_id=2, index_type="tantivy-fulltext",
            file_name="body.index", row_range_start=0, row_range_end=9)
        table = _StubTable(
            fields=[title_field, body_field], entries=[title_entry, body_entry])
        _patch_snapshot(self, [title_entry, body_entry])

        def _fake_create(index_type, file_io, index_path, index_io_meta_list):
            file_name = index_io_meta_list[0].file_name

            class _FakeReader:
                def visit_full_text_search(self_inner, fts):
                    if file_name == "title.index":
                        return _completed_future(
                            DictBasedScoredIndexResult({1: 2.0, 3: 1.0}))
                    return _completed_future(
                        DictBasedScoredIndexResult({1: 5.0, 2: 1.0}))

                def close(self_inner):
                    pass
            return _FakeReader()

        with mock.patch(
                "pypaimon.table.source.full_text_read._create_full_text_reader",
                side_effect=_fake_create):
            result = (
                FullTextSearchBuilderImpl(table)
                .with_query(MultiMatchQuery("paimon", ["title", "body"]))
                .with_limit(10)
                .execute_local()
            )

        self.assertEqual({1, 2, 3}, set(result.results().to_list()))
        self.assertEqual(7.0, result.score_getter()(1))
        self.assertEqual(1.0, result.score_getter()(2))
        self.assertEqual(1.0, result.score_getter()(3))

    def test_full_text_scan_ignores_other_index_types_on_same_column(self):
        from pypaimon.table.source.full_text_scan import FullTextScanImpl

        text_field = _field(1, "content", "STRING")
        ft_entry = _entry(
            None, field_id=1, index_type="tantivy-fulltext",
            file_name="ft.index", row_range_start=0, row_range_end=9)
        btree_entry = _entry(
            None, field_id=1, index_type="btree",
            file_name="btree.index", row_range_start=0, row_range_end=9)
        table = _StubTable(fields=[text_field], entries=[ft_entry, btree_entry])
        _patch_snapshot(self, [ft_entry, btree_entry])

        splits = FullTextScanImpl(table, [text_field]).scan().splits()

        self.assertEqual(1, len(splits))
        self.assertEqual(
            ["ft.index"],
            [f.file_name for f in splits[0].full_text_index_files])

    def test_full_text_scan_rejects_full_mode_raw_search(self):
        from pypaimon.common.options.core_options import CoreOptions
        from pypaimon.common.options.options import Options
        from pypaimon.table.source.full_text_scan import FullTextScanImpl

        class _Options:
            options = Options({"global-index.search-mode": "full"})

            def global_index_search_mode(self_inner):
                return CoreOptions(self_inner.options).global_index_search_mode()

        text_field = _field(1, "content", "STRING")
        entry = _entry(
            None, field_id=1, index_type="tantivy-fulltext",
            file_name="ft.index", row_range_start=0, row_range_end=4)
        table = _StubTable(fields=[text_field], entries=[entry])
        table.options = _Options()
        _patch_snapshot(self, [entry], types.SimpleNamespace(next_row_id=10))

        with self.assertRaises(NotImplementedError) as ctx:
            FullTextScanImpl(table, [text_field]).scan()

        self.assertIn("global-index.search-mode=full/detail", str(ctx.exception))
        self.assertIn("rebuilding temporary full-text indexes", str(ctx.exception))


class VectorSearchFilterTest(unittest.TestCase):
    """Non-partitioned wiring: scan + read + external_path plumbing."""

    def setUp(self):
        self.id_field = _field(0, "id")
        self.embedding_field = _field(1, "embedding", "FLOAT")
        # 2 vector files ([0,4], [5,9]) + 1 btree on `id` covering [0,9] with
        # an external_path so we can assert external_path is threaded through.
        self.entries = [
            _entry(None, field_id=1, index_type="lumina-vector-ann",
                   file_name="vec-0.index",
                   row_range_start=0, row_range_end=4,
                   external_path="oss://bucket/vec-0.index"),
            _entry(None, field_id=1, index_type="lumina-vector-ann",
                   file_name="vec-1.index",
                   row_range_start=5, row_range_end=9,
                   external_path="oss://bucket/vec-1.index"),
            _entry(None, field_id=0, index_type="btree",
                   file_name="id-btree-0.index",
                   row_range_start=0, row_range_end=9,
                   external_path="oss://bucket/id-btree-0.index"),
        ]
        self.table = _StubTable(fields=[self.id_field, self.embedding_field],
                                entries=self.entries)
        _patch_snapshot(self, self.entries)

    def tearDown(self):
        mock.patch.stopall()

    def _builder(self, filter_pred=None):
        b = (VectorSearchBuilderImpl(self.table)
             .with_vector_column("embedding")
             .with_query_vector([1.0, 0.0, 0.0, 0.0])
             .with_limit(3))
        if filter_pred is not None:
            b = b.with_filter(filter_pred)
        return b

    def test_scan_attaches_overlapping_scalar_index_files(self):
        """``with_filter`` + scan: each vector-range split must carry the
        scalar index files whose row range overlaps it."""
        filter_pred = Predicate(method="greaterOrEqual", index=0, field="id",
                                literals=[5])
        splits = self._builder(filter_pred).new_vector_search_scan().scan().splits()

        self.assertEqual(2, len(splits))
        splits_sorted = sorted(splits, key=lambda s: s.row_range_start)
        for s in splits_sorted:
            self.assertEqual(1, len(s.vector_index_files))
            self.assertEqual(["id-btree-0.index"],
                             [f.file_name for f in s.scalar_index_files])
        self.assertEqual((0, 4),
                         (splits_sorted[0].row_range_start,
                          splits_sorted[0].row_range_end))
        self.assertEqual((5, 9),
                         (splits_sorted[1].row_range_start,
                          splits_sorted[1].row_range_end))

    def test_read_threads_prefilter_bitmap_as_include_row_ids(self):
        """preFilter bitmap from scanner.scan(filter) must reach each split's
        VectorSearch, offset-rebased to local coords by OffsetGlobalIndexReader.
        Also: the vector reader's io_meta carries external_path."""
        filter_pred = Predicate(method="greaterOrEqual", index=0, field="id",
                                literals=[5])
        scan_plan = self._builder(filter_pred).new_vector_search_scan().scan()

        bitmap = RoaringBitmap64()
        for rid in range(5, 10):
            bitmap.add(rid)
        scanner = mock.MagicMock()
        scanner.scan.return_value = GlobalIndexResult.create(bitmap)

        captured_searches = []
        captured_io_metas = []

        def _capture_create(index_type, file_io, index_path,
                            index_io_meta_list, options=None):
            captured_io_metas.append(list(index_io_meta_list))

            class _FakeReader:
                def visit_vector_search(self_inner, vs):
                    captured_searches.append(vs)
                    return _completed_future(ScoredGlobalIndexResult.create_empty())

                def close(self_inner):
                    pass

                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, *a):
                    return False
            return _FakeReader()

        with mock.patch(
                "pypaimon.globalindex.global_index_scanner.GlobalIndexScanner.create",
                return_value=scanner), \
             mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_capture_create):
            self._builder(filter_pred).new_vector_search_read().read_plan(scan_plan)

        # Pre-filter happened once with our filter.
        self.assertEqual(1, scanner.scan.call_count)
        self.assertIs(filter_pred, scanner.scan.call_args[0][0])

        # [0,4] sees empty local bitmap; [5,9] sees {0..4}.
        self.assertEqual(
            [0, 5],
            sorted(vs.include_row_ids.cardinality()
                   for vs in captured_searches))

        # Vector reader io_meta carries external_path from IndexFileMeta.
        seen_paths = {meta.external_path
                      for metas in captured_io_metas
                      for meta in metas}
        self.assertEqual(
            {"oss://bucket/vec-0.index", "oss://bucket/vec-1.index"},
            seen_paths)

    def test_read_threads_options_to_vector_search(self):
        scan_plan = self._builder().new_vector_search_scan().scan()

        captured_searches = []

        def _capture_create(index_type, file_io, index_path,
                            index_io_meta_list, options=None):
            class _FakeReader:
                def visit_vector_search(self_inner, vs):
                    captured_searches.append(vs)
                    return _completed_future(ScoredGlobalIndexResult.create_empty())

                def close(self_inner):
                    pass

            return _FakeReader()

        with mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_capture_create):
            (self._builder()
             .with_option("ivf.nprobe", "16")
             .with_options({"hnsw.ef_search": "64"})
             .new_vector_search_read()
             .read_plan(scan_plan))

        self.assertEqual(2, len(captured_searches))
        for search in captured_searches:
            self.assertEqual(
                {"ivf.nprobe": "16", "hnsw.ef_search": "64"},
                search.options,
            )

    def test_refine_factor_reranks_index_candidates_with_raw_vectors(self):
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )

        entry = _entry(None, field_id=1, index_type="ivf-pq",
                       file_name="vec.index", row_range_start=0,
                       row_range_end=2)
        table = _StubTable(fields=[self.id_field, self.embedding_field],
                           entries=[entry])
        _patch_snapshot(self, [entry])
        raw_calls = _install_raw_vector_read_builder(
            table, "embedding", {0: [0.0], 1: [10.0], 2: [20.0]})
        captured_limits = []

        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list, options=None):
            class _FakeReader:
                def visit_vector_search(self_inner, vs):
                    captured_limits.append(vs.limit)
                    approximate_scores = [(2, 100.0), (1, 50.0), (0, 1.0)]
                    return _completed_future(
                        DictBasedScoredIndexResult(dict(approximate_scores[:vs.limit])))

                def close(self_inner):
                    pass

            return _FakeReader()

        with mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_fake_create):
            result = (
                VectorSearchBuilderImpl(table)
                .with_vector_column("embedding")
                .with_query_vector([0.0])
                .with_limit(1)
                .with_option("ivf.refine_factor", "3")
                .execute_local()
            )

        self.assertEqual([3], captured_limits)
        self.assertEqual([Range(0, 2)], raw_calls["global_index_ranges"])
        self.assertEqual([0, 1, 2], raw_calls["candidate_ids"])
        self.assertEqual([0], sorted(list(result.results())))

    def test_refine_factor_one_reranks_without_expanding_candidates(self):
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )

        entry = _entry(None, field_id=1, index_type="ivf-pq",
                       file_name="vec.index", row_range_start=0,
                       row_range_end=2)
        table = _StubTable(fields=[self.id_field, self.embedding_field],
                           entries=[entry])
        _patch_snapshot(self, [entry])
        raw_calls = _install_raw_vector_read_builder(
            table, "embedding", {0: [0.0], 1: [10.0], 2: [20.0]})
        captured_limits = []

        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list, options=None):
            class _FakeReader:
                def visit_vector_search(self_inner, vs):
                    captured_limits.append(vs.limit)
                    return _completed_future(
                        DictBasedScoredIndexResult({2: 100.0}))

                def close(self_inner):
                    pass

            return _FakeReader()

        with mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_fake_create):
            result = (
                VectorSearchBuilderImpl(table)
                .with_vector_column("embedding")
                .with_query_vector([0.0])
                .with_limit(1)
                .with_option("ivf.refine_factor", "1")
                .execute_local()
            )

        self.assertEqual([1], captured_limits)
        self.assertEqual([Range(2, 2)], raw_calls["global_index_ranges"])
        self.assertEqual([2], raw_calls["candidate_ids"])
        self.assertEqual([2], sorted(list(result.results())))
        self.assertLess(result.score_getter()(2), 1.0)

    def test_refine_factor_query_options_override_table_options(self):
        from pypaimon.common.options.options import Options
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )

        class _Options:
            options = Options({"ivf.refine_factor": "2"})

        entry = _entry(None, field_id=1, index_type="ivf-pq",
                       file_name="vec.index", row_range_start=0,
                       row_range_end=9)
        table = _StubTable(fields=[self.id_field, self.embedding_field],
                           entries=[entry])
        table.options = _Options()
        _patch_snapshot(self, [entry])
        _install_raw_vector_read_builder(
            table, "embedding", {i: [float(i)] for i in range(10)})
        captured_limits = []

        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list, options=None):
            class _FakeReader:
                def visit_vector_search(self_inner, vs):
                    captured_limits.append(vs.limit)
                    return _completed_future(
                        DictBasedScoredIndexResult(
                            {i: float(i) for i in range(vs.limit)}))

                def close(self_inner):
                    pass

            return _FakeReader()

        with mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_fake_create):
            (
                VectorSearchBuilderImpl(table)
                .with_vector_column("embedding")
                .with_query_vector([0.0])
                .with_limit(1)
                .with_option("ivf.refine_factor", "3")
                .execute_local()
            )

        self.assertEqual([3], captured_limits)

    def test_refine_factor_validation(self):
        entry = _entry(None, field_id=1, index_type="ivf-pq",
                       file_name="vec.index", row_range_start=0,
                       row_range_end=2)
        table = _StubTable(fields=[self.id_field, self.embedding_field],
                           entries=[entry])
        _patch_snapshot(self, [entry])

        with self.assertRaisesRegex(ValueError, "refine factor must be positive"):
            (
                VectorSearchBuilderImpl(table)
                .with_vector_column("embedding")
                .with_query_vector([0.0])
                .with_limit(1)
                .with_option("refine_factor", "0")
                .execute_local()
            )

    def test_scanner_threads_external_path_to_btree_reader(self):
        """GlobalIndexScanner (backing _pre_filter) must thread external_path
        onto the GlobalIndexIOMeta handed to the btree reader factory."""
        from pypaimon.globalindex.global_index_scanner import GlobalIndexScanner

        scalar_file = self.entries[2].index_file

        captured_io_metas = []

        class _FakeLazyReader:
            def __init__(self_inner, key_serializer, file_io, index_path,
                         io_metas, executor=None, fallback_scan_max_size=None):
                captured_io_metas.append(list(io_metas))

            def close(self_inner):
                pass

        with mock.patch(
                "pypaimon.globalindex.btree.lazy_filtered_btree_reader.LazyFilteredBTreeReader",
                _FakeLazyReader):
            scanner = GlobalIndexScanner(
                fields=self.table.fields,
                file_io=self.table.file_io,
                index_path="/unused/index-path",
                index_files=[scalar_file],
            )
            try:
                list(scanner._evaluator._readers_function(self.id_field))
            finally:
                scanner.close()

        self.assertEqual(1, len(captured_io_metas))
        self.assertEqual("oss://bucket/id-btree-0.index",
                         captured_io_metas[0][0].external_path)

    def test_full_mode_scan_adds_raw_split_for_unindexed_vector_rows(self):
        from pypaimon.common.options.core_options import CoreOptions
        from pypaimon.common.options.options import Options
        from pypaimon.table.source.vector_search_split import (
            IndexVectorSearchSplit,
            RawVectorSearchSplit,
        )

        class _Options:
            options = Options({"global-index.search-mode": "full"})

            def global_index_search_mode(self_inner):
                return CoreOptions(self_inner.options).global_index_search_mode()

        class _Snapshots:
            def get_latest_snapshot(self_inner):
                return types.SimpleNamespace(next_row_id=10)

        table = _StubTable(fields=[self.id_field, self.embedding_field],
                           entries=[self.entries[0]])
        table.options = _Options()
        table.snapshot_manager = lambda: _Snapshots()
        self._scan_patch.stop()
        self._travel_patch.stop()
        _patch_snapshot(self, [self.entries[0]])
        self._travel_patch.stop()

        splits = (
            VectorSearchBuilderImpl(table)
            .with_vector_column("embedding")
            .with_query_vector([1.0, 0.0, 0.0, 0.0])
            .with_limit(3)
            .new_vector_search_scan()
            .scan()
            .splits()
        )

        self.assertEqual(2, len(splits))
        self.assertTrue(any(isinstance(s, IndexVectorSearchSplit)
                            for s in splits))
        raw = [s for s in splits if isinstance(s, RawVectorSearchSplit)]
        self.assertEqual(1, len(raw))
        self.assertEqual([Range(5, 9)], raw[0].row_ranges)

    def test_full_mode_scan_adds_raw_split_for_uncovered_scalar_filter(self):
        from pypaimon.common.options.core_options import CoreOptions
        from pypaimon.common.options.options import Options
        from pypaimon.table.source.vector_search_split import RawVectorSearchSplit

        class _Options:
            options = Options({"global-index.search-mode": "full"})

            def global_index_search_mode(self_inner):
                return CoreOptions(self_inner.options).global_index_search_mode()

        class _Snapshots:
            def get_latest_snapshot(self_inner):
                return types.SimpleNamespace(next_row_id=10)

        # Vector index covers all rows, but there is no scalar id index, so
        # full mode must produce a raw split for the scalar-filtered path.
        table = _StubTable(
            fields=[self.id_field, self.embedding_field],
            entries=[
                _entry(None, field_id=1,
                       index_type="lumina-vector-ann",
                       file_name="vec-all.index",
                       row_range_start=0,
                       row_range_end=9)
            ],
        )
        table.options = _Options()
        table.snapshot_manager = lambda: _Snapshots()
        self._scan_patch.stop()
        self._travel_patch.stop()
        _patch_snapshot(self, table._entries)
        self._travel_patch.stop()
        filter_pred = Predicate(method="greaterOrEqual", index=0, field="id",
                                literals=[5])

        splits = (
            VectorSearchBuilderImpl(table)
            .with_vector_column("embedding")
            .with_query_vector([1.0, 0.0, 0.0, 0.0])
            .with_limit(3)
            .with_filter(filter_pred)
            .new_vector_search_scan()
            .scan()
            .splits()
        )

        raw = [s for s in splits if isinstance(s, RawVectorSearchSplit)]
        self.assertEqual(1, len(raw))
        self.assertEqual([Range(0, 9)], raw[0].row_ranges)

    def test_scan_threads_builder_options_to_raw_split_index_type(self):
        from pypaimon.common.options.core_options import CoreOptions
        from pypaimon.common.options.options import Options
        from pypaimon.table.source.vector_search_split import RawVectorSearchSplit

        class _Options:
            options = Options({"global-index.search-mode": "full"})

            def global_index_search_mode(self_inner):
                return CoreOptions(self_inner.options).global_index_search_mode()

        class _Snapshots:
            def get_latest_snapshot(self_inner):
                return types.SimpleNamespace(next_row_id=10)

        table = _StubTable(fields=[self.id_field, self.embedding_field],
                           entries=[])
        table.options = _Options()
        table.snapshot_manager = lambda: _Snapshots()
        self._scan_patch.stop()
        self._travel_patch.stop()
        _patch_snapshot(self, [])
        self._travel_patch.stop()

        splits = (
            VectorSearchBuilderImpl(table)
            .with_vector_column("embedding")
            .with_query_vector([1.0, 0.0, 0.0, 0.0])
            .with_limit(3)
            .with_option("index-type", "ivf-flat")
            .new_vector_search_scan()
            .scan()
            .splits()
        )

        raw = [s for s in splits if isinstance(s, RawVectorSearchSplit)]
        self.assertEqual(1, len(raw))
        self.assertEqual("ivf-flat", raw[0].index_type)

    def test_scan_attaches_scalar_index_when_filter_hits_extra_field(self):
        id_name_index = _entry(None, field_id=2, index_type="btree",
                               file_name="name-id.index",
                               row_range_start=0,
                               row_range_end=9)
        id_name_index.index_file.global_index_meta.extra_field_ids = [0]
        table = _StubTable(fields=[
            self.id_field,
            self.embedding_field,
            _field(2, "name", "STRING"),
        ], entries=[
            self.entries[0],
            id_name_index,
        ])
        self._scan_patch.stop()
        self._travel_patch.stop()
        _patch_snapshot(self, table._entries)
        filter_pred = Predicate(method="equal", index=0, field="id",
                                literals=[5])

        splits = (
            VectorSearchBuilderImpl(table)
            .with_vector_column("embedding")
            .with_query_vector([1.0, 0.0, 0.0, 0.0])
            .with_limit(3)
            .with_filter(filter_pred)
            .new_vector_search_scan()
            .scan()
            .splits()
        )

        self.assertEqual(["name-id.index"],
                         [f.file_name for f in splits[0].scalar_index_files])


class VectorSearchMultiShardScalarTest(unittest.TestCase):
    """Scalar pre-filter across multiple btree shards of the same field.

    Exercises the real GlobalIndexScanner reader-construction path (with
    OffsetGlobalIndexReader + UnionGlobalIndexReader wrapping) so that:
      - Local row ids from each shard are rebased to the global row-id space
        before being unioned.
      - An empty first shard does NOT short-circuit subsequent shards.
    """

    def tearDown(self):
        mock.patch.stopall()

    def test_hit_only_in_later_shard_returns_global_row_id(self):
        from pypaimon.globalindex.global_index_result import GlobalIndexResult
        from pypaimon.globalindex.global_index_scanner import (
            GlobalIndexScanner,
        )

        id_field = _field(0, "id")
        emb_field = _field(1, "embedding", "FLOAT")

        # Two btree shards: [0,4] and [5,9]. Predicate: id == 7, which only
        # exists in shard [5,9] at local row id 2 (= 7 - 5).
        shard_a = _entry(None, field_id=0, index_type="btree",
                         file_name="id-0.index",
                         row_range_start=0, row_range_end=4).index_file
        shard_b = _entry(None, field_id=0, index_type="btree",
                         file_name="id-1.index",
                         row_range_start=5, row_range_end=9).index_file
        table = _StubTable(fields=[id_field, emb_field], entries=[])

        # Stub BTreeIndexReader: shard_a returns empty, shard_b returns {2}
        # (local row id). After Offset wrapping the scanner should emit {7}.
        class _StubBTreeReader:
            def __init__(self_inner, key_serializer, file_io, index_path,
                         io_meta):
                self_inner._file = io_meta.file_name

            def visit_equal(self_inner, literal):
                bm = RoaringBitmap64()
                if self_inner._file == "id-1.index":
                    bm.add(2)  # local offset inside [5,9]
                return GlobalIndexResult.create(bm)

            def close(self_inner):
                pass

        import struct
        wide_meta = BTreeIndexMeta(
            first_key=struct.pack('<i', 0),
            last_key=struct.pack('<i', 9),
            has_nulls=False)

        with mock.patch(
                "pypaimon.globalindex.btree.lazy_filtered_btree_reader.BTreeIndexReader",
                _StubBTreeReader):
            with mock.patch(
                    "pypaimon.globalindex.sorted_file_global_index_reader.SortedIndexFileMeta.deserialize",
                    return_value=wide_meta):
                scanner = GlobalIndexScanner(
                    fields=table.fields,
                    file_io=table.file_io,
                    index_path="/unused",
                    index_files=[shard_a, shard_b],
                )
                try:
                    result = scanner.scan(
                        Predicate(method="equal", index=0, field="id",
                                  literals=[7]))
                finally:
                    scanner.close()

        self.assertIsNotNone(result)
        hits = sorted(list(result.results()))
        # Must be the GLOBAL row id (7 = 5 + 2), not the local (2).
        # Must not be empty despite shard_a being empty (no short-circuit).
        self.assertEqual([7], hits)

    def test_extra_field_groups_are_padded_before_and(self):
        from pypaimon.globalindex.global_index_reader import GlobalIndexReader
        from pypaimon.globalindex.global_index_scanner import (
            GlobalIndexScanner,
        )

        a_field = _field(0, "a")
        b_field = _field(1, "b")
        c_field = _field(2, "c")

        short = _entry(None, field_id=0, index_type="btree",
                       file_name="a-c.index",
                       row_range_start=0, row_range_end=4).index_file
        short.global_index_meta.extra_field_ids = [2]
        long = _entry(None, field_id=1, index_type="btree",
                      file_name="b-c.index",
                      row_range_start=0, row_range_end=9).index_file
        long.global_index_meta.extra_field_ids = [2]

        class _StubReader(GlobalIndexReader):
            def __init__(self_inner, file_name):
                self_inner._file_name = file_name

            def visit_equal(self_inner, field_ref, literal):
                bm = RoaringBitmap64()
                if self_inner._file_name == "a-c.index":
                    for row_id in [1, 3, 4]:
                        bm.add(row_id)
                else:
                    for row_id in [1, 3, 7, 8]:
                        bm.add(row_id)
                return _completed_future(GlobalIndexResult.create(bm))

            def close(self_inner):
                pass

        def _stub_create_inner_readers(
                index_type, file_io, index_path, field, io_metas,
                executor=None, options=None):
            return [_StubReader(io_meta.file_name) for io_meta in io_metas]

        with mock.patch(
                "pypaimon.globalindex.global_index_scanner._create_inner_readers",
                side_effect=_stub_create_inner_readers):
            scanner = GlobalIndexScanner(
                fields=[a_field, b_field, c_field],
                file_io=object(),
                index_path="/unused",
                index_files=[short, long],
            )
            try:
                result = scanner.scan(
                    Predicate(method="equal", index=2, field="c",
                              literals=[42]))
            finally:
                scanner.close()

        self.assertIsNotNone(result)
        # The short group is all-hit padded for rows 5..9 before AND-ing with
        # the long group. Row 4 is filtered out by the long group, while tail
        # rows 7 and 8 survive because the short group has not indexed them.
        self.assertEqual([1, 3, 7, 8], sorted(list(result.results())))

    def test_extra_field_groups_pad_missing_coverage_before_and(self):
        from pypaimon.globalindex.global_index_reader import GlobalIndexReader
        from pypaimon.globalindex.global_index_scanner import (
            GlobalIndexScanner,
        )

        a_field = _field(0, "a")
        b_field = _field(1, "b")
        c_field = _field(2, "c")

        a_early = _entry(None, field_id=0, index_type="btree",
                         file_name="a-c-early.index",
                         row_range_start=2, row_range_end=3).index_file
        a_early.global_index_meta.extra_field_ids = [2]
        a_late = _entry(None, field_id=0, index_type="btree",
                        file_name="a-c-late.index",
                        row_range_start=7, row_range_end=9).index_file
        a_late.global_index_meta.extra_field_ids = [2]
        b_full = _entry(None, field_id=1, index_type="btree",
                        file_name="b-c-full.index",
                        row_range_start=0, row_range_end=9).index_file
        b_full.global_index_meta.extra_field_ids = [2]

        class _StubReader(GlobalIndexReader):
            def __init__(self_inner, file_name):
                self_inner._file_name = file_name

            def visit_equal(self_inner, field_ref, literal):
                bm = RoaringBitmap64()
                if self_inner._file_name == "a-c-early.index":
                    bm.add(0)  # global 2 after offset
                elif self_inner._file_name == "a-c-late.index":
                    for row_id in [0, 1]:  # global 7, 8 after offset
                        bm.add(row_id)
                else:
                    for row_id in [1, 2, 5, 7, 8]:
                        bm.add(row_id)
                return _completed_future(GlobalIndexResult.create(bm))

            def close(self_inner):
                pass

        def _stub_create_inner_readers(
                index_type, file_io, index_path, field, io_metas,
                executor=None, options=None):
            return [_StubReader(io_meta.file_name) for io_meta in io_metas]

        with mock.patch(
                "pypaimon.globalindex.global_index_scanner._create_inner_readers",
                side_effect=_stub_create_inner_readers):
            scanner = GlobalIndexScanner(
                fields=[a_field, b_field, c_field],
                file_io=object(),
                index_path="/unused",
                index_files=[a_early, a_late, b_full],
            )
            try:
                result = scanner.scan(
                    Predicate(method="equal", index=2, field="c",
                              literals=[42]))
            finally:
                scanner.close()

        self.assertIsNotNone(result)
        # The first group has not indexed [0,1] and [4,6], so these ranges are
        # neutral under AND. Its indexed ranges still filter normally.
        self.assertEqual([1, 2, 5, 7, 8], sorted(list(result.results())))

    def test_extra_field_padding_does_not_convert_none_to_hits(self):
        from pypaimon.globalindex.global_index_reader import GlobalIndexReader
        from pypaimon.globalindex.global_index_scanner import (
            GlobalIndexScanner,
        )

        a_field = _field(0, "a", "STRING")
        b_field = _field(1, "b", "STRING")
        c_field = _field(2, "c", "STRING")

        short = _entry(None, field_id=0, index_type="btree",
                       file_name="a-c.index",
                       row_range_start=0, row_range_end=4).index_file
        short.global_index_meta.extra_field_ids = [2]
        long = _entry(None, field_id=1, index_type="btree",
                      file_name="b-c.index",
                      row_range_start=0, row_range_end=9).index_file
        long.global_index_meta.extra_field_ids = [2]

        class _StubReader(GlobalIndexReader):
            def visit_contains(self_inner, field_ref, literal):
                return _completed_future(None)

            def close(self_inner):
                pass

        def _stub_create_inner_readers(
                index_type, file_io, index_path, field, io_metas,
                executor=None, options=None):
            return [_StubReader() for _ in io_metas]

        with mock.patch(
                "pypaimon.globalindex.global_index_scanner._create_inner_readers",
                side_effect=_stub_create_inner_readers):
            scanner = GlobalIndexScanner(
                fields=[a_field, b_field, c_field],
                file_io=object(),
                index_path="/unused",
                index_files=[short, long],
            )
            try:
                result = scanner.scan(
                    Predicate(method="contains", index=2, field="c",
                              literals=["x"]))
            finally:
                scanner.close()

        self.assertIsNone(result)

    def test_tantivy_fulltext_index_is_dispatched_by_scanner(self):
        """Non-btree scalar global indexes (tantivy-fulltext, etc.) must be
        instantiated by GlobalIndexScanner — previously only 'btree' was
        handled and everything else was silently dropped, making text-column
        pre-filter a no-op."""
        from pypaimon.globalindex.global_index_result import GlobalIndexResult
        from pypaimon.globalindex.global_index_scanner import (
            GlobalIndexScanner,
        )

        name_field = _field(0, "name", "STRING")
        emb_field = _field(1, "embedding", "FLOAT")
        tantivy_shard = _entry(
            None, field_id=0, index_type="tantivy-fulltext",
            file_name="name-ft.index",
            row_range_start=0, row_range_end=9,
            external_path="oss://bucket/name-ft.index").index_file
        table = _StubTable(fields=[name_field, emb_field], entries=[])

        captured_ctor_args = []
        visit_calls = []

        from pypaimon.globalindex.global_index_reader import _completed_future as _cf

        class _StubTantivyReader:
            def __init__(self_inner, file_io, index_path, io_metas):
                captured_ctor_args.append(
                    (file_io, index_path, list(io_metas)))

            def visit_equal(self_inner, field_ref, literal):
                visit_calls.append(("equal", literal))
                bm = RoaringBitmap64()
                bm.add(4)
                return _cf(GlobalIndexResult.create(bm))

            def close(self_inner):
                pass

        with mock.patch(
                "pypaimon.globalindex.tantivy.TantivyFullTextGlobalIndexReader",
                _StubTantivyReader):
            scanner = GlobalIndexScanner(
                fields=table.fields,
                file_io=table.file_io,
                index_path="/unused",
                index_files=[tantivy_shard],
            )
            try:
                result = scanner.scan(
                    Predicate(method="equal", index=0, field="name",
                              literals=["x"]))
            finally:
                scanner.close()

        # Tantivy reader was instantiated (it would NOT be before this fix).
        self.assertEqual(1, len(captured_ctor_args))
        _, _, io_metas = captured_ctor_args[0]
        self.assertEqual("oss://bucket/name-ft.index",
                         io_metas[0].external_path)
        # visit_equal was dispatched all the way through evaluator → union →
        # offset → stub tantivy reader.
        self.assertEqual([("equal", "x")], visit_calls)
        # Row id 4 is inside [0,9] so offset rebase is a no-op.
        self.assertEqual([4], sorted(list(result.results())))

    def test_like_predicate_is_dispatched_to_reader(self):
        """Evaluator must dispatch ``like`` to reader.visit_like — otherwise
        the pre-filter is silently skipped and vector search returns rows
        that violate the predicate."""
        from pypaimon.globalindex.global_index_result import GlobalIndexResult
        from pypaimon.globalindex.global_index_scanner import (
            GlobalIndexScanner,
        )

        name_field = _field(0, "name", "STRING")
        emb_field = _field(1, "embedding", "FLOAT")
        shard = _entry(None, field_id=0, index_type="btree",
                       file_name="name-0.index",
                       row_range_start=0, row_range_end=9).index_file
        table = _StubTable(fields=[name_field, emb_field], entries=[])

        observed_calls = []

        class _StubBTreeReader:
            def __init__(self_inner, key_serializer, file_io, index_path,
                         io_meta):
                pass

            def visit_like(self_inner, literal):
                observed_calls.append(("like", literal))
                bm = RoaringBitmap64()
                bm.add(3)  # local, will be offset-rebased to 3 (range starts at 0)
                return GlobalIndexResult.create(bm)

            def close(self_inner):
                pass

        with mock.patch(
                "pypaimon.globalindex.btree.lazy_filtered_btree_reader.BTreeIndexReader",
                _StubBTreeReader):
            with mock.patch(
                    "pypaimon.globalindex.sorted_file_global_index_reader.SortedIndexFileMeta.deserialize",
                    return_value=BTreeIndexMeta(first_key=b'', last_key=b'zzzz', has_nulls=False)):
                scanner = GlobalIndexScanner(
                    fields=table.fields,
                    file_io=table.file_io,
                    index_path="/unused",
                    index_files=[shard],
                )
                try:
                    result = scanner.scan(
                        Predicate(method="like", index=0, field="name",
                                  literals=["a_c%"]))
                finally:
                    scanner.close()

        self.assertEqual([("like", "a_c%")], observed_calls)
        self.assertIsNotNone(result)
        self.assertEqual([3], sorted(list(result.results())))

    def test_scanner_reports_unindexed_rows_for_full_mode(self):
        from pypaimon.common.options.core_options import CoreOptions
        from pypaimon.common.options.options import Options
        from pypaimon.globalindex.global_index_scanner import (
            GlobalIndexScanner,
        )

        class _Options:
            options = Options({"global-index.search-mode": "full"})

            def global_index_search_mode(self_inner):
                return CoreOptions(self_inner.options).global_index_search_mode()

            def global_index_thread_num(self_inner):
                return 32

        class _Snapshots:
            def get_latest_snapshot(self_inner):
                return types.SimpleNamespace(next_row_id=10)

        id_field = _field(0, "id")
        emb_field = _field(1, "embedding", "FLOAT")
        indexed = _entry(None, field_id=0, index_type="btree",
                         file_name="id-0.index",
                         row_range_start=0, row_range_end=4).index_file
        table = _StubTable(fields=[id_field, emb_field], entries=[])
        table.options = _Options()
        table.snapshot_manager = lambda: _Snapshots()

        scanner = GlobalIndexScanner.create(table, index_files=[indexed])
        try:
            result = scanner.unindexed_rows(
                Predicate(method="equal", index=0, field="id", literals=[7]))
        finally:
            scanner.close()

        self.assertEqual([Range(5, 9)], result.results().to_range_list())

    def test_scanner_create_selects_extra_field_indexes(self):
        from pypaimon.globalindex.global_index_scanner import (
            GlobalIndexScanner,
        )

        name_field = _field(0, "name", "STRING")
        id_field = _field(1, "id")
        emb_field = _field(2, "embedding", "FLOAT")
        multi = _entry(None, field_id=0, index_type="btree",
                       file_name="name-id.index",
                       row_range_start=0, row_range_end=9).index_file
        multi.global_index_meta.extra_field_ids = [1]
        table = _StubTable(
            fields=[name_field, id_field, emb_field],
            entries=[
                IndexManifestEntry(kind=0, partition=None,
                                   bucket=0, index_file=multi)
            ],
        )
        _patch_snapshot(self, table._entries)

        class _StubBTreeReader:
            def __init__(self_inner, key_serializer, file_io, index_path,
                         io_meta):
                pass

            def visit_equal(self_inner, literal):
                return GlobalIndexResult.create_empty()

            def close(self_inner):
                pass

        with mock.patch(
                "pypaimon.globalindex.btree.lazy_filtered_btree_reader.BTreeIndexReader",
                _StubBTreeReader):
            with mock.patch(
                    "pypaimon.globalindex.sorted_file_global_index_reader.SortedIndexFileMeta.deserialize",
                    return_value=BTreeIndexMeta(first_key=b'', last_key=b'zzzz', has_nulls=False)):
                scanner = GlobalIndexScanner.create(
                    table,
                    predicate=Predicate(method="equal", index=1, field="id",
                                        literals=[3]),
                )
                try:
                    self.assertIsNotNone(scanner)
                    readers = scanner._evaluator._readers_function(id_field)
                    self.assertTrue(readers)
                finally:
                    if scanner is not None:
                        scanner.close()


class VectorSearchPartitionedFilterTest(unittest.TestCase):
    """Partitioned-table paths: with_filter auto-split + partition-filter
    input validation."""

    def setUp(self):
        self.pt_field = _field(0, "pt")
        self.id_field = _field(1, "id")
        self.embedding_field = _field(2, "embedding", "FLOAT")

        partition_pt1 = GenericRow([1], [self.pt_field])
        partition_pt2 = GenericRow([2], [self.pt_field])
        self.entries = [
            _entry(partition_pt1, field_id=2,
                   index_type="lumina",
                   file_name="vec-pt1.index",
                   row_range_start=0, row_range_end=4),
            _entry(partition_pt2, field_id=2,
                   index_type="lumina",
                   file_name="vec-pt2.index",
                   row_range_start=5, row_range_end=9),
        ]
        self.table = _StubTable(
            fields=[self.pt_field, self.id_field, self.embedding_field],
            partition_fields=[self.pt_field],
            entries=self.entries)
        _patch_snapshot(self, self.entries)

    def tearDown(self):
        mock.patch.stopall()

    def test_with_filter_auto_splits_and_prunes_wrong_partition(self):
        """A normal full-row predicate ``pt == 2`` must (a) be auto-split
        into _partition_filter with indices re-based to the partition-only
        row, and (b) drop pt=1 entries during manifest pruning."""
        pb = PredicateBuilder(self.table.fields)
        builder = (VectorSearchBuilderImpl(self.table)
                   .with_vector_column("embedding")
                   .with_query_vector([1.0, 0.0, 0.0, 0.0])
                   .with_limit(3)
                   .with_filter(pb.equal("pt", 2)))

        # Partition filter's leaf index points into the partition-only row.
        self.assertEqual("pt", builder._partition_filter.field)
        self.assertEqual(0, builder._partition_filter.index)

        splits = builder.new_vector_search_scan().scan().splits()
        self.assertEqual(1, len(splits))
        self.assertEqual(["vec-pt2.index"],
                         [f.file_name for f in splits[0].vector_index_files])

    def test_with_partition_filter_rejects_non_partition_field(self):
        """Non-partition conjuncts would be silently dropped by the extractor,
        producing wrong results; the API must refuse them up front."""
        pb = PredicateBuilder(self.table.fields)
        builder = VectorSearchBuilderImpl(self.table)
        with self.assertRaises(ValueError) as ctx:
            builder.with_partition_filter(
                PredicateBuilder.and_predicates(
                    [pb.equal("pt", 1), pb.equal("id", 5)]))
        self.assertIn("non-partition", str(ctx.exception))


class HybridSearchBuilderTest(unittest.TestCase):

    def tearDown(self):
        mock.patch.stopall()

    def test_hybrid_search_ranks_multiple_routes(self):
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )
        from pypaimon.table.source.hybrid_search_builder import (
            HybridSearchBuilderImpl,
        )

        id_field = _field(0, "id")
        title_embedding = _field(1, "title_embedding", "FLOAT")
        body_embedding = _field(2, "body_embedding", "FLOAT")
        content = _field(3, "content", "STRING")
        table = _StubTable(
            fields=[id_field, title_embedding, body_embedding, content],
            entries=[],
        )

        captured_builders = []

        class _FakeRouteBuilder:
            def __init__(self, result):
                self._result = result
                self.calls = []

            def with_vector_column(self, name):
                self.calls.append(("vector_column", name))
                return self

            def with_query_vector(self, vector):
                self.calls.append(("query_vector", vector))
                return self

            def with_query(self, query):
                self.calls.append(("query", query))
                return self

            def with_limit(self, limit):
                self.calls.append(("limit", limit))
                return self

            def with_options(self, options):
                self.calls.append(("options", options))
                return self

            def execute_local(self):
                return self._result

        vector_result = DictBasedScoredIndexResult({1: 0.9, 2: 0.8})
        text_result = DictBasedScoredIndexResult({2: 0.7, 3: 0.6})

        def new_vector_search_builder():
            builder = _FakeRouteBuilder(vector_result)
            captured_builders.append(builder)
            return builder

        def new_full_text_search_builder():
            builder = _FakeRouteBuilder(text_result)
            captured_builders.append(builder)
            return builder

        table.new_vector_search_builder = new_vector_search_builder
        table.new_full_text_search_builder = new_full_text_search_builder

        result = (
            HybridSearchBuilderImpl(table)
            .add_vector_route(
                "title_embedding", [1.0, 0.0], 10, weight=1.0,
                options={"ivf.nprobe": "32"})
            .add_full_text_route(
                '{"match":{"column":"content","terms":"paimon search",'
                '"operator":"and"}}',
                10,
                weight=1.0)
            .with_limit(2)
            .with_rrf_ranker()
            .execute_local()
        )

        self.assertEqual(2, result.results().cardinality())
        self.assertIn(2, list(result.results()))
        self.assertGreater(result.score_getter()(2), result.score_getter()(1))
        self.assertIn(("options", {"ivf.nprobe": "32"}),
                      captured_builders[0].calls)
        self.assertEqual("paimon search",
                         captured_builders[1].calls[0][1].query)

    def test_hybrid_search_rejects_data_filter_with_full_text_route(self):
        from pypaimon.table.source.hybrid_search_builder import (
            HybridSearchBuilderImpl,
        )

        id_field = _field(0, "id")
        content = _field(1, "content", "STRING")
        table = _StubTable(fields=[id_field, content], entries=[])
        pb = PredicateBuilder(table.fields)

        builder = (
            HybridSearchBuilderImpl(table)
            .add_full_text_route(
                '{"match":{"column":"content","terms":"paimon search"}}', 10)
            .with_filter(pb.equal("id", 1))
            .with_limit(5)
        )

        with self.assertRaises(ValueError) as ctx:
            builder.route_builders()
        self.assertIn("full-text routes", str(ctx.exception))

    def test_hybrid_search_rejects_full_text_route_options(self):
        from pypaimon.table.source.hybrid_search_builder import (
            HybridSearchBuilderImpl,
        )

        id_field = _field(0, "id")
        content = _field(1, "content", "STRING")
        table = _StubTable(fields=[id_field, content], entries=[])

        with self.assertRaises(ValueError) as ctx:
            HybridSearchBuilderImpl(table).add_full_text_route(
                '{"match":{"column":"content","terms":"paimon search"}}',
                10,
                options={"some.option": "x"})
        self.assertIn("Full-text hybrid route options are not supported yet",
                      str(ctx.exception))

    def test_hybrid_search_partition_filter_prunes_full_text_route(self):
        from pypaimon.table.source.hybrid_search_builder import (
            HybridSearchBuilderImpl,
        )

        pt_field = _field(0, "pt")
        content = _field(1, "content", "STRING")
        partition_pt1 = GenericRow([1], [pt_field])
        partition_pt2 = GenericRow([2], [pt_field])
        entries = [
            _entry(partition_pt1, field_id=1, index_type="tantivy-fulltext",
                   file_name="ft-pt1.index", row_range_start=0,
                   row_range_end=4),
            _entry(partition_pt2, field_id=1, index_type="tantivy-fulltext",
                   file_name="ft-pt2.index", row_range_start=5,
                   row_range_end=9),
        ]
        table = _StubTable(
            fields=[pt_field, content],
            entries=entries,
            partition_fields=[pt_field],
        )
        _patch_snapshot(self, entries)

        pb = PredicateBuilder(table.fields)
        route_builders = (
            HybridSearchBuilderImpl(table)
            .add_full_text_route(
                '{"match":{"column":"content","terms":"paimon search"}}', 10)
            .with_filter(pb.equal("pt", 2))
            .with_limit(5)
            .route_builders()
        )
        splits = (
            route_builders[0]
            .search_builder
            .new_full_text_scan()
            .scan()
            .splits()
        )

        self.assertEqual(1, len(splits))
        self.assertEqual(
            ["ft-pt2.index"],
            [f.file_name for f in splits[0].full_text_index_files],
        )


class VectorSearchManySplitsTest(unittest.TestCase):

    def test_vector_search_with_many_splits(self):
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )
        from pypaimon.table.source.vector_search_read import VectorSearchReadImpl
        from pypaimon.table.source.vector_search_split import VectorSearchSplit

        num_splits = 1200
        embedding_field = _field(1, "embedding", "FLOAT")
        entries = [
            _entry(None, field_id=1, index_type="lumina-vector-ann",
                   file_name="vec-%d.index" % i,
                   row_range_start=i, row_range_end=i)
            for i in range(num_splits)
        ]
        table = _StubTable(fields=[embedding_field], entries=entries)
        _patch_snapshot(self, entries)

        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list, options=None):
            row_id = index_io_meta_list[0].file_name
            row_id = int(row_id.split("-")[1].split(".")[0])

            class _FakeReader:
                def visit_vector_search(self_inner, vs):
                    return _completed_future(
                        DictBasedScoredIndexResult({row_id: float(row_id)}))

                def close(self_inner):
                    pass

                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, *a):
                    return False
            return _FakeReader()

        splits = [
            VectorSearchSplit(
                row_range_start=i, row_range_end=i,
                vector_index_files=[entries[i].index_file])
            for i in range(num_splits)
        ]

        with mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_fake_create):
            reader = VectorSearchReadImpl(
                table, limit=10, vector_column=embedding_field,
                query_vector=[1.0], filter_=None)
            result = reader.read(splits)

        self.assertLessEqual(result.results().cardinality(), 10)
        self.assertEqual(result.results().cardinality(), 10)
        scores = sorted(result.score_getter()(rid) for rid in result.results())
        self.assertEqual(scores, [float(i) for i in range(1190, 1200)])

    def test_read_merges_raw_search_results(self):
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )
        from pypaimon.table.source.vector_search_read import VectorSearchReadImpl
        from pypaimon.table.source.vector_search_split import (
            IndexVectorSearchSplit,
            RawVectorSearchSplit,
        )

        embedding_field = _field(1, "embedding", "FLOAT")
        entry = _entry(None, field_id=1, index_type="lumina-vector-ann",
                       file_name="vec.index",
                       row_range_start=0, row_range_end=4)
        table = _StubTable(fields=[embedding_field], entries=[entry])

        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list, options=None):
            class _FakeReader:
                def visit_vector_search(self_inner, vs):
                    return _completed_future(
                        DictBasedScoredIndexResult({1: 0.1}))

                def close(self_inner):
                    pass

            return _FakeReader()

        split = IndexVectorSearchSplit(
            row_range_start=0,
            row_range_end=4,
            vector_index_files=[entry.index_file],
        )
        raw = RawVectorSearchSplit([Range(5, 9)], [], "lumina-vector-ann")

        with mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_fake_create):
            reader = VectorSearchReadImpl(
                table, limit=2, vector_column=embedding_field,
                query_vector=[1.0], filter_=None)
            with mock.patch.object(
                    reader,
                    "_read_raw_search",
                    return_value=DictBasedScoredIndexResult({8: 0.9})) as raw_read:
                result = reader.read([split, raw])

        raw_read.assert_called_once()
        self.assertEqual([1, 8], sorted(list(result.results())))

    def test_batch_merges_raw_search_results(self):
        from pypaimon.globalindex.global_index_reader import GlobalIndexReader
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )
        from pypaimon.table.source.vector_search_read import (
            BatchVectorSearchReadImpl,
        )
        from pypaimon.table.source.vector_search_split import (
            IndexVectorSearchSplit,
            RawVectorSearchSplit,
        )

        embedding_field = _field(1, "embedding", "FLOAT")
        entry = _entry(None, field_id=1, index_type="lumina-vector-ann",
                       file_name="vec.index",
                       row_range_start=0, row_range_end=4)
        table = _StubTable(fields=[embedding_field], entries=[entry])

        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list, options=None):
            class _FakeReader(GlobalIndexReader):
                def visit_vector_search(self_inner, vs):
                    row_id = int(vs.vector[0])
                    return _completed_future(
                        DictBasedScoredIndexResult({row_id: 0.5}))

                def close(self_inner):
                    pass
            return _FakeReader()

        split = IndexVectorSearchSplit(
            row_range_start=0,
            row_range_end=4,
            vector_index_files=[entry.index_file],
        )
        raw = RawVectorSearchSplit([Range(5, 9)], [], "lumina-vector-ann")

        with mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_fake_create):
            reader = BatchVectorSearchReadImpl(
                table, limit=5, vector_column=embedding_field,
                query_vectors=[[1.0], [2.0]], filter_=None)
            with mock.patch.object(
                    reader, "_read_raw_search",
                    return_value=DictBasedScoredIndexResult({8: 0.9})) as raw_read:
                results = reader.read_batch([split, raw])

        # The raw fallback must be merged into EACH query, not dropped.
        self.assertEqual(2, raw_read.call_count)
        self.assertEqual([1, 8], sorted(list(results[0].results())))
        self.assertEqual([2, 8], sorted(list(results[1].results())))

    def test_read_uses_empty_index_prefilter_when_scalar_index_missing(self):
        from pypaimon.table.source.vector_search_read import VectorSearchReadImpl
        from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit

        id_field = _field(0, "id")
        embedding_field = _field(1, "embedding", "FLOAT")
        entry = _entry(None, field_id=1, index_type="lumina-vector-ann",
                       file_name="vec.index",
                       row_range_start=0, row_range_end=4)
        table = _StubTable(fields=[id_field, embedding_field], entries=[entry])
        filter_pred = Predicate(method="equal", index=0, field="id",
                                literals=[3])
        split = IndexVectorSearchSplit(
            row_range_start=0,
            row_range_end=4,
            vector_index_files=[entry.index_file],
            scalar_index_files=[],
        )

        reader = VectorSearchReadImpl(
            table, limit=2, vector_column=embedding_field,
            query_vector=[1.0], filter_=filter_pred)

        pre_filters = reader._pre_filters([split])

        self.assertEqual(1, len(pre_filters))
        self.assertEqual(0, pre_filters[0].cardinality())

    def test_raw_search_uses_partition_filter_and_index_type_metric(self):
        import pyarrow as pa

        from pypaimon.table.source.vector_search_read import VectorSearchReadImpl

        id_field = _field(0, "id")
        embedding_field = _field(1, "embedding", "FLOAT")
        table = _StubTable(fields=[id_field, embedding_field], entries=[])
        partition_filter = Predicate(method="equal", index=0, field="pt",
                                     literals=[1])
        filter_pred = Predicate(method="greaterOrEqual", index=0, field="id",
                                literals=[0])
        calls = {}

        class _Plan:
            def splits(self_inner):
                return ["split"]

        class _Scan:
            def with_global_index_result(self_inner, result):
                calls["global_index_ranges"] = result.results().to_range_list()
                return self_inner

            def plan(self_inner):
                return _Plan()

        class _Read:
            def to_arrow(self_inner, splits):
                calls["splits"] = list(splits)
                return pa.table({
                    "id": pa.array([5, 6], type=pa.int32()),
                    "embedding": pa.array([[1.0], [0.0]]),
                    "_ROW_ID": pa.array([5, 6], type=pa.int64()),
                })

        class _Builder:
            def with_partition_filter(self_inner, predicate):
                calls["partition_filter"] = predicate
                return self_inner

            def with_filter(self_inner, predicate):
                calls["filter"] = predicate
                return self_inner

            def with_projection(self_inner, projection):
                calls["projection"] = list(projection)
                return self_inner

            def new_scan(self_inner):
                return _Scan()

            def new_read(self_inner):
                return _Read()

        table.new_read_builder = lambda: _Builder()
        reader = VectorSearchReadImpl(
            table,
            limit=1,
            vector_column=embedding_field,
            query_vector=[1.0],
            filter_=filter_pred,
            partition_filter=partition_filter,
            options={"ivf-flat.metric": "inner_product"},
        )

        result = reader._read_raw_search(
            [Range(5, 6)], None, [1.0], "ivf-flat")

        self.assertIs(partition_filter, calls["partition_filter"])
        self.assertIs(filter_pred, calls["filter"])
        self.assertEqual([Range(5, 6)], calls["global_index_ranges"])
        self.assertIn("_ROW_ID", calls["projection"])
        self.assertEqual(["split"], calls["splits"])
        self.assertEqual([5], sorted(list(result.results())))

    def tearDown(self):
        mock.patch.stopall()


class FullTextSearchManySplitsTest(unittest.TestCase):

    def test_full_text_read_threads_external_path_to_reader(self):
        from pypaimon.table.source.full_text_read import FullTextReadImpl
        from pypaimon.table.source.full_text_search_split import (
            FullTextSearchSplit,
        )

        text_field = _field(1, "content", "STRING")
        entry = _entry(None, field_id=1, index_type="tantivy-fulltext",
                       file_name="ft.index",
                       row_range_start=0, row_range_end=9,
                       external_path="oss://bucket/ft.index")
        table = _StubTable(fields=[text_field], entries=[entry])
        captured_io_metas = []

        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list):
            captured_io_metas.append(list(index_io_meta_list))

            class _FakeReader:
                def visit_full_text_search(self_inner, fts):
                    return _completed_future(None)

                def close(self_inner):
                    pass
            return _FakeReader()

        split = FullTextSearchSplit(
            column_name="content",
            row_range_start=0, row_range_end=9,
            full_text_index_files=[entry.index_file])

        with mock.patch(
                "pypaimon.table.source.full_text_read._create_full_text_reader",
                side_effect=_fake_create):
            reader = FullTextReadImpl(
                table, limit=10, text_column=text_field,
                query=MatchQuery("test", "content"))
            reader.read([split])

        self.assertEqual(1, len(captured_io_metas))
        self.assertEqual("oss://bucket/ft.index",
                         captured_io_metas[0][0].external_path)

    def test_full_text_search_with_many_splits(self):
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )
        from pypaimon.table.source.full_text_read import FullTextReadImpl
        from pypaimon.table.source.full_text_search_split import (
            FullTextSearchSplit,
        )

        num_splits = 1200
        text_field = _field(1, "content", "STRING")
        entries = [
            _entry(None, field_id=1, index_type="tantivy-fulltext",
                   file_name="ft-%d.index" % i,
                   row_range_start=i, row_range_end=i)
            for i in range(num_splits)
        ]
        table = _StubTable(fields=[text_field], entries=entries)
        _patch_snapshot(self, entries)

        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list):
            row_id = index_io_meta_list[0].file_name
            row_id = int(row_id.split("-")[1].split(".")[0])

            class _FakeReader:
                def visit_full_text_search(self_inner, fts):
                    return _completed_future(
                        DictBasedScoredIndexResult({row_id: float(row_id)}))

                def close(self_inner):
                    pass
            return _FakeReader()

        splits = [
            FullTextSearchSplit(
                column_name="content",
                row_range_start=i, row_range_end=i,
                full_text_index_files=[entries[i].index_file])
            for i in range(num_splits)
        ]

        with mock.patch(
                "pypaimon.table.source.full_text_read._create_full_text_reader",
                side_effect=_fake_create):
            reader = FullTextReadImpl(
                table, limit=10, text_column=text_field,
                query=MatchQuery("test", "content"))
            result = reader.read(splits)

        self.assertLessEqual(result.results().cardinality(), 10)
        self.assertEqual(result.results().cardinality(), 10)
        scores = sorted(result.score_getter()(rid) for rid in result.results())
        self.assertEqual(scores, [float(i) for i in range(1190, 1200)])

    def tearDown(self):
        mock.patch.stopall()


class BatchVectorSearchTest(unittest.TestCase):
    """Batch vector search returns one result per query vector, in input order."""

    def test_batch_returns_per_query_results_in_order(self):
        from pypaimon.globalindex.global_index_reader import GlobalIndexReader
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )
        from pypaimon.table.source.batch_vector_search_builder import (
            BatchVectorSearchBuilderImpl,
        )

        embedding_field = _field(1, "embedding", "FLOAT")
        entry = _entry(None, field_id=1, index_type="lumina-vector-ann",
                       file_name="vec.index",
                       row_range_start=0, row_range_end=99)
        table = _StubTable(fields=[embedding_field], entries=[entry])
        _patch_snapshot(self, [entry])

        # A reader implementing only single search exercises the default batch
        # fan-out; it routes each query vector to a row id derived from the
        # vector itself, so result i must reflect query_vectors[i].
        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list, options=None):
            class _FakeReader(GlobalIndexReader):
                def visit_vector_search(self_inner, vs):
                    row_id = int(vs.vector[0])
                    return _completed_future(
                        DictBasedScoredIndexResult({row_id: 1.0}))

                def close(self_inner):
                    pass
            return _FakeReader()

        query_vectors = [[10.0], [20.0], [30.0]]
        with mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_fake_create):
            results = (
                BatchVectorSearchBuilderImpl(table)
                .with_vector_column("embedding")
                .with_query_vectors(query_vectors)
                .with_limit(5)
                .execute_batch_local()
            )

        self.assertEqual(len(results), len(query_vectors))
        for i, query_vector in enumerate(query_vectors):
            expected_row = int(query_vector[0])
            self.assertTrue(results[i].results().contains(expected_row))
            self.assertEqual(results[i].results().cardinality(), 1)
        # Different query vectors yield different results.
        self.assertNotEqual(
            list(results[0].results()), list(results[1].results()))

    def test_batch_uses_reader_native_batch_when_available(self):
        from pypaimon.globalindex.global_index_reader import GlobalIndexReader
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )
        from pypaimon.table.source.batch_vector_search_builder import (
            BatchVectorSearchBuilderImpl,
        )

        embedding_field = _field(1, "embedding", "FLOAT")
        entry = _entry(None, field_id=1, index_type="lumina-vector-ann",
                       file_name="vec.index",
                       row_range_start=0, row_range_end=99)
        table = _StubTable(fields=[embedding_field], entries=[entry])
        _patch_snapshot(self, [entry])

        calls = {"single": 0, "batch": 0}

        # A reader that implements native batch must be driven through one
        # batch call per split, not per-vector single calls.
        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list, options=None):
            class _FakeReader(GlobalIndexReader):
                def visit_vector_search(self_inner, vs):
                    calls["single"] += 1
                    return _completed_future(
                        DictBasedScoredIndexResult({int(vs.vector[0]): 1.0}))

                def visit_batch_vector_search(self_inner, bvs):
                    calls["batch"] += 1
                    return _completed_future([
                        DictBasedScoredIndexResult({int(bvs.vectors[i][0]): 1.0})
                        for i in range(bvs.vector_count)
                    ])

                def close(self_inner):
                    pass
            return _FakeReader()

        query_vectors = [[10.0], [20.0], [30.0]]
        with mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_fake_create):
            results = (
                BatchVectorSearchBuilderImpl(table)
                .with_vector_column("embedding")
                .with_query_vectors(query_vectors)
                .with_limit(5)
                .execute_batch_local()
            )

        self.assertEqual(calls["batch"], 1)
        self.assertEqual(calls["single"], 0)
        self.assertEqual(len(results), len(query_vectors))
        for i, query_vector in enumerate(query_vectors):
            self.assertTrue(results[i].results().contains(int(query_vector[0])))

    def test_batch_refine_factor_reranks_each_query(self):
        from pypaimon.globalindex.global_index_reader import GlobalIndexReader
        from pypaimon.globalindex.vector_search_result import (
            DictBasedScoredIndexResult,
        )
        from pypaimon.table.source.batch_vector_search_builder import (
            BatchVectorSearchBuilderImpl,
        )

        embedding_field = _field(1, "embedding", "FLOAT")
        entry = _entry(None, field_id=1, index_type="ivf-pq",
                       file_name="vec.index",
                       row_range_start=0, row_range_end=2)
        table = _StubTable(fields=[embedding_field], entries=[entry])
        _patch_snapshot(self, [entry])
        _install_raw_vector_read_builder(
            table, "embedding", {0: [0.0], 1: [10.0], 2: [20.0]})
        captured_limits = []

        def _fake_create(index_type, file_io, index_path,
                         index_io_meta_list, options=None):
            class _FakeReader(GlobalIndexReader):
                def visit_batch_vector_search(self_inner, bvs):
                    captured_limits.append(bvs.limit)
                    approximate_scores = [(2, 100.0), (1, 50.0), (0, 1.0)]
                    return _completed_future([
                        DictBasedScoredIndexResult(
                            dict(approximate_scores[:bvs.limit]))
                        for _ in range(bvs.vector_count)
                    ])

                def close(self_inner):
                    pass

            return _FakeReader()

        with mock.patch(
                "pypaimon.table.source.vector_search_read._create_vector_reader",
                side_effect=_fake_create):
            results = (
                BatchVectorSearchBuilderImpl(table)
                .with_vector_column("embedding")
                .with_query_vectors([[0.0], [20.0]])
                .with_limit(1)
                .with_option("ivf.refine_factor", "3")
                .execute_batch_local()
            )

        self.assertEqual([3], captured_limits)
        self.assertEqual([0], sorted(list(results[0].results())))
        self.assertEqual([2], sorted(list(results[1].results())))

    def test_batch_empty_splits_returns_empty_per_query(self):
        from pypaimon.table.source.batch_vector_search_builder import (
            BatchVectorSearchBuilderImpl,
        )

        embedding_field = _field(1, "embedding", "FLOAT")
        table = _StubTable(fields=[embedding_field], entries=[])
        _patch_snapshot(self, [])

        results = (
            BatchVectorSearchBuilderImpl(table)
            .with_vector_column("embedding")
            .with_query_vectors([[1.0], [2.0]])
            .with_limit(5)
            .execute_batch_local()
        )
        self.assertEqual(len(results), 2)
        for result in results:
            self.assertEqual(result.results().cardinality(), 0)

    def tearDown(self):
        mock.patch.stopall()


if __name__ == "__main__":
    unittest.main()
