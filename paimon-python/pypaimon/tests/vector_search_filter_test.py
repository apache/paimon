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
from pypaimon.globalindex.vector_search_result import ScoredGlobalIndexResult
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


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


def _patch_snapshot(testcase, entries):
    """Stub IndexFileHandler.scan + snapshot resolution."""

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
        return_value=object())
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
    def term_query(schema, field_name, field_value, index_option="position"):
        return ("term", schema, field_name, field_value, index_option)

    @staticmethod
    def boolean_query(subqueries, minimum_number_should_match=None):
        return ("boolean", tuple(subqueries), minimum_number_should_match)


class _FakeOccur:
    Should = "should"
    Must = "must"


class _FakeSearchResults:
    hits = [(2.0, "addr")]


class _FakeSearcher:
    def __init__(self):
        self.query = None

    def search(self, query, limit):
        self.query = query
        return _FakeSearchResults()

    def fast_field_values(self, name, addresses):
        return [7]


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
                    FullTextSearch("中文", 10, "content")).result()
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
             ("ngram", 2, 3, True, ("lowercase",))),
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
                    FullTextSearch("hello", 5, "content")).result()
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
                    FullTextSearch("running", 10, "content")).result()
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
                    FullTextSearch("中文", 10, "content")).result()
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
                    FullTextSearch("售货员", 10, "content", "and")).result()
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
                        FullTextSearch("售货员", 10, "content")).result()
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

    def test_scanner_threads_external_path_to_btree_reader(self):
        """GlobalIndexScanner (backing _pre_filter) must thread external_path
        onto the GlobalIndexIOMeta handed to the btree reader factory."""
        from pypaimon.globalindex.global_index_scanner import GlobalIndexScanner

        scalar_file = self.entries[2].index_file

        captured_io_metas = []

        class _FakeLazyReader:
            def __init__(self_inner, key_serializer, file_io, index_path,
                         io_metas, executor=None):
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


class VectorSearchMultiShardScalarTest(unittest.TestCase):
    """Scalar pre-filter across multiple btree shards of the same field.

    Exercises the real GlobalIndexScanner reader-construction path (with
    OffsetGlobalIndexReader + UnionGlobalIndexReader wrapping) so that:
      - Local row ids from each shard are rebased to the global row-id space
        before being unioned.
      - An empty first shard does NOT short-circuit subsequent shards.
    """

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
                    "pypaimon.globalindex.btree.lazy_filtered_btree_reader.BTreeIndexMeta.deserialize",
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
                    "pypaimon.globalindex.btree.lazy_filtered_btree_reader.BTreeIndexMeta.deserialize",
                    return_value=BTreeIndexMeta(first_key=None, last_key=None, has_nulls=False)):
                scanner = GlobalIndexScanner(
                    fields=table.fields,
                    file_io=table.file_io,
                    index_path="/unused",
                    index_files=[shard],
                )
                try:
                    result = scanner.scan(
                        Predicate(method="like", index=0, field="name",
                                  literals=["abc%"]))
                finally:
                    scanner.close()

        self.assertEqual([("like", "abc%")], observed_calls)
        self.assertIsNotNone(result)
        self.assertEqual([3], sorted(list(result.results())))


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
            row_range_start=0, row_range_end=9,
            full_text_index_files=[entry.index_file])

        with mock.patch(
                "pypaimon.table.source.full_text_read._create_full_text_reader",
                side_effect=_fake_create):
            reader = FullTextReadImpl(
                table, limit=10, text_column=text_field,
                query_text="test")
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
                row_range_start=i, row_range_end=i,
                full_text_index_files=[entries[i].index_file])
            for i in range(num_splits)
        ]

        with mock.patch(
                "pypaimon.table.source.full_text_read._create_full_text_reader",
                side_effect=_fake_create):
            reader = FullTextReadImpl(
                table, limit=10, text_column=text_field,
                query_text="test")
            result = reader.read(splits)

        self.assertLessEqual(result.results().cardinality(), 10)
        self.assertEqual(result.results().cardinality(), 10)
        scores = sorted(result.score_getter()(rid) for rid in result.results())
        self.assertEqual(scores, [float(i) for i in range(1190, 1200)])

    def tearDown(self):
        mock.patch.stopall()


if __name__ == "__main__":
    unittest.main()
