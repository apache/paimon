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

"""Full-text global index reader using Tantivy.

Reads the archive header to get file layout, then opens a Tantivy searcher
backed by a stream-based Directory. No temp files are created on disk.
"""

import json
import logging
import os
import struct
import threading
from dataclasses import dataclass
from typing import Dict, List, Mapping

from pypaimon.globalindex.global_index_reader import GlobalIndexReader, FieldRef, _completed_future
from pypaimon.globalindex.vector_search_result import (
    DictBasedScoredIndexResult,
)
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta

logger = logging.getLogger(__name__)

TANTIVY_FULLTEXT_IDENTIFIER = "tantivy-fulltext"
TANTIVY_NGRAM_TOKENIZER = "paimon_ngram"
TANTIVY_JIEBA_TOKENIZER = "paimon_jieba"
TANTIVY_CUSTOM_TOKENIZER = "paimon_custom"
_SUPPORTED_LANGUAGES = {
    "arabic",
    "danish",
    "dutch",
    "english",
    "finnish",
    "french",
    "german",
    "greek",
    "hungarian",
    "italian",
    "norwegian",
    "portuguese",
    "romanian",
    "russian",
    "spanish",
    "swedish",
    "tamil",
    "turkish",
}
_TANTIVY_OPTION_PREFIX = "tantivy."
_TANTIVY_OPTION_KEYS = {
    "tokenizer",
    "ngram.min-gram",
    "ngram.max-gram",
    "ngram.prefix-only",
    "lower-case",
    "max-token-length",
    "ascii-folding",
    "stem",
    "language",
    "remove-stop-words",
    "stop-words",
    "with-position",
}


@dataclass(frozen=True)
class TantivyFullTextIndexOptions:
    """Tokenizer options serialized by the Java Tantivy full-text index."""

    tokenizer: str = "default"
    ngram_min_gram: int = 2
    ngram_max_gram: int = 2
    ngram_prefix_only: bool = False
    lower_case: bool = True
    max_token_length: int = 40
    ascii_folding: bool = False
    stem: bool = False
    language: str = "english"
    remove_stop_words: bool = False
    stop_words: str = ""
    with_position: bool = True

    @staticmethod
    def deserialize(data):
        if not data:
            return TantivyFullTextIndexOptions()
        return TantivyFullTextIndexOptions._deserialize_json(data)

    @staticmethod
    def from_options(options: Mapping[str, object]):
        """Create options from table/global-index build options.

        Java's Tantivy factory strips the ``tantivy.`` prefix before creating
        TantivyFullTextIndexOptions, while users configure PyPaimon with the
        public prefixed keys. Accept both forms to keep metadata compatible.
        """

        config = {}
        for key, value in options.items():
            key = str(key)
            if key.startswith(_TANTIVY_OPTION_PREFIX):
                key = key[len(_TANTIVY_OPTION_PREFIX):]
            if key in _TANTIVY_OPTION_KEYS:
                config[key] = value
        return TantivyFullTextIndexOptions._from_config(config)

    @staticmethod
    def _deserialize_json(data):
        config = json.loads(data.decode("utf-8"))
        return TantivyFullTextIndexOptions._from_config(config)

    @staticmethod
    def _from_config(config):
        stop_words = config.get("stop-words", [])
        if isinstance(stop_words, list):
            stop_words = ";".join(
                str(word) for word in stop_words if word is not None)

        return TantivyFullTextIndexOptions(
            tokenizer=_get_string(config, "tokenizer", "default"),
            ngram_min_gram=_get_int(config, "ngram.min-gram", 2),
            ngram_max_gram=_get_int(config, "ngram.max-gram", 2),
            ngram_prefix_only=_get_bool(config, "ngram.prefix-only", False),
            lower_case=_get_bool(config, "lower-case", True),
            max_token_length=_get_int(config, "max-token-length", 40),
            ascii_folding=_get_bool(config, "ascii-folding", False),
            stem=_get_bool(config, "stem", False),
            language=_get_string(config, "language", "english"),
            remove_stop_words=_get_bool(config, "remove-stop-words", False),
            stop_words=stop_words,
            with_position=_get_bool(config, "with-position", True))

    def serialize(self) -> bytes:
        return self.to_native_config_json().encode("utf-8")

    def to_native_config_json(self) -> str:
        config = {}
        if self.tokenizer != "default":
            config["tokenizer"] = self.tokenizer
        if self.ngram_min_gram != 2:
            config["ngram.min-gram"] = self.ngram_min_gram
        if self.ngram_max_gram != 2:
            config["ngram.max-gram"] = self.ngram_max_gram
        if self.ngram_prefix_only:
            config["ngram.prefix-only"] = self.ngram_prefix_only
        if not self.lower_case:
            config["lower-case"] = self.lower_case
        if self.max_token_length != 40:
            config["max-token-length"] = self.max_token_length
        if self.ascii_folding:
            config["ascii-folding"] = self.ascii_folding
        if self.stem:
            config["stem"] = self.stem
        if self.language != "english":
            config["language"] = self.language
        if self.remove_stop_words:
            config["remove-stop-words"] = self.remove_stop_words
        stop_words = self.stop_word_list()
        if stop_words:
            config["stop-words"] = stop_words
        if not self.with_position:
            config["with-position"] = self.with_position
        return json.dumps(config, separators=(",", ":"))

    def __post_init__(self):
        tokenizer = "" if self.tokenizer is None else self.tokenizer.strip().lower()
        object.__setattr__(self, "tokenizer", tokenizer)
        language = "" if self.language is None else self.language.strip().lower()
        object.__setattr__(self, "language", language)
        object.__setattr__(self, "stop_words", _normalize_stop_words(self.stop_words))

        supported_tokenizers = ("default", "simple", "whitespace", "raw", "ngram", "jieba")
        if tokenizer not in supported_tokenizers:
            raise ValueError("Unsupported Tantivy tokenizer: %s" % tokenizer)
        if self.ngram_min_gram <= 0:
            raise ValueError("ngram min gram must be positive.")
        if self.ngram_max_gram <= 0:
            raise ValueError("ngram max gram must be positive.")
        if self.ngram_min_gram > self.ngram_max_gram:
            raise ValueError(
                "ngram min gram must not be greater than max gram.")
        if self.max_token_length <= 0:
            raise ValueError("max token length must be positive.")
        if self.language not in _SUPPORTED_LANGUAGES:
            raise ValueError("Unsupported Tantivy language: %s" % self.language)

    def tokenizer_name(self):
        if self.tokenizer == "ngram":
            return TANTIVY_NGRAM_TOKENIZER
        if self.tokenizer == "jieba":
            return TANTIVY_JIEBA_TOKENIZER
        if self._needs_custom_tokenizer():
            return TANTIVY_CUSTOM_TOKENIZER
        return self.tokenizer

    def stop_word_list(self):
        if not self.stop_words:
            return []
        return [
            word.strip()
            for word in self.stop_words.split(";")
            if word.strip()
        ]

    def _needs_custom_tokenizer(self):
        return (
            self.tokenizer in ("simple", "whitespace", "raw")
            or self.max_token_length != 40
            or not self.lower_case
            or self.ascii_folding
            or self.stem
            or self.remove_stop_words
            or bool(self.stop_word_list())
        )


class StreamDirectory:
    """Directory backed by a seekable input stream.

    Implements the Python Directory protocol expected by tantivy-py's
    support_directory branch. Reads file data on demand via seek+read
    on the underlying stream, avoiding loading the entire archive into memory.

    Thread-safe: all seek+read pairs are serialized by a lock.
    """

    def __init__(self, stream, file_names: List[str],
                 file_offsets: List[int], file_lengths: List[int]):
        self._stream = stream
        self._lock = threading.Lock()
        self._layout: Dict[str, tuple] = {}
        for name, offset, length in zip(file_names, file_offsets, file_lengths):
            self._layout[name] = (offset, length)
        # In-memory store for files written by tantivy (e.g. lock files, meta)
        self._mem_files: Dict[str, bytes] = {}
        # In-memory writers: writer_id -> (path, buffer)
        self._writers: Dict[int, tuple] = {}
        self._next_writer_id = 0

    def get_file_handle(self, path: str) -> bytes:
        if path in self._mem_files:
            return self._mem_files[path]
        if path not in self._layout:
            raise FileNotFoundError(path)
        offset, length = self._layout[path]
        with self._lock:
            self._stream.seek(offset)
            return self._read_fully(length)

    def exists(self, path: str) -> bool:
        return path in self._layout or path in self._mem_files

    def atomic_read(self, path: str) -> bytes:
        if path in self._mem_files:
            return self._mem_files[path]
        if path not in self._layout:
            raise FileNotFoundError(path)
        offset, length = self._layout[path]
        with self._lock:
            self._stream.seek(offset)
            return self._read_fully(length)

    def atomic_write(self, path: str, data: bytes) -> None:
        self._mem_files[path] = data

    def open_write(self, path: str) -> int:
        writer_id = self._next_writer_id
        self._next_writer_id += 1
        self._writers[writer_id] = (path, bytearray())
        return writer_id

    def delete(self, path: str) -> None:
        self._mem_files.pop(path, None)

    def write(self, writer_id: int, data: bytes) -> None:
        self._writers[writer_id][1].extend(data)

    def flush(self, writer_id: int) -> None:
        pass

    def terminate(self, writer_id: int) -> None:
        path, buf = self._writers.pop(writer_id)
        self._mem_files[path] = bytes(buf)

    def sync_directory(self) -> None:
        pass

    def close(self, writer_id: int) -> None:
        """Called when a writer is dropped (e.g., lock files dropped without terminate)."""
        self._writers.pop(writer_id, None)

    def _read_fully(self, length: int) -> bytes:
        buf = bytearray()
        remaining = length
        while remaining > 0:
            chunk = self._stream.read(remaining)
            if not chunk:
                raise IOError("Unexpected end of stream")
            buf.extend(chunk)
            remaining -= len(chunk)
        return bytes(buf)


class TantivyFullTextGlobalIndexReader(GlobalIndexReader):
    """Full-text global index reader using Tantivy.

    Reads the archive header to get file layout, then opens a Tantivy searcher
    backed by stream-based callbacks. No temp files are created.

    Archive format (big-endian):
    [fileCount(4)] then for each file: [nameLen(4)] [name(utf8)] [dataLen(8)] [data]
    """

    def __init__(self, file_io, index_path: str, io_metas: List[GlobalIndexIOMeta]):
        assert len(io_metas) == 1, "Expected exactly one index file per shard"
        self._file_io = file_io
        self._index_path = index_path
        self._io_meta = io_metas[0]
        self._index_options = TantivyFullTextIndexOptions.deserialize(
            self._io_meta.metadata)
        self._searcher = None
        self._index = None
        self._schema = None
        self._stream = None
        self._load_lock = threading.Lock()

    def visit_full_text_search(self, full_text_search):
        self._ensure_loaded()

        limit = full_text_search.limit

        import tantivy

        id_to_scores = self._search_full_text_query(
            tantivy, full_text_search.query, limit)
        return _completed_future(
            DictBasedScoredIndexResult(id_to_scores).top_k(limit))

    def _search_full_text_query(self, tantivy, query, limit):
        from pypaimon.globalindex.full_text_query import (
            BooleanQuery,
            BoostQuery,
            MultiMatchQuery,
        )

        if isinstance(query, BoostQuery):
            child_limit = self._child_query_limit(limit)
            positive = self._search_full_text_query(
                tantivy, query.positive, child_limit)
            negative = self._search_full_text_query(
                tantivy, query.negative, child_limit)
            return _demote_scores(positive, negative, query.negative_boost)
        if isinstance(query, BooleanQuery) and _contains_boost_query(query):
            return self._search_boolean_query(
                tantivy, query, self._child_query_limit(limit))
        if isinstance(query, MultiMatchQuery):
            raise ValueError(
                "multi_match is not supported by single-column Tantivy full-text indexes"
            )

        return self._search_tantivy_query(
            self._parse_structured_query(tantivy, query), limit)

    def _child_query_limit(self, limit):
        num_docs = getattr(self._searcher, "num_docs", None)
        if num_docs is None:
            return limit
        return max(limit, int(num_docs))

    def _search_boolean_query(self, tantivy, query, limit):
        from pypaimon.globalindex.full_text_query import Occur

        result = None
        should_results = []
        must_not_results = []
        for occur, child in query.queries:
            child_scores = self._search_full_text_query(tantivy, child, limit)
            if occur == Occur.MUST:
                result = (
                    child_scores if result is None
                    else _intersect_scores(result, child_scores)
                )
            elif occur == Occur.SHOULD:
                should_results.append(child_scores)
            else:
                must_not_results.append(child_scores)

        if should_results:
            should_result = _union_scores(should_results)
            result = (
                should_result if result is None
                else _and_with_bonus_scores(result, should_result)
            )
        if result is None:
            return {}
        for must_not in must_not_results:
            result = _remove_scores(result, must_not)
        return result

    def _search_tantivy_query(self, query, limit):
        results = self._searcher.search(query, limit)

        if not results.hits:
            return {}

        doc_addresses = [addr for score, addr in results.hits]
        scores = [score for score, addr in results.hits]
        row_ids = self._searcher.fast_field_values("row_id", doc_addresses)

        id_to_scores: Dict[int, float] = {}
        for row_id, score in zip(row_ids, scores):
            id_to_scores[row_id] = score
        return id_to_scores

    def _ensure_loaded(self):
        if self._searcher is not None:
            return

        with self._load_lock:
            if self._searcher is not None:
                return

            import tantivy

            self._verify_tantivy_tokenizer_api(tantivy)
            file_path = (self._io_meta.external_path
                         if self._io_meta.external_path
                         else os.path.join(self._index_path, self._io_meta.file_name))
            stream = self._file_io.new_input_stream(file_path)
            try:
                file_names, file_offsets, file_lengths = self._parse_archive_header(stream)
                directory = StreamDirectory(stream, file_names, file_offsets, file_lengths)

                schema = self._build_schema(tantivy)
                try:
                    self._index = tantivy.Index(
                        schema, directory=directory,
                    )
                except ValueError as e:
                    if "schema does not match" not in str(e):
                        raise
                    logger.warning(
                        "Schema mismatch, retrying with "
                        "row_id stored=true"
                    )
                    schema = self._build_schema(
                        tantivy, row_id_stored=True,
                    )
                    self._index = tantivy.Index(
                        schema, directory=directory,
                    )
                self._schema = schema
                self._register_tokenizer(tantivy, self._index)
                self._index.reload()
                self._searcher = self._index.searcher()
                self._stream = stream
            except Exception:
                stream.close()
                raise

    def _build_schema(self, tantivy, row_id_stored=False):
        schema_builder = tantivy.SchemaBuilder()
        schema_builder.add_unsigned_field(
            "row_id", stored=row_id_stored, indexed=True, fast=True,
        )
        tokenizer_name = self._index_options.tokenizer_name()
        field_kwargs = {}
        if not self._index_options.with_position:
            field_kwargs["index_option"] = "freq"
        if tokenizer_name == "default":
            schema_builder.add_text_field(
                "text", stored=False, **field_kwargs,
            )
        else:
            schema_builder.add_text_field(
                "text", stored=False,
                tokenizer_name=tokenizer_name, **field_kwargs,
            )
        return schema_builder.build()

    def _register_tokenizer(self, tantivy, index):
        if (self._index_options.tokenizer == "default"
                and self._index_options.tokenizer_name() == "default"):
            return

        if self._index_options.tokenizer == "ngram":
            tokenizer = tantivy.Tokenizer.ngram(
                min_gram=self._index_options.ngram_min_gram,
                max_gram=self._index_options.ngram_max_gram,
                prefix_only=self._index_options.ngram_prefix_only)
        elif self._index_options.tokenizer in ("default", "simple"):
            tokenizer = tantivy.Tokenizer.simple()
        elif self._index_options.tokenizer == "whitespace":
            tokenizer = tantivy.Tokenizer.whitespace()
        elif self._index_options.tokenizer == "raw":
            tokenizer = tantivy.Tokenizer.raw()
        else:
            return

        analyzer_builder = tantivy.TextAnalyzerBuilder(tokenizer)
        analyzer_builder = analyzer_builder.filter(
            tantivy.Filter.remove_long(self._index_options.max_token_length))
        if self._index_options.lower_case:
            analyzer_builder = analyzer_builder.filter(tantivy.Filter.lowercase())
        if self._index_options.ascii_folding:
            analyzer_builder = analyzer_builder.filter(tantivy.Filter.ascii_fold())
        if self._index_options.stem:
            analyzer_builder = analyzer_builder.filter(
                tantivy.Filter.stemmer(self._index_options.language))
        if self._index_options.remove_stop_words:
            analyzer_builder = analyzer_builder.filter(
                tantivy.Filter.stopword(self._index_options.language))
        stop_words = self._index_options.stop_word_list()
        if stop_words:
            analyzer_builder = analyzer_builder.filter(
                tantivy.Filter.custom_stopword(stop_words))
        analyzer = analyzer_builder.build()
        index.register_tokenizer(self._index_options.tokenizer_name(), analyzer)

    def _parse_query(self, tantivy, full_text_search):
        return self._parse_structured_query(tantivy, full_text_search.query)

    def _parse_structured_query(self, tantivy, query):
        from pypaimon.globalindex.full_text_query import (
            BooleanQuery,
            BoostQuery,
            MatchQuery,
            MultiMatchQuery,
            Occur,
            PhraseQuery,
        )

        if isinstance(query, MatchQuery):
            return self._parse_match_query(tantivy, query)
        if isinstance(query, PhraseQuery):
            if self._index_options.tokenizer == "jieba":
                raise ValueError("phrase query is not supported for jieba tokenizer indexes")
            escaped = query.query.replace("\\", "\\\\").replace('"', '\\"')
            query_text = '"%s"' % escaped
            if query.slop != 0:
                query_text = "%s~%s" % (query_text, query.slop)
            return self._index.parse_query(query_text, ["text"])
        if isinstance(query, BooleanQuery):
            subqueries = []
            for occur, child in query.queries:
                if occur == Occur.SHOULD:
                    tantivy_occur = tantivy.Occur.Should
                elif occur == Occur.MUST:
                    tantivy_occur = tantivy.Occur.Must
                else:
                    tantivy_occur = getattr(tantivy.Occur, "MustNot", None)
                    if tantivy_occur is None:
                        raise RuntimeError(
                            "PyPaimon Tantivy full-text search requires a tantivy-py "
                            "version with Occur.MustNot support for boolean must_not queries."
                        )
                subqueries.append(
                    (tantivy_occur, self._parse_structured_query(tantivy, child)))
            if not subqueries:
                raise ValueError("boolean query must contain at least one clause")
            return tantivy.Query.boolean_query(subqueries)
        if isinstance(query, BoostQuery):
            raise ValueError(
                "boost query must be evaluated through search, not parsed as a Tantivy query"
            )
        if isinstance(query, MultiMatchQuery):
            raise ValueError(
                "multi_match is not supported by single-column Tantivy full-text indexes"
            )
        raise ValueError("Unsupported full-text query type: %s" % type(query).__name__)

    def _parse_match_query(self, tantivy, query):
        if query.max_expansions != 50:
            raise ValueError(
                "match query max_expansions is not supported by Tantivy 0.22"
            )
        if query.prefix_length != 0:
            raise ValueError(
                "match query prefix_length is not supported by Tantivy 0.22"
            )
        conjunction_by_default = query.operator.value == "AND"
        if self._index_options.tokenizer != "jieba":
            parse_kwargs = {}
            if conjunction_by_default:
                parse_kwargs["conjunction_by_default"] = True
            if query.fuzziness not in (None, 0):
                parse_kwargs["fuzzy_fields"] = {
                    "text": (False, int(query.fuzziness), True)
                }
            parsed = self._parse_index_query(
                tantivy, query.query, parse_kwargs)
            return self._boost_query(tantivy, parsed, query.boost)

        tokens = self._jieba_query_tokens(query.query)
        if not tokens:
            return tantivy.Query.empty_query()

        term_queries = []
        for token in tokens:
            if query.fuzziness not in (None, 0):
                fuzzy_query = getattr(tantivy.Query, "fuzzy_term_query", None)
                if fuzzy_query is None:
                    raise RuntimeError(
                        "PyPaimon Tantivy full-text search requires a tantivy-py "
                        "version with Query.fuzzy_term_query support for "
                        "match query fuzziness."
                    )
                term_queries.append(
                    fuzzy_query(
                        self._schema,
                        "text",
                        token,
                        distance=int(query.fuzziness),
                        transposition_cost_one=True,
                        prefix=False,
                    )
                )
            else:
                term_queries.append(
                    tantivy.Query.term_query(self._schema, "text", token))
        if len(term_queries) == 1:
            return self._boost_query(tantivy, term_queries[0], query.boost)
        occur = tantivy.Occur.Must if conjunction_by_default else tantivy.Occur.Should
        parsed = tantivy.Query.boolean_query([
            (occur, query)
            for query in term_queries
        ])
        return self._boost_query(tantivy, parsed, query.boost)

    def _parse_index_query(self, tantivy, query_text, parse_kwargs):
        try:
            return self._index.parse_query(query_text, ["text"], **parse_kwargs)
        except TypeError as e:
            if "fuzzy_fields" in parse_kwargs:
                raise RuntimeError(
                    "PyPaimon Tantivy full-text search requires a tantivy-py "
                    "version with Index.parse_query fuzzy_fields support for "
                    "match query fuzziness."
                ) from e
            raise

    @staticmethod
    def _boost_query(tantivy, parsed_query, boost):
        if boost == 1.0:
            return parsed_query
        boost_query = getattr(tantivy.Query, "boost_query", None)
        if boost_query is None:
            raise RuntimeError(
                "PyPaimon Tantivy full-text search requires a tantivy-py "
                "version with Query.boost_query support for match query boost."
            )
        return boost_query(parsed_query, float(boost))

    def _jieba_query_tokens(self, query_text):
        try:
            import jieba
        except ImportError as e:
            raise RuntimeError(
                "PyPaimon Tantivy full-text search requires Python package "
                "'jieba' to query jieba tokenizer indexes. Install it with "
                "`pip install jieba`.") from e

        seen = set()
        tokens = []
        for word, _, _ in jieba.tokenize(query_text, mode="search", HMM=True):
            token = word.strip()
            if self._index_options.lower_case:
                token = token.lower()
            if token and token not in seen:
                seen.add(token)
                tokens.append(token)
        return tokens

    def _verify_tantivy_tokenizer_api(self, tantivy):
        if (self._index_options.tokenizer == "default"
                and self._index_options.tokenizer_name() == "default"):
            return

        missing = []
        if self._index_options.tokenizer != "jieba":
            required_classes = ["TextAnalyzerBuilder", "Tokenizer", "Filter"]
        else:
            required_classes = ["Query", "Occur"]
        for name in required_classes:
            if not hasattr(tantivy, name):
                missing.append(name)

        tokenizer = getattr(tantivy, "Tokenizer", None)
        filter_ = getattr(tantivy, "Filter", None)
        query = getattr(tantivy, "Query", None)
        occur = getattr(tantivy, "Occur", None)
        tokenizer_apis = {
            "default": "simple",
            "ngram": "ngram",
            "simple": "simple",
            "whitespace": "whitespace",
            "raw": "raw",
        }
        tokenizer_api = tokenizer_apis.get(self._index_options.tokenizer)
        if (tokenizer_api is not None and tokenizer is not None
                and not hasattr(tokenizer, tokenizer_api)):
            missing.append("Tokenizer.%s" % tokenizer_api)
        if self._index_options.tokenizer != "jieba" and filter_ is not None:
            filter_checks = [("remove_long", "Filter.remove_long")]
            if self._index_options.lower_case:
                filter_checks.append(("lowercase", "Filter.lowercase"))
            if self._index_options.ascii_folding:
                filter_checks.append(("ascii_fold", "Filter.ascii_fold"))
            if self._index_options.stem:
                filter_checks.append(("stemmer", "Filter.stemmer"))
            if self._index_options.remove_stop_words:
                filter_checks.append(("stopword", "Filter.stopword"))
            if self._index_options.stop_word_list():
                filter_checks.append(("custom_stopword", "Filter.custom_stopword"))
            for attr, api_name in filter_checks:
                if not hasattr(filter_, attr):
                    missing.append(api_name)
        if self._index_options.tokenizer == "jieba" and query is not None:
            for name in ("empty_query", "term_query", "boolean_query"):
                if not hasattr(query, name):
                    missing.append("Query.%s" % name)
        if self._index_options.tokenizer == "jieba" and occur is not None:
            for name in ("Should", "Must", "MustNot"):
                if not hasattr(occur, name):
                    missing.append("Occur.%s" % name)
        if missing:
            tokenizer_name = self._index_options.tokenizer
            raise RuntimeError(
                "PyPaimon Tantivy full-text search requires a tantivy-py "
                "version with %s tokenizer support. Missing API(s): %s"
                % (tokenizer_name, ", ".join(missing)))

    @staticmethod
    def _parse_archive_header(stream):
        """Parse the archive header to extract file names, offsets, and lengths.

        Only reads the header; does not load file data into memory.
        Computes the absolute byte offset of each file's data within the stream.
        """
        file_count = _read_int(stream)
        file_names = []
        file_offsets = []
        file_lengths = []

        for _ in range(file_count):
            name_len = _read_int(stream)
            name_bytes = _read_fully(stream, name_len)
            file_names.append(name_bytes.decode('utf-8'))

            data_len = _read_long(stream)
            data_offset = stream.tell()
            file_offsets.append(data_offset)
            file_lengths.append(data_len)

            # Skip past the file data
            stream.seek(data_offset + data_len)

        return file_names, file_offsets, file_lengths

    # =================== unsupported =====================

    def visit_equal(self, field_ref: FieldRef, literal: object):
        return _completed_future(None)

    def visit_not_equal(self, field_ref: FieldRef, literal: object):
        return _completed_future(None)

    def visit_less_than(self, field_ref: FieldRef, literal: object):
        return _completed_future(None)

    def visit_less_or_equal(self, field_ref: FieldRef, literal: object):
        return _completed_future(None)

    def visit_greater_than(self, field_ref: FieldRef, literal: object):
        return _completed_future(None)

    def visit_greater_or_equal(self, field_ref: FieldRef, literal: object):
        return _completed_future(None)

    def visit_is_null(self, field_ref: FieldRef):
        return _completed_future(None)

    def visit_is_not_null(self, field_ref: FieldRef):
        return _completed_future(None)

    def visit_in(self, field_ref: FieldRef, literals: List[object]):
        return _completed_future(None)

    def visit_not_in(self, field_ref: FieldRef, literals: List[object]):
        return _completed_future(None)

    def visit_starts_with(self, field_ref: FieldRef, literal: object):
        return _completed_future(None)

    def visit_ends_with(self, field_ref: FieldRef, literal: object):
        return _completed_future(None)

    def visit_contains(self, field_ref: FieldRef, literal: object):
        return _completed_future(None)

    def visit_like(self, field_ref: FieldRef, literal: object):
        return _completed_future(None)

    def visit_between(self, field_ref: FieldRef, min_v: object, max_v: object):
        return _completed_future(None)

    def close(self) -> None:
        self._searcher = None
        self._index = None
        self._schema = None
        if self._stream is not None:
            self._stream.close()
            self._stream = None


def _read_int(stream) -> int:
    data = _read_fully(stream, 4)
    return struct.unpack('>i', data)[0]


def _read_long(stream) -> int:
    data = _read_fully(stream, 8)
    return struct.unpack('>q', data)[0]


def _read_fully(stream, length: int) -> bytes:
    buf = bytearray()
    remaining = length
    while remaining > 0:
        chunk = stream.read(remaining)
        if not chunk:
            raise IOError("Unexpected end of stream")
        buf.extend(chunk)
        remaining -= len(chunk)
    return bytes(buf)


def _get_string(config, key, default):
    value = config.get(key)
    return default if value is None else str(value)


def _get_int(config, key, default):
    value = config.get(key)
    if value is None:
        return default
    return int(value)


def _get_bool(config, key, default):
    value = config.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def _normalize_stop_words(stop_words):
    if stop_words is None:
        return ""
    if isinstance(stop_words, list):
        return ";".join(
            str(word).strip()
            for word in stop_words
            if word is not None and str(word).strip()
        )
    return ";".join(
        word.strip()
        for word in stop_words.split(";")
        if word.strip()
    )


def _contains_boost_query(query):
    from pypaimon.globalindex.full_text_query import BooleanQuery, BoostQuery

    if isinstance(query, BoostQuery):
        return True
    if isinstance(query, BooleanQuery):
        return any(_contains_boost_query(child) for _, child in query.queries)
    return False


def _demote_scores(positive, negative, negative_boost):
    return {
        row_id: score * (negative_boost if row_id in negative else 1.0)
        for row_id, score in positive.items()
    }


def _intersect_scores(left, right):
    return {
        row_id: left[row_id] + right[row_id]
        for row_id in left.keys() & right.keys()
    }


def _and_with_bonus_scores(base, bonus):
    return {
        row_id: score + bonus.get(row_id, 0.0)
        for row_id, score in base.items()
    }


def _union_scores(results):
    merged = {}
    for result in results:
        for row_id, score in result.items():
            merged[row_id] = merged.get(row_id, 0.0) + score
    return merged


def _remove_scores(left, right):
    return {
        row_id: score
        for row_id, score in left.items()
        if row_id not in right
    }
