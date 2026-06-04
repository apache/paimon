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
from typing import Dict, List

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
    def _deserialize_json(data):
        config = json.loads(data.decode("utf-8"))
        stop_words = config.get("stop-words", [])
        if isinstance(stop_words, list):
            stop_words = ";".join(
                word for word in stop_words if word is not None)

        return TantivyFullTextIndexOptions(
            tokenizer=config.get("tokenizer", "default"),
            ngram_min_gram=config.get("ngram.min-gram", 2),
            ngram_max_gram=config.get("ngram.max-gram", 2),
            ngram_prefix_only=config.get("ngram.prefix-only", False),
            lower_case=config.get("lower-case", True),
            max_token_length=config.get("max-token-length", 40),
            ascii_folding=config.get("ascii-folding", False),
            stem=config.get("stem", False),
            language=config.get("language", "english"),
            remove_stop_words=config.get("remove-stop-words", False),
            stop_words=stop_words,
            with_position=config.get("with-position", True))

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

        searcher = self._searcher
        import tantivy

        query = self._parse_query(tantivy, full_text_search)

        results = searcher.search(query, limit)
        if not results.hits:
            return _completed_future(DictBasedScoredIndexResult({}))

        doc_addresses = [addr for score, addr in results.hits]
        scores = [score for score, addr in results.hits]
        row_ids = searcher.fast_field_values("row_id", doc_addresses)

        id_to_scores: Dict[int, float] = {}
        for row_id, score in zip(row_ids, scores):
            id_to_scores[row_id] = score

        return _completed_future(DictBasedScoredIndexResult(id_to_scores))

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
        if self._index_options.max_token_length != 40:
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
        query_text = full_text_search.query_text
        conjunction_by_default = full_text_search.query_operator == "and"
        if self._index_options.tokenizer != "jieba":
            if conjunction_by_default:
                return self._index.parse_query(
                    query_text, ["text"], conjunction_by_default=True)
            return self._index.parse_query(query_text, ["text"])

        tokens = self._jieba_query_tokens(query_text)
        if not tokens:
            return tantivy.Query.empty_query()

        term_queries = [
            tantivy.Query.term_query(self._schema, "text", token)
            for token in tokens
        ]
        if len(term_queries) == 1:
            return term_queries[0]
        occur = tantivy.Occur.Must if conjunction_by_default else tantivy.Occur.Should
        return tantivy.Query.boolean_query([
            (occur, query)
            for query in term_queries
        ])

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
            required_classes = ["TextAnalyzerBuilder", "Tokenizer"]
            if (self._index_options.lower_case
                    or self._index_options.max_token_length != 40
                    or self._index_options.ascii_folding
                    or self._index_options.stem
                    or self._index_options.remove_stop_words
                    or self._index_options.stop_word_list()):
                required_classes.append("Filter")
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
            filter_checks = []
            if self._index_options.max_token_length != 40:
                filter_checks.append(("remove_long", "Filter.remove_long"))
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
            for name in ("Should", "Must"):
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


def _normalize_stop_words(stop_words):
    if stop_words is None:
        return ""
    if isinstance(stop_words, list):
        return ";".join(
            word.strip()
            for word in stop_words
            if word is not None and word.strip()
        )
    return ";".join(
        word.strip()
        for word in stop_words.split(";")
        if word.strip()
    )
