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

TANTIVY_FULLTEXT_IDENTIFIER = "tantivy-fulltext"
TANTIVY_NGRAM_TOKENIZER = "paimon_ngram"
TANTIVY_JIEBA_TOKENIZER = "paimon_jieba"
_META_VERSION = 1


@dataclass(frozen=True)
class TantivyFullTextIndexOptions:
    """Tokenizer options serialized by the Java Tantivy full-text index."""

    tokenizer: str = "default"
    ngram_min_gram: int = 2
    ngram_max_gram: int = 2
    ngram_prefix_only: bool = False
    lower_case: bool = True

    @staticmethod
    def deserialize(data):
        if not data:
            return TantivyFullTextIndexOptions()

        offset = 0
        version, offset = _read_meta_int(data, offset)
        if version != _META_VERSION:
            raise ValueError(
                "Unsupported Tantivy full-text index meta version: %s" % version)

        tokenizer, offset = _read_meta_utf(data, offset)
        ngram_min_gram, offset = _read_meta_int(data, offset)
        ngram_max_gram, offset = _read_meta_int(data, offset)
        ngram_prefix_only, offset = _read_meta_bool(data, offset)
        lower_case, offset = _read_meta_bool(data, offset)

        return TantivyFullTextIndexOptions(
            tokenizer=tokenizer,
            ngram_min_gram=ngram_min_gram,
            ngram_max_gram=ngram_max_gram,
            ngram_prefix_only=ngram_prefix_only,
            lower_case=lower_case)

    def __post_init__(self):
        tokenizer = "" if self.tokenizer is None else self.tokenizer.strip().lower()
        object.__setattr__(self, "tokenizer", tokenizer)

        if tokenizer not in ("default", "ngram", "jieba"):
            raise ValueError("Unsupported Tantivy tokenizer: %s" % tokenizer)
        if self.ngram_min_gram <= 0:
            raise ValueError("ngram min gram must be positive.")
        if self.ngram_max_gram <= 0:
            raise ValueError("ngram max gram must be positive.")
        if self.ngram_min_gram > self.ngram_max_gram:
            raise ValueError(
                "ngram min gram must not be greater than max gram.")

    def tokenizer_name(self):
        if self.tokenizer == "ngram":
            return TANTIVY_NGRAM_TOKENIZER
        if self.tokenizer == "jieba":
            return TANTIVY_JIEBA_TOKENIZER
        return self.tokenizer


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

        query_text = full_text_search.query_text
        limit = full_text_search.limit

        searcher = self._searcher
        import tantivy

        query = self._parse_query(tantivy, query_text)

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

                schema_builder = tantivy.SchemaBuilder()
                schema_builder.add_unsigned_field("row_id", stored=False, indexed=True, fast=True)
                self._add_text_field(schema_builder)
                schema = schema_builder.build()

                self._schema = schema
                self._index = tantivy.Index(schema, directory=directory)
                self._register_tokenizer(tantivy, self._index)
                self._index.reload()
                self._searcher = self._index.searcher()
                self._stream = stream
            except Exception:
                stream.close()
                raise

    def _add_text_field(self, schema_builder):
        tokenizer_name = self._index_options.tokenizer_name()
        if tokenizer_name == "default":
            schema_builder.add_text_field("text", stored=False)
        else:
            schema_builder.add_text_field(
                "text", stored=False, tokenizer_name=tokenizer_name)

    def _register_tokenizer(self, tantivy, index):
        if self._index_options.tokenizer != "ngram":
            return

        analyzer_builder = tantivy.TextAnalyzerBuilder(
            tantivy.Tokenizer.ngram(
                min_gram=self._index_options.ngram_min_gram,
                max_gram=self._index_options.ngram_max_gram,
                prefix_only=self._index_options.ngram_prefix_only))
        if self._index_options.lower_case:
            analyzer_builder = analyzer_builder.filter(tantivy.Filter.lowercase())
        analyzer = analyzer_builder.build()
        index.register_tokenizer(TANTIVY_NGRAM_TOKENIZER, analyzer)

    def _parse_query(self, tantivy, query_text):
        if self._index_options.tokenizer != "jieba":
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
        return tantivy.Query.boolean_query([
            (tantivy.Occur.Should, query)
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
        if self._index_options.tokenizer not in ("ngram", "jieba"):
            return

        missing = []
        if self._index_options.tokenizer == "ngram":
            required_classes = ["TextAnalyzerBuilder", "Tokenizer"]
            if self._index_options.lower_case:
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
        if (self._index_options.tokenizer == "ngram" and tokenizer is not None
                and not hasattr(tokenizer, "ngram")):
            missing.append("Tokenizer.ngram")
        if (self._index_options.tokenizer == "ngram"
                and self._index_options.lower_case and filter_ is not None
                and not hasattr(filter_, "lowercase")):
            missing.append("Filter.lowercase")
        if self._index_options.tokenizer == "jieba" and query is not None:
            for name in ("empty_query", "term_query", "boolean_query"):
                if not hasattr(query, name):
                    missing.append("Query.%s" % name)
        if (self._index_options.tokenizer == "jieba" and occur is not None
                and not hasattr(occur, "Should")):
            missing.append("Occur.Should")
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


def _read_meta_int(data: bytes, offset: int):
    _check_meta_remaining(data, offset, 4)
    return struct.unpack_from('>i', data, offset)[0], offset + 4


def _read_meta_bool(data: bytes, offset: int):
    _check_meta_remaining(data, offset, 1)
    return data[offset] != 0, offset + 1


def _read_meta_utf(data: bytes, offset: int):
    _check_meta_remaining(data, offset, 2)
    utf_len = struct.unpack_from('>H', data, offset)[0]
    offset += 2
    _check_meta_remaining(data, offset, utf_len)
    return data[offset:offset + utf_len].decode('utf-8'), offset + utf_len


def _check_meta_remaining(data: bytes, offset: int, length: int):
    if len(data) - offset < length:
        raise ValueError("Malformed Tantivy full-text index metadata.")
