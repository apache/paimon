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

"""Full-text global index reader backed by paimon-full-text."""

import json
import os
import threading
from dataclasses import dataclass
from typing import List, Mapping

from pypaimon.globalindex.global_index_reader import (
    FieldRef,
    GlobalIndexReader,
    _completed_future,
)
from pypaimon.globalindex.vector_search_result import (
    DictBasedScoredIndexResult,
)
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta

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

    def to_native_options(self):
        config = json.loads(self.to_native_config_json())
        options = {}
        for key, value in config.items():
            if isinstance(value, list):
                options[key] = ";".join(str(item) for item in value)
            elif isinstance(value, bool):
                options[key] = str(value).lower()
            else:
                options[key] = str(value)
        return options

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


class PaimonFullTextInput:
    """Positional input adapter for paimon-ftindex."""

    def __init__(self, stream):
        self._stream = stream
        self._lock = threading.Lock()

    def pread(self, pos, length):
        with self._lock:
            self._stream.seek(pos)
            return _read_fully(self._stream, length)


class TantivyFullTextGlobalIndexReader(GlobalIndexReader):
    """Full-text global index reader using paimon-full-text."""

    def __init__(self, file_io, index_path: str, io_metas: List[GlobalIndexIOMeta]):
        assert len(io_metas) == 1, "Expected exactly one index file per shard"
        self._file_io = file_io
        self._index_path = index_path
        self._io_meta = io_metas[0]
        self._index_options = TantivyFullTextIndexOptions.deserialize(
            self._io_meta.metadata)
        self._searcher = None
        self._native_reader = None
        self._stream = None
        self._load_lock = threading.Lock()

    def visit_full_text_search(self, full_text_search):
        self._ensure_loaded()

        limit = full_text_search.limit
        include_row_ids = full_text_search.include_row_ids
        if include_row_ids is not None:
            if include_row_ids.is_empty():
                return _completed_future(DictBasedScoredIndexResult({}))
        if len(full_text_search.columns) != 1:
            raise ValueError(
                "Tantivy full-text index reader expects a single-column "
                "query, got: %s" % full_text_search.columns
            )
        filter_bytes = (
            None if include_row_ids is None else include_row_ids.serialize()
        )
        row_ids, scores = self._native_reader.search(
            full_text_search.query_json(),
            limit=limit,
            filter_bytes=filter_bytes)
        id_to_scores = dict(zip(row_ids, scores))
        return _completed_future(
            DictBasedScoredIndexResult(id_to_scores).top_k(limit))

    def _ensure_loaded(self):
        if self._searcher is not None:
            return

        with self._load_lock:
            if self._searcher is not None:
                return

            file_path = (
                self._io_meta.external_path
                if self._io_meta.external_path
                else os.path.join(self._index_path, self._io_meta.file_name)
            )
            stream = self._file_io.new_input_stream(file_path)
            try:
                from paimon_ftindex import FullTextIndexReader

                self._native_reader = FullTextIndexReader(PaimonFullTextInput(stream))
                self._searcher = self._native_reader
                self._stream = stream
            except Exception:
                stream.close()
                raise

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
        if self._native_reader is not None:
            self._native_reader.close()
            self._native_reader = None
        self._searcher = None
        if self._stream is not None:
            self._stream.close()
            self._stream = None


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
