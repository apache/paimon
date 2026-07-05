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

FULL_TEXT_IDENTIFIER = "full-text"
_FULL_TEXT_OPTION_PREFIX = "full-text."


class NativeFullTextIndexOptions:
    """Native full-text options with the public ``full-text.`` prefix removed."""

    def __init__(self, options=None):
        self._options = dict(options or {})

    @staticmethod
    def deserialize(data):
        if not data:
            return NativeFullTextIndexOptions()
        return NativeFullTextIndexOptions(json.loads(data.decode("utf-8")))

    @staticmethod
    def from_options(options: Mapping[str, object]):
        config = {}
        for key, value in options.items():
            key = str(key)
            if key.startswith(_FULL_TEXT_OPTION_PREFIX):
                key = key[len(_FULL_TEXT_OPTION_PREFIX):]
                config[key] = _option_value(value)
        return NativeFullTextIndexOptions._from_config(config)

    @staticmethod
    def _from_config(config):
        return NativeFullTextIndexOptions(
            {str(key): _option_value(value) for key, value in config.items()})

    def serialize(self) -> bytes:
        return json.dumps(self._options, separators=(",", ":")).encode("utf-8")

    def to_native_options(self):
        return dict(self._options)


class PaimonFullTextInput:
    """Positional input adapter for paimon-ftindex."""

    def __init__(self, stream):
        self._stream = stream
        self._lock = threading.Lock()

    def pread(self, pos, length):
        with self._lock:
            self._stream.seek(pos)
            return _read_fully(self._stream, length)


class NativeFullTextGlobalIndexReader(GlobalIndexReader):
    """Full-text global index reader using paimon-full-text."""

    def __init__(self, file_io, index_path: str, io_metas: List[GlobalIndexIOMeta]):
        assert len(io_metas) == 1, "Expected exactly one index file per shard"
        self._file_io = file_io
        self._index_path = index_path
        self._io_meta = io_metas[0]
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
        filter_bytes = (
            None if include_row_ids is None else include_row_ids.serialize()
        )
        row_ids, scores = self._native_reader.search(
            full_text_search.query,
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


def _option_value(value) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, list):
        return ";".join(str(item) for item in value)
    return str(value)
