################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Full-text global index reader using Tantivy.

Reads the archive header to get file layout, then opens a Tantivy searcher
backed by a stream-based Directory. No temp files are created on disk.
"""

import os
import struct
import threading
from typing import Dict, List, Optional

from pypaimon.globalindex.global_index_reader import GlobalIndexReader, FieldRef
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.vector_search_result import (
    ScoredGlobalIndexResult,
    DictBasedScoredIndexResult,
)
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta

TANTIVY_FULLTEXT_IDENTIFIER = "tantivy-fulltext"


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
        self._searcher = None
        self._index = None
        self._stream = None

    def visit_full_text_search(self, full_text_search) -> Optional[ScoredGlobalIndexResult]:
        self._ensure_loaded()

        query_text = full_text_search.query_text
        limit = full_text_search.limit

        searcher = self._searcher
        query = self._index.parse_query(query_text, ["text"])
        results = searcher.search(query, limit)

        id_to_scores: Dict[int, float] = {}
        for score, doc_address in results.hits:
            doc = searcher.doc(doc_address)
            row_id = doc["row_id"][0]
            id_to_scores[row_id] = score

        return DictBasedScoredIndexResult(id_to_scores)

    def _ensure_loaded(self):
        if self._searcher is not None:
            return

        import tantivy

        # Open the archive stream (prefer external_path if the manifest set it).
        file_path = (self._io_meta.external_path
                     if self._io_meta.external_path
                     else os.path.join(self._index_path, self._io_meta.file_name))
        stream = self._file_io.new_input_stream(file_path)
        try:
            # Parse archive header to get file layout
            file_names, file_offsets, file_lengths = self._parse_archive_header(stream)
            directory = StreamDirectory(stream, file_names, file_offsets, file_lengths)

            # Open tantivy index from stream-backed directory
            schema_builder = tantivy.SchemaBuilder()
            schema_builder.add_unsigned_field("row_id", stored=True, indexed=True, fast=True)
            schema_builder.add_text_field("text", stored=False)
            schema = schema_builder.build()

            self._index = tantivy.Index(schema, directory=directory)
            self._index.reload()
            self._searcher = self._index.searcher()
            self._stream = stream
        except Exception:
            stream.close()
            raise

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

    def visit_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return None

    def visit_not_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return None

    def visit_less_than(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return None

    def visit_less_or_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return None

    def visit_greater_than(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return None

    def visit_greater_or_equal(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return None

    def visit_is_null(self, field_ref: FieldRef) -> Optional[GlobalIndexResult]:
        return None

    def visit_is_not_null(self, field_ref: FieldRef) -> Optional[GlobalIndexResult]:
        return None

    def visit_in(self, field_ref: FieldRef, literals: List[object]) -> Optional[GlobalIndexResult]:
        return None

    def visit_not_in(self, field_ref: FieldRef, literals: List[object]) -> Optional[GlobalIndexResult]:
        return None

    def visit_starts_with(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return None

    def visit_ends_with(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return None

    def visit_contains(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return None

    def visit_like(self, field_ref: FieldRef, literal: object) -> Optional[GlobalIndexResult]:
        return None

    def visit_between(self, field_ref: FieldRef, min_v: object, max_v: object) -> Optional[GlobalIndexResult]:
        return None

    def close(self) -> None:
        self._searcher = None
        self._index = None
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
