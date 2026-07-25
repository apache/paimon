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

"""Full-text global index writer backed by paimon-full-text."""

from typing import List, Mapping

from pypaimon.globalindex.index_file_utils import new_global_index_file_name
from pypaimon.globalindex.result_entry import ResultEntry
from pypaimon.globalindex.full_text.native_full_text_global_index_reader import (
    NativeFullTextIndexOptions,
)
from pypaimon.schema.data_types import AtomicType, DataType


_FILE_NAME_PREFIX = "full-text"


class NativeFullTextIndexWriter:
    """Writer for one native full-text global index file.

    Row IDs are local to the manifest row range, matching the Java generic
    global index writer contract. Null text values increase row_count but are
    not added to the native full-text index.
    """

    def __init__(
        self,
        file_io,
        index_path: str,
        data_type: DataType,
        options: Mapping[str, object],
    ):
        validate_text_type(data_type)
        self.file_name = new_global_index_file_name(_FILE_NAME_PREFIX)
        self._file_io = file_io
        self._index_path = index_path.rstrip("/")
        self._index_options = NativeFullTextIndexOptions.from_options(options)
        self._row_count = 0
        self._native_writer = None
        self._closed = False

    @property
    def index_options(self) -> NativeFullTextIndexOptions:
        return self._index_options

    def write(self, text, relative_row_id: int) -> None:
        if self._closed:
            raise RuntimeError("NativeFullTextIndexWriter is already closed.")

        self._row_count += 1
        if text is None:
            return

        text = _materialize_text(text)
        self._ensure_writer()
        self._add_document(int(relative_row_id), text)

    def finish(self) -> List[ResultEntry]:
        if self._closed:
            raise RuntimeError("NativeFullTextIndexWriter is already closed.")
        self._closed = True

        file_path = self._file_path()
        try:
            if self._row_count == 0:
                return []

            self._ensure_writer()

            self._file_io.check_or_mkdirs(self._index_path)
            with self._file_io.new_output_stream(file_path) as output_stream:
                self._native_writer.write(output_stream)
        except Exception:
            self._file_io.delete_quietly(file_path)
            raise
        finally:
            if self._native_writer is not None:
                self._native_writer.close()
                self._native_writer = None

        return [
            ResultEntry(
                self.file_name,
                self._row_count,
                self._index_options.serialize(),
            )
        ]

    def close(self) -> None:
        if not self._closed:
            self._closed = True
        if self._native_writer is not None:
            self._native_writer.close()
            self._native_writer = None

    def _file_path(self) -> str:
        return "%s/%s" % (self._index_path, self.file_name)

    def _ensure_writer(self) -> None:
        if self._native_writer is not None:
            return

        try:
            from paimon_ftindex import FullTextIndexWriter
        except ImportError as e:
            raise ImportError(
                "paimon-ftindex is required to build full-text indexes. "
                "Install paimon-ftindex==0.1.0 or pypaimon[full-text]."
            ) from e

        self._native_writer = FullTextIndexWriter(
            self._index_options.to_native_options())

    def _add_document(self, row_id: int, text: str) -> None:
        self._native_writer.add_document(row_id, text)


def validate_text_type(data_type: DataType) -> None:
    if not isinstance(data_type, AtomicType):
        raise ValueError(
            "Native full-text index requires string type, but got: %s"
            % data_type
        )

    type_name = data_type.type.upper()
    if (
        type_name == "STRING"
        or type_name.startswith("CHAR")
        or type_name.startswith("VARCHAR")
    ):
        return
    raise ValueError(
        "Native full-text index requires string type, but got: %s"
        % data_type
    )


def _materialize_text(value) -> str:
    if hasattr(value, "as_py"):
        value = value.as_py()
    if not isinstance(value, str):
        raise ValueError("Unsupported field type: %s" % type(value).__name__)
    return value
