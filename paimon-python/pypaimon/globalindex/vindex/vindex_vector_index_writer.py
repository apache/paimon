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

"""paimon-vindex global index writer."""

import math
import os
import tempfile
import uuid
from array import array
from typing import BinaryIO, Dict, List, Mapping, Optional

from pypaimon.globalindex.result_entry import ResultEntry
from pypaimon.schema.data_types import ArrayType, AtomicType, DataType, VectorType


FILE_NAME_PREFIX = "vector"
DEFAULT_DIMENSION = 128
ADD_BATCH_SIZE = 10000


class VindexVectorIndexWriter:
    """Writer for one paimon-vindex global index file."""

    def __init__(
        self,
        file_io,
        index_path: str,
        data_type: DataType,
        index_type: str,
        options: Mapping[str, object],
        field_name: str,
    ):
        self.file_name = (
            "%s-%s-global-index-%s.index"
            % (FILE_NAME_PREFIX, index_type, uuid.uuid4())
        )
        self._file_io = file_io
        self._index_path = index_path.rstrip('/')
        self._index_type = index_type
        self._native_options = native_options(
            data_type, options, index_type, field_name)
        self._dimension = int(self._native_options["dimension"])
        self._row_count = 0
        self._vector_count = 0
        self._row_id_temp: Optional[BinaryIO] = None
        self._vector_temp: Optional[BinaryIO] = None
        self._row_id_temp_path: Optional[str] = None
        self._vector_temp_path: Optional[str] = None
        self._closed = False

        validate_vector_type(data_type)

    @property
    def native_options(self) -> Dict[str, str]:
        return dict(self._native_options)

    def write(self, vector, relative_row_id: int) -> None:
        if self._closed:
            raise RuntimeError("VindexVectorIndexWriter is already closed.")

        self._row_count += 1
        if vector is None:
            return

        materialized = _materialize_vector(
            vector, self._dimension, relative_row_id)
        self._ensure_temp_files()
        self._row_id_temp.write(array("q", [int(relative_row_id)]).tobytes())
        self._vector_temp.write(array("f", materialized).tobytes())
        self._vector_count += 1

    def finish(self) -> List[ResultEntry]:
        if self._closed:
            raise RuntimeError("VindexVectorIndexWriter is already closed.")
        self._closed = True

        file_path = self._file_path()
        try:
            if self._vector_count == 0:
                return []

            try:
                import numpy as np
                from paimon_vindex import VectorIndexWriter
            except ImportError as e:
                raise ImportError(
                    "paimon-vindex is required to build vindex vector indexes. "
                    "Install paimon-vindex==0.1.0 or pypaimon[vindex].") from e

            self._close_temp_files()
            self._file_io.check_or_mkdirs(self._index_path)
            vectors = np.fromfile(
                self._vector_temp_path,
                dtype=np.float32,
                count=self._vector_count * self._dimension,
            ).reshape(self._vector_count, self._dimension)
            with VectorIndexWriter(self._native_options) as writer:
                writer.train(vectors)
                del vectors
                self._add_vectors_in_batches(np, writer)
                with self._file_io.new_output_stream(file_path) as output_stream:
                    writer.write(output_stream)
        except Exception:
            self._file_io.delete_quietly(file_path)
            raise
        finally:
            self._delete_temp_files()

        return [ResultEntry(self.file_name, self._row_count, b"{}")]

    def _file_path(self) -> str:
        return "%s/%s" % (self._index_path, self.file_name)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
        self._delete_temp_files()

    def _ensure_temp_files(self) -> None:
        if self._row_id_temp is not None:
            return

        row_id_temp = tempfile.NamedTemporaryFile(
            prefix="paimon-vindex-row-ids-", suffix=".bin", delete=False)
        vector_temp = tempfile.NamedTemporaryFile(
            prefix="paimon-vindex-vectors-", suffix=".bin", delete=False)
        self._row_id_temp = row_id_temp
        self._vector_temp = vector_temp
        self._row_id_temp_path = row_id_temp.name
        self._vector_temp_path = vector_temp.name

    def _close_temp_files(self) -> None:
        for temp_file in (self._row_id_temp, self._vector_temp):
            if temp_file is not None and not temp_file.closed:
                temp_file.flush()
                temp_file.close()
        self._row_id_temp = None
        self._vector_temp = None

    def _delete_temp_files(self) -> None:
        self._close_temp_files()
        for path in (self._row_id_temp_path, self._vector_temp_path):
            if path is not None and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
        self._row_id_temp_path = None
        self._vector_temp_path = None

    def _add_vectors_in_batches(self, np, writer) -> None:
        with open(self._row_id_temp_path, "rb") as row_id_file, open(
            self._vector_temp_path, "rb"
        ) as vector_file:
            remaining = self._vector_count
            while remaining > 0:
                batch_size = min(ADD_BATCH_SIZE, remaining)
                row_ids = np.fromfile(
                    row_id_file, dtype=np.int64, count=batch_size)
                vectors = np.fromfile(
                    vector_file,
                    dtype=np.float32,
                    count=batch_size * self._dimension,
                ).reshape(batch_size, self._dimension)
                writer.add_vectors(row_ids, vectors)
                remaining -= batch_size


def native_options(
    data_type: DataType,
    options: Mapping[str, object],
    index_type: str,
    field_name: str,
) -> Dict[str, str]:
    result: Dict[str, str] = {}
    option_prefix = "%s." % index_type
    field_prefix = "fields.%s." % field_name

    for key, value in options.items():
        key = str(key)
        if key.startswith(option_prefix):
            native_key = _native_option_key(key[len(option_prefix):])
            if native_key is not None:
                result[native_key] = str(value)

    for key, value in options.items():
        key = str(key)
        if key.startswith(field_prefix):
            native_key = _native_option_key(key[len(field_prefix):])
            if native_key is not None:
                result[native_key] = str(value)

    result["index.type"] = index_type.replace('-', '_')
    result["dimension"] = str(_dimension(data_type, result, index_type))
    return result


def validate_vector_type(data_type: DataType) -> None:
    if isinstance(data_type, VectorType):
        element_type = data_type.element
        if _is_float_type(element_type):
            return
        raise ValueError(
            "Vector index requires float vector, but got: %s" % element_type)

    if isinstance(data_type, ArrayType):
        element_type = data_type.element
        if _is_float_type(element_type):
            return
        raise ValueError(
            "Vector index requires float array, but got: %s" % element_type)

    raise ValueError(
        "Vector index requires VectorType or ArrayType<FLOAT>, but got: %s"
        % data_type)


def _native_option_key(option_key: str) -> Optional[str]:
    if option_key in ("index.dimension", "dimension"):
        return "dimension"
    if option_key in ("distance.metric", "metric"):
        return "metric"
    if option_key in (
        "nlist",
        "pq.m",
        "hnsw.m",
        "hnsw.ef-construction",
        "hnsw.max-level",
    ):
        return option_key
    if option_key in ("pq.use-opq", "use-opq"):
        return "use-opq"
    return None


def _dimension(
    data_type: DataType, native_options_map: Mapping[str, str], index_type: str
) -> int:
    if isinstance(data_type, VectorType):
        return data_type.length

    dimension = native_options_map.get("dimension")
    value = DEFAULT_DIMENSION if dimension is None else int(dimension)
    if value <= 0:
        raise ValueError(
            "Invalid value for '%s.dimension': %s. Must be a positive integer."
            % (index_type, value)
        )
    return value


def _is_float_type(data_type: DataType) -> bool:
    return (
        isinstance(data_type, AtomicType)
        and data_type.type.upper() == "FLOAT"
    )


def _materialize_vector(
    value, dimension: int, relative_row_id: int
) -> List[float]:
    if hasattr(value, "as_py"):
        value = value.as_py()
    if hasattr(value, "tolist"):
        value = value.tolist()
    if not isinstance(value, (list, tuple)):
        raise ValueError("Unsupported vector value: %s" % type(value).__name__)
    if len(value) != dimension:
        raise ValueError(
            "Vector dimension mismatch: expected %d, but got %d"
            % (dimension, len(value))
        )

    vector = []
    for index, element in enumerate(value):
        if element is None:
            raise ValueError("Vector element at index %d is null" % index)
        element = float(element)
        if not math.isfinite(element):
            raise ValueError(
                "Vector element at rowId=%d, index=%d is %s"
                % (relative_row_id, index, element)
            )
        vector.append(element)
    return vector
