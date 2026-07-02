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

"""Blob I/O helpers for the Daft integration."""

from __future__ import annotations

from functools import lru_cache
from typing import BinaryIO, Dict


@lru_cache(maxsize=16)
def _get_file_io(catalog_key: tuple, table: str):
    from pypaimon.catalog.catalog_factory import CatalogFactory
    catalog_options = dict(catalog_key)
    return CatalogFactory.create(catalog_options).get_table(table).file_io


def _cache_key(catalog_options: Dict[str, str]) -> tuple:
    return tuple(sorted(catalog_options.items()))


def _file_range_fields():
    from pypaimon.daft.daft_compat import file_range_position_field, file_range_size_field
    return file_range_position_field(), file_range_size_field()


def _range_attr(file, name):
    # Prefer _inner accessors: they read the embedded range; public File.size
    # does a network stat (~200ms/blob).
    inner = getattr(file, "_inner", None)
    src = inner if inner is not None and hasattr(inner, name) else file
    v = getattr(src, name)
    return v() if callable(v) else v


def _resolve_file_range(file, pos_field=None, size_field=None):
    if pos_field is None:
        pos_field, size_field = _file_range_fields()
    return (_range_attr(file, "path"),
            _range_attr(file, pos_field),
            _range_attr(file, size_field))


def open_blob(file, catalog_options: Dict[str, str], table: str) -> BinaryIO:
    """Open a Daft ``File`` as a seekable stream via pypaimon's FileIO."""
    from pypaimon.table.row.blob import OffsetInputStream
    path, offset, length = _resolve_file_range(file)
    fio = _get_file_io(_cache_key(catalog_options), table)
    stream = fio.new_input_stream(path)
    try:
        return OffsetInputStream(stream, offset, length)
    except Exception:
        stream.close()
        raise


def read_blob(column, catalog_options: Dict[str, str], table: str, *, max_concurrency: int = 64):
    """Read a blob ``File`` column to ``binary`` bytes via concurrent ranged reads."""
    import daft
    from daft import DataType

    @daft.func.batch(return_dtype=DataType.binary())
    def _read_blobs(files):
        pos_field, size_field = _file_range_fields()
        # Vectorized field extraction: per-File attr access (to_pylist +
        # f.path/.position/.size) is GIL-bound and serializes the pool. daft
        # File is an arrow ExtensionArray; .storage holds url/offset/size.
        try:
            struct = files.to_arrow()
            struct = getattr(struct, "storage", struct)
            ranges = list(zip(struct.field("url").to_pylist(),
                              struct.field(pos_field).to_pylist(),
                              struct.field(size_field).to_pylist()))
        except (AttributeError, KeyError, TypeError, ValueError):
            # Unexpected column layout: fall back to per-File resolution (cheap).
            ranges = [None if f is None else _resolve_file_range(f, pos_field, size_field)
                      for f in files.to_pylist()]

        fio = _get_file_io(_cache_key(catalog_options), table)
        # Coalesce same-file adjacent reads to cut per-request round trips.
        return fio.read_ranges_coalesced(ranges, max_concurrency)

    return _read_blobs(column)
