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

"""Read contiguous windows of flat or standard VARIANT columns using OffsetIndex.

Selected encoded pages are placed in bounded, in-memory Parquet files. PyArrow
still decodes the pages, including dictionary and compression encodings. Source
files are never rewritten. Other nested schemas and files without indexes use
the normal reader. Disjoint ranges use the normal reader too: per-page seeks
can amplify requests in filesystems that prefetch remote data (including Jindo).
"""

import base64
import bisect
import struct

import pyarrow as pa
import pyarrow.parquet as pq


# Bound encoded data retained by the temporary column files, independently of
# the number of requested rows and the size of the source row group.
_MAX_PAGE_BYTES = 32 * 1024 * 1024
_MAX_INDEX_BYTES = 8 * 1024 * 1024


class _Compact:
    """Thrift compact values used by Parquet metadata (no generated bindings)."""

    def __init__(self, data):
        self.data = memoryview(data)
        self.position = 0

    def take(self, size):
        end = self.position + size
        if size < 0 or end > len(self.data):
            raise ValueError("Truncated Parquet page-index metadata")
        result = self.data[self.position:end]
        self.position = end
        return result

    def unsigned(self):
        result = 0
        for shift in range(0, 70, 7):
            value = self.take(1)[0]
            result |= (value & 127) << shift
            if value < 128:
                return result
        raise ValueError("Invalid Parquet compact integer")

    def value(self, kind, depth=0):
        if depth > 64:
            raise ValueError("Parquet metadata nesting exceeds 64 levels")
        if kind in (1, 2):
            return kind == 1
        if kind == 3:
            return self.take(1).tobytes()
        if kind in (4, 5, 6):
            value = self.unsigned()
            return (value >> 1) ^ -(value & 1)
        if kind == 7:
            return self.take(8).tobytes()
        if kind == 8:
            return self.take(self.unsigned()).tobytes()
        if kind in (9, 10):
            header = self.take(1)[0]
            count, element = header >> 4, header & 15
            if count == 15:
                count = self.unsigned()
            if count > len(self.data) - self.position:
                raise ValueError("Invalid Parquet compact collection size")
            return element, [
                self.value(self.take(1)[0] if element in (1, 2) else element, depth + 1)
                for _ in range(count)
            ]
        if kind == 12:
            fields = {}
            previous = 0
            while True:
                header = self.take(1)[0]
                if header == 0:
                    return fields
                delta, field_kind = header >> 4, header & 15
                field = previous + delta if delta else self.value(4)
                if field in fields:
                    raise ValueError("Duplicate Parquet compact field")
                fields[field] = field_kind, self.value(field_kind, depth + 1)
                previous = field
        raise ValueError("Unsupported Parquet compact type: {}".format(kind))


def _unsigned(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def _encode(kind, value):
    if kind in (1, 2):
        return bytes([1 if value else 2])
    if kind in (3, 7):
        return value
    if kind in (4, 5, 6):
        return _unsigned(value * 2 if value >= 0 else -value * 2 - 1)
    if kind == 8:
        return _unsigned(len(value)) + value
    if kind in (9, 10):
        element, items = value
        size = len(items)
        header = bytes([(min(size, 15) << 4) | element])
        if size >= 15:
            header += _unsigned(size)
        return header + b"".join(_encode(element, item) for item in items)
    if kind == 12:
        result = bytearray()
        previous = 0
        for field, (field_kind, item) in sorted(value.items()):
            delta = field - previous
            if field_kind in (1, 2):
                field_kind = 1 if item else 2
            if 0 < delta < 16:
                result.append((delta << 4) | field_kind)
            else:
                result.append(field_kind)
                result.extend(_encode(4, field))
            if field_kind not in (1, 2):
                result.extend(_encode(field_kind, item))
            previous = field
        result.append(0)
        return bytes(result)
    raise ValueError("Unsupported Parquet compact type: {}".format(kind))


def _get(fields, field, default=None):
    return fields[field][1] if field in fields else default


def _read_exact(source, offset, length):
    if offset < 4 or length <= 0:
        raise ValueError("Invalid Parquet page-index byte range")
    data = source.read_at(length, offset)
    if len(data) != length:
        raise OSError("Truncated Parquet page-index byte range")
    return data


class ParquetPageIndexReader:
    def __init__(self, source, metadata, footer, columns, projections,
                 leaf_fields, schema_elements, batch_size):
        self.source = source
        self.metadata = metadata
        self.footer = footer
        self.columns = columns
        self.projections = projections
        self.leaf_fields = leaf_fields
        self.schema_elements = schema_elements
        self.batch_size = batch_size

    @classmethod
    def create(cls, source, parquet_file, columns, row_groups, batch_size):
        metadata = parquet_file.metadata
        schema = parquet_file.schema_arrow
        if not columns or metadata.num_row_groups == 0:
            return None
        if len(set(schema.names)) != len(schema):
            return None
        output = pa.BufferOutputStream()
        metadata.write_metadata_file(output)
        serialized = output.getvalue().to_pybytes()
        length = struct.unpack("<I", serialized[-8:-4])[0]
        footer = _Compact(serialized[-8 - length:-8]).value(12)
        if 8 in footer or 9 in footer:
            return None  # Encrypted pages need the original file identity/AAD.
        elements = _get(footer, 2)[1]
        if any(b'.' in _get(element, 4) for element in elements):
            return None  # A joined physical path would be ambiguous.
        leaf_elements = {}

        def visit(position, prefix):
            if len(prefix) > 64:
                raise ValueError('Parquet schema nesting exceeds 64 levels')
            element = elements[position]
            name = _get(element, 4).decode('utf-8')
            path = prefix + (name,)
            position += 1
            children = _get(element, 5, 0)
            if children:
                for _ in range(children):
                    position = visit(position, path)
            else:
                leaf_elements['.'.join(path[1:])] = position - 1
            return position

        if visit(0, ()) != len(elements):
            return None
        physical = {
            metadata.row_group(0).column(index).path_in_schema: index
            for index in range(metadata.num_columns)
        }
        if len(physical) != metadata.num_columns:
            return None
        indices = []
        projections = []
        leaf_fields = {}
        schema_elements = {}
        for name in columns:
            if '.' in name:
                return None  # Avoid ambiguous physical paths for quoted dotted names.
            field_index = schema.get_field_index(name)
            if field_index < 0:
                return None
            field = schema.field(field_index)
            if not pa.types.is_nested(field.type):
                paths = [(name, field)]
            elif (pa.types.is_struct(field.type)
                  and len(field.type) == 2
                  and [child.name for child in field.type] == ['value', 'metadata']
                  and all(pa.types.is_binary(child.type) and not child.nullable
                          for child in field.type)):
                paths = [
                    (name + '.' + child.name,
                     pa.field(child.name, child.type, nullable=field.nullable))
                    for child in field.type
                ]
            else:
                return None
            physical_indices = []
            for path, leaf_field in paths:
                index = physical.get(path)
                if (index is None or path not in leaf_elements
                        or parquet_file.schema.column(index).max_repetition_level != 0
                        or parquet_file.schema.column(index).max_definition_level
                        != int(leaf_field.nullable)):
                    return None
                physical_indices.append(index)
                leaf_fields[index] = leaf_field
                schema_elements[index] = leaf_elements[path]
            indices.extend(physical_indices)
            projections.append((field, physical_indices))
        if len(set(indices)) != len(indices):
            return None
        if not any(all(getattr(metadata.row_group(group).column(index),
                               'has_offset_index', False) for index in indices)
                   for group in row_groups):
            return None
        return cls(source, metadata, footer, indices, projections,
                   leaf_fields, schema_elements, batch_size)

    def read_row_group(self, group, runs):
        """Return selected batches, or None when the ordinary reader is cheaper."""
        # Decide before reading indexes so scattered selections preserve the
        # existing I/O pattern. A future multi-range path needs an I/O planner
        # that accounts for filesystem prefetch, not just compressed page sizes.
        if len(runs) != 1:
            return None
        row_group = _get(self.footer, 4)[1][group]
        row_count = _get(row_group, 3)
        if sum(upper - lower + 1 for lower, upper in runs) >= row_count:
            return None
        chunks = _get(row_group, 1)[1]
        if any(4 not in chunks[index] or 5 not in chunks[index]
               or _get(chunks[index], 1) or 8 in chunks[index] or 9 in chunks[index]
               or 10 in _get(chunks[index], 3)  # Legacy index pages.
               for index in self.columns):
            return None
        plans = []
        selected_bytes = 0
        index_bytes = 0
        full_bytes = 0
        for index in sorted(self.columns):
            chunk = chunks[index]
            index_size = _get(chunk, 5)
            if index_size > _MAX_INDEX_BYTES:
                return None
            raw = _read_exact(self.source, _get(chunk, 4), index_size)
            index_bytes += index_size
            locations = _get(_Compact(raw).value(12), 1)[1]
            column = _get(chunk, 3)
            data_offset = _get(column, 9)
            dictionary_offset = _get(column, 11, data_offset)
            chunk_end = dictionary_offset + _get(column, 7)
            starts = [_get(page, 3) for page in locations]
            if not starts or starts[0] != 0 or starts[-1] >= row_count:
                raise ValueError("Invalid Parquet OffsetIndex row boundaries")
            previous_end = data_offset
            previous_row = -1
            for page in locations:
                offset, size, first_row = (_get(page, field) for field in (1, 2, 3))
                if (offset < previous_end or size <= 0 or offset + size > chunk_end
                        or first_row <= previous_row):
                    raise ValueError("Invalid Parquet OffsetIndex page location")
                previous_end, previous_row = offset + size, first_row
            if _get(locations[0], 1) != data_offset or dictionary_offset > data_offset:
                raise ValueError("Invalid Parquet OffsetIndex first page")
            lower, upper = runs[0]
            selected = range(bisect.bisect_right(starts, lower) - 1,
                             bisect.bisect_right(starts, upper))
            pages = []
            infos = []
            for position in selected:
                page = locations[position]
                pages.append((_get(page, 1), _get(page, 2)))
                end = starts[position + 1] if position + 1 < len(starts) else row_count
                infos.append((starts[position], end - starts[position]))
            dictionary_size = data_offset - dictionary_offset
            selected_bytes += dictionary_size + sum(size for _, size in pages)
            full_bytes += _get(column, 7)
            plans.append((index, column, dictionary_offset, dictionary_size, pages, infos))
        if (selected_bytes + index_bytes >= full_bytes
                or selected_bytes > _MAX_PAGE_BYTES):
            return None
        return self._batches(plans, runs)

    def _column_batches(self, plan, runs):
        from pypaimon.read.reader.format_pyarrow_reader import _RowRunSlicer

        index, column, dictionary_offset, dictionary_size, pages, infos = plan
        ranges = ([(dictionary_offset, dictionary_size)] if dictionary_size else []) + pages
        # Coalesce adjacent dictionary/data pages without fetching skipped pages.
        groups = []
        for offset, length in ranges:
            if groups and groups[-1][0] + groups[-1][1] == offset:
                groups[-1][1] += length
            else:
                groups.append([offset, length])
        payload = b"".join(_read_exact(self.source, offset, length)
                           for offset, length in groups)
        cursor = 0
        uncompressed_size = 0
        for position, (_, length) in enumerate(ranges):
            parser = _Compact(memoryview(payload)[cursor:cursor + length])
            header = parser.value(12)
            if parser.position + _get(header, 3) != length:
                raise ValueError("Parquet page size disagrees with OffsetIndex")
            if dictionary_size and position == 0:
                if _get(header, 1) != 2 or 7 not in header:
                    raise ValueError("Invalid Parquet dictionary page")
            else:
                expected = infos[position - bool(dictionary_size)][1]
                page_type = _get(header, 1)
                if page_type == 0:
                    actual = _get(_get(header, 5), 1)
                elif page_type == 3:
                    page_header = _get(header, 8)
                    actual = _get(page_header, 3)
                    if _get(page_header, 1) != actual:
                        raise ValueError("Invalid non-repeated Parquet data page")
                else:
                    raise ValueError("Invalid Parquet data page type")
                if actual != expected:
                    raise ValueError("Parquet page rows disagree with OffsetIndex")
            uncompressed_size += parser.position + _get(header, 2)
            cursor += length

        num_rows = sum(count for _, count in infos)
        patched_column = {key: value for key, value in column.items() if key <= 8}
        patched_column.update({5: (6, num_rows), 6: (6, uncompressed_size),
                               7: (6, len(payload)), 9: (6, 4 + dictionary_size)})
        if dictionary_size:
            patched_column[11] = (6, 4)
        patched_group = {1: (9, (12, [{2: (6, 0), 3: (12, patched_column)}])),
                         2: (6, uncompressed_size), 3: (6, num_rows)}
        elements = _get(self.footer, 2)[1]
        root = dict(elements[0])
        root[5] = (5, 1)
        schema = pa.schema([self.leaf_fields[index]])
        arrow_schema = base64.b64encode(schema.serialize().to_pybytes())
        leaf = dict(elements[self.schema_elements[index]])
        # A required VARIANT child inherits its optional parent's definition
        # level when decoded as a temporary, flat leaf column.
        leaf[3] = (5, int(self.leaf_fields[index].nullable))
        footer = {1: self.footer[1],
                  2: (9, (12, [root, leaf])),
                  3: (6, num_rows), 4: (9, (12, [patched_group])),
                  5: (9, (12, [{1: (8, b"ARROW:schema"), 2: (8, arrow_schema)}]))}
        if 6 in self.footer:
            footer[6] = self.footer[6]
        encoded = _encode(12, footer)
        data = b"PAR1" + payload + encoded + struct.pack("<I", len(encoded)) + b"PAR1"
        reader = pq.ParquetFile(pa.BufferReader(data))
        del payload, parser
        try:
            batches = reader.iter_batches(batch_size=self.batch_size, use_threads=False)
            slicer = _RowRunSlicer(infos, runs)
            while True:
                batch = slicer.next_batch(batches)
                if batch is None:
                    break
                yield batch.column(0)
        finally:
            reader.close()

    def _batches(self, plans, runs):
        readers = [self._column_batches(plan, runs) for plan in plans]
        remaining = sum(upper - lower + 1 for lower, upper in runs)
        positions = {plan[0]: position for position, plan in enumerate(plans)}
        try:
            arrays = [next(reader, None) for reader in readers]
            offsets = [0] * len(readers)
            while any(array is not None for array in arrays):
                if any(array is None for array in arrays):
                    raise ValueError("Parquet page-index columns have different row counts")
                count = min(len(array) - offset for array, offset in zip(arrays, offsets))
                remaining -= count
                if count <= 0 or remaining < 0:
                    raise ValueError("Invalid Parquet page-index result length")
                projected = []
                for field, indices in self.projections:
                    parts = [arrays[positions[index]].slice(
                        offsets[positions[index]], count) for index in indices]
                    if len(parts) == 1:
                        projected.append(parts[0])
                    else:
                        if not parts[0].is_null().equals(parts[1].is_null()):
                            raise ValueError('VARIANT leaves have different null rows')
                        projected.append(pa.StructArray.from_arrays(
                            parts, fields=list(field.type),
                            mask=parts[0].is_null() if field.nullable else None))
                yield pa.RecordBatch.from_arrays(
                    projected, schema=pa.schema([field for field, _ in self.projections]))
                for index, array in enumerate(arrays):
                    offsets[index] += count
                    if offsets[index] == len(array):
                        arrays[index] = next(readers[index], None)
                        offsets[index] = 0
            if remaining:
                raise ValueError("Truncated Parquet page-index result")
        finally:
            for reader in readers:
                reader.close()
