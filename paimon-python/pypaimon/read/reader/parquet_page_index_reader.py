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

"""Read contiguous Parquet row windows using their OffsetIndex.

Selected encoded pages are placed in bounded, in-memory Parquet files. PyArrow
still decodes the pages, including dictionary and compression encodings. Source
files are never rewritten. Nested fields retain their complete physical schema
and align their leaf columns at common row boundaries. Files without indexes and
disjoint ranges use the normal reader: per-page seeks can amplify requests in
filesystems that prefetch remote data (including Jindo).
"""

import base64
import bisect
import struct

import pyarrow as pa
import pyarrow.parquet as pq


# Bound encoded page data retained by the temporary column files.
_MAX_PAGE_BYTES = 32 * 1024 * 1024
# Bound the generic FileMetaData tree before creating Python objects for it.
_MAX_FOOTER_BYTES = 1024 * 1024
_MAX_FOOTER_COLUMN_CHUNKS = 1024
# Bound all serialized OffsetIndexes and their retained typed PageLocations.
_MAX_INDEX_BYTES = 8 * 1024 * 1024
_MAX_PAGE_LOCATIONS = 128 * 1024


class _PageIndexBudgetExceeded(Exception):
    pass


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


class _OffsetIndexDecoder:
    """Decode typed PageLocations without materializing generic Thrift trees."""

    def __init__(self, data, max_locations):
        self.parser = _Compact(data)
        self.max_locations = max_locations
        # PageLocations retain three fields each. Allow the standard optional
        # per-page byte-count list plus a small amount of forward metadata.
        self.remaining_items = max_locations * 6 + 16

    def decode(self):
        locations = None
        previous = 0
        while True:
            field = self._field(previous)
            if field is None:
                break
            field_id, kind = field
            previous = field_id
            if field_id == 1:
                if locations is not None:
                    raise ValueError("Duplicate Parquet OffsetIndex field")
                if kind != 9:
                    raise ValueError("Invalid Parquet OffsetIndex page locations")
                locations = self._locations()
            else:
                self._skip(kind)
        if locations is None or self.parser.position != len(self.parser.data):
            raise ValueError("Invalid Parquet OffsetIndex")
        return locations

    def _field(self, previous):
        header = self.parser.take(1)[0]
        if header == 0:
            return None
        delta, kind = header >> 4, header & 15
        field = previous + delta if delta else self.parser.value(4)
        if field <= 0:
            raise ValueError("Invalid Parquet compact field")
        self._consume(1)
        return field, kind

    def _collection(self):
        header = self.parser.take(1)[0]
        count, element = header >> 4, header & 15
        if count == 15:
            count = self.parser.unsigned()
        if count > len(self.parser.data) - self.parser.position:
            raise ValueError("Invalid Parquet compact collection size")
        return count, element

    def _locations(self):
        count, element = self._collection()
        if element != 12:
            raise ValueError("Invalid Parquet OffsetIndex page locations")
        if count > self.max_locations:
            raise _PageIndexBudgetExceeded(
                "Parquet OffsetIndex exceeds page-location budget")
        self._consume(count)
        return [self._location() for _ in range(count)]

    def _location(self):
        values = [None, None, None]
        expected = (6, 5, 6)
        previous = 0
        while True:
            field = self._field(previous)
            if field is None:
                break
            field_id, kind = field
            previous = field_id
            if 1 <= field_id <= 3:
                if values[field_id - 1] is not None:
                    raise ValueError("Duplicate Parquet PageLocation field")
                if kind != expected[field_id - 1]:
                    raise ValueError("Invalid Parquet PageLocation field type")
                values[field_id - 1] = self.parser.value(kind)
            else:
                self._skip(kind)
        if any(value is None for value in values):
            raise ValueError("Missing Parquet PageLocation field")
        offset, size, first_row = values
        if offset < 0 or size <= 0 or first_row < 0:
            raise ValueError("Invalid Parquet PageLocation")
        return offset, size, first_row

    def _consume(self, count):
        if count > self.remaining_items:
            raise _PageIndexBudgetExceeded(
                "Parquet compact metadata exceeds object budget")
        self.remaining_items -= count

    def _skip_collection_value(self, kind, depth):
        if kind in (1, 2):
            actual = self.parser.take(1)[0]
            if actual not in (1, 2):
                raise ValueError("Invalid Parquet compact boolean")
        else:
            self._skip(kind, depth)

    def _skip(self, kind, depth=0):
        if depth > 64:
            raise ValueError("Parquet metadata nesting exceeds 64 levels")
        if kind in (1, 2):
            return
        if kind == 3:
            self.parser.take(1)
            return
        if kind in (4, 5, 6):
            self.parser.unsigned()
            return
        if kind == 7:
            self.parser.take(8)
            return
        if kind == 8:
            self.parser.take(self.parser.unsigned())
            return
        if kind in (9, 10):
            count, element = self._collection()
            self._consume(count)
            for _ in range(count):
                self._skip_collection_value(element, depth + 1)
            return
        if kind == 11:
            count = self.parser.unsigned()
            self._consume(count * 2)
            if count:
                kinds = self.parser.take(1)[0]
                key_kind, value_kind = kinds >> 4, kinds & 15
                for _ in range(count):
                    self._skip_collection_value(key_kind, depth + 1)
                    self._skip_collection_value(value_kind, depth + 1)
            return
        if kind == 12:
            previous = 0
            while True:
                field = self._field(previous)
                if field is None:
                    return
                field_id, field_kind = field
                previous = field_id
                self._skip(field_kind, depth + 1)
        raise ValueError("Unsupported Parquet compact type: {}".format(kind))


def _decode_offset_index(data, max_locations):
    return _OffsetIndexDecoder(data, max_locations).decode()


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


def _read_index_ranges(source, ranges):
    groups = []
    for key, offset, length in sorted(ranges, key=lambda item: item[1]):
        if offset < 4 or length <= 0:
            raise ValueError("Invalid Parquet page-index byte range")
        end = offset + length
        if groups and offset < groups[-1][1]:
            raise ValueError("Overlapping Parquet page-index byte ranges")
        if groups and offset == groups[-1][1]:
            groups[-1][1] = end
            groups[-1][2].append((key, offset, length))
        else:
            groups.append([offset, end, [(key, offset, length)]])
    result = {}
    for start, end, members in groups:
        data = memoryview(_read_exact(source, start, end - start))
        for key, offset, length in members:
            result[key] = data[offset - start:offset - start + length]
    return result


class ParquetPageIndexReader:
    def __init__(self, source, metadata, schema, footer, columns, fields, batch_size):
        self.source = source
        self.metadata = metadata
        self.schema = schema
        self.footer = footer
        self.columns = columns
        self.fields = fields
        self.batch_size = batch_size

    @classmethod
    def create(cls, source, parquet_file, columns, row_groups, batch_size):
        metadata = parquet_file.metadata
        schema = parquet_file.schema_arrow
        if not columns or len(set(schema.names)) != len(schema):
            return None
        indices = [schema.get_field_index(name) for name in columns]
        if any(index < 0 for index in indices) or len(set(indices)) != len(indices):
            return None
        if not any(getattr(metadata.row_group(group).column(index),
                           "has_offset_index", False)
                   for group in row_groups for index in range(metadata.num_columns)):
            return None
        if (metadata.serialized_size > _MAX_FOOTER_BYTES
                or metadata.num_row_groups * metadata.num_columns
                > _MAX_FOOTER_COLUMN_CHUNKS):
            return None
        output = pa.BufferOutputStream()
        metadata.write_metadata_file(output)
        serialized = output.getvalue().to_pybytes()
        length = struct.unpack("<I", serialized[-8:-4])[0]
        if length > _MAX_FOOTER_BYTES:
            return None
        footer = _Compact(serialized[-8 - length:-8]).value(12)
        if 8 in footer or 9 in footer:
            return None  # Encrypted pages need the original file identity/AAD.
        elements = _get(footer, 2)[1]
        # Parquet stores a preorder schema tree and one chunk per physical leaf.
        # Arrow field positions cannot be used as physical column positions.
        fields = []
        position, leaf = 1, 0
        for _ in range(_get(elements[0], 5)):
            start, first_leaf, pending = position, leaf, 1
            while pending:
                if position >= len(elements):
                    raise ValueError("Truncated Parquet schema tree")
                element = elements[position]
                children = _get(element, 5, 0)
                if children < 0 or (1 in element and children) or (1 not in element and not children):
                    raise ValueError("Invalid Parquet schema child count")
                pending += children - 1
                leaf += int(1 in element)
                position += 1
            fields.append((elements[start:position], list(range(first_leaf, leaf))))
        if (position != len(elements) or leaf != metadata.num_columns
                or len(fields) != len(schema)):
            return None
        if not any(all(getattr(metadata.row_group(group).column(leaf),
                               "has_offset_index", False)
                       for index in indices for leaf in fields[index][1])
                   for group in row_groups):
            return None
        return cls(source, metadata, schema, footer, indices, fields, batch_size)

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
        physical_columns = [leaf for index in self.columns for leaf in self.fields[index][1]]
        if any(4 not in chunks[index] or 5 not in chunks[index]
               or _get(chunks[index], 1) or 8 in chunks[index] or 9 in chunks[index]
               or 10 in _get(chunks[index], 3)  # Legacy index pages.
               for index in physical_columns):
            return None
        indexed = {}
        plans = []
        selected_bytes = 0
        full_bytes = 0
        index_ranges = []
        for index in sorted(physical_columns):
            chunk = chunks[index]
            index_size = _get(chunk, 5)
            index_ranges.append((index, _get(chunk, 4), index_size))
        index_bytes = sum(length for _, _, length in index_ranges)
        if index_bytes > _MAX_INDEX_BYTES:
            return None
        raw_indexes = _read_index_ranges(self.source, index_ranges)
        remaining_locations = _MAX_PAGE_LOCATIONS
        for index in sorted(physical_columns):
            chunk = chunks[index]
            try:
                locations = _decode_offset_index(raw_indexes[index], remaining_locations)
            except _PageIndexBudgetExceeded:
                return None
            remaining_locations -= len(locations)
            column = _get(chunk, 3)
            data_offset = _get(column, 9)
            dictionary_offset = _get(column, 11, data_offset)
            chunk_end = dictionary_offset + _get(column, 7)
            starts = [page[2] for page in locations]
            if not starts or starts[0] != 0 or starts[-1] >= row_count:
                raise ValueError("Invalid Parquet OffsetIndex row boundaries")
            previous_end = data_offset
            previous_row = -1
            for page in locations:
                offset, size, first_row = page
                if (offset < previous_end or size <= 0 or offset + size > chunk_end
                        or first_row <= previous_row):
                    raise ValueError("Invalid Parquet OffsetIndex page location")
                previous_end, previous_row = offset + size, first_row
            if locations[0][0] != data_offset or dictionary_offset > data_offset:
                raise ValueError("Invalid Parquet OffsetIndex first page")
            indexed[index] = (column, dictionary_offset, data_offset - dictionary_offset,
                              locations, starts)
            full_bytes += _get(column, 7)
        for field in sorted(self.columns):
            leaves = self.fields[field][1]
            # OffsetIndex pages must start at row boundaries (repetition level 0).
            # Keep all leaves of a field aligned so Arrow can reconstruct nesting.
            # ponytail: common boundaries may widen to the whole group; independent
            # leaf decoding/reassembly can recover savings if this becomes costly.
            boundaries = set(indexed[leaves[0]][4])
            for leaf in leaves[1:]:
                boundaries.intersection_update(indexed[leaf][4])
            boundaries = sorted(boundaries) + [row_count]
            lower, upper = runs[0]
            lower = boundaries[bisect.bisect_right(boundaries, lower) - 1]
            end = boundaries[bisect.bisect_right(boundaries, upper)]
            column_plans = []
            for index in leaves:
                column, dictionary_offset, dictionary_size, locations, starts = indexed[index]
                selected = range(bisect.bisect_left(starts, lower),
                                 bisect.bisect_left(starts, end))
                pages, infos = [], []
                for position in selected:
                    page = locations[position]
                    pages.append(page[:2])
                    next_row = starts[position + 1] if position + 1 < len(starts) else row_count
                    infos.append((starts[position], next_row - starts[position]))
                selected_bytes += dictionary_size + sum(size for _, size in pages)
                column_plans.append((index, column, dictionary_offset, dictionary_size, pages, infos))
            plans.append((field, column_plans, [(lower, end - lower)]))
        if (selected_bytes + index_bytes >= full_bytes
                or selected_bytes > _MAX_PAGE_BYTES):
            return None
        return self._batches(plans, runs)

    def _column_payload(self, plan):
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
        num_values = 0
        repeated = self.metadata.schema.column(index).max_repetition_level > 0
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
                    values = _get(_get(header, 5), 1)
                    actual = expected if repeated else values
                elif page_type == 3:
                    page_header = _get(header, 8)
                    actual = _get(page_header, 3)
                    values = _get(page_header, 1)
                    if not repeated and values != actual:
                        raise ValueError("Invalid non-repeated Parquet data page")
                else:
                    raise ValueError("Invalid Parquet data page type")
                if actual != expected or values < expected:
                    raise ValueError("Parquet page rows disagree with OffsetIndex")
                num_values += values
            uncompressed_size += parser.position + _get(header, 2)
            cursor += length

        patched_column = {key: value for key, value in column.items() if key <= 8}
        patched_column.update({5: (6, num_values), 6: (6, uncompressed_size),
                               7: (6, len(payload)), 9: (6, 4 + dictionary_size)})
        if dictionary_size:
            patched_column[11] = (6, 4)
        return payload, patched_column, uncompressed_size

    def _column_batches(self, plan, runs):
        from pypaimon.read.reader.format_pyarrow_reader import _RowRunSlicer

        index, column_plans, infos = plan
        payloads, chunks = [], []
        offset, uncompressed_size = 0, 0
        for column_plan in column_plans:
            payload, column, size = self._column_payload(column_plan)
            for field in (9, 11):
                if field in column:
                    column[field] = (6, _get(column, field) + offset)
            chunks.append({2: (6, 0), 3: (12, column)})
            payloads.append(payload)
            offset += len(payload)
            uncompressed_size += size
        num_rows = sum(count for _, count in infos)
        patched_group = {1: (9, (12, chunks)),
                         2: (6, uncompressed_size), 3: (6, num_rows)}
        elements = _get(self.footer, 2)[1]
        root = dict(elements[0])
        root[5] = (5, 1)
        schema = pa.schema([self.schema.field(index)])
        arrow_schema = base64.b64encode(schema.serialize().to_pybytes())
        footer = {1: self.footer[1], 2: (9, (12, [root] + self.fields[index][0])),
                  3: (6, num_rows), 4: (9, (12, [patched_group])),
                  5: (9, (12, [{1: (8, b"ARROW:schema"), 2: (8, arrow_schema)}]))}
        if 6 in self.footer:
            footer[6] = self.footer[6]
        encoded = _encode(12, footer)
        data = b"".join([b"PAR1"] + payloads + [encoded, struct.pack("<I", len(encoded)), b"PAR1"])
        del payloads, payload
        reader = pq.ParquetFile(pa.BufferReader(data))
        try:
            def checked_batches():
                count = 0
                for batch in reader.iter_batches(batch_size=self.batch_size, use_threads=False):
                    count += batch.num_rows
                    if count > num_rows:
                        raise ValueError("Parquet decoded rows disagree with OffsetIndex")
                    yield batch
                if count != num_rows:
                    raise ValueError("Parquet decoded rows disagree with OffsetIndex")

            batches = checked_batches()
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
        projection = [positions[index] for index in self.columns]
        try:
            arrays = [next(reader, None) for reader in readers]
            offsets = [0] * len(readers)
            schema = pa.schema([self.schema.field(index) for index in self.columns])
            while any(array is not None for array in arrays):
                if any(array is None for array in arrays):
                    raise ValueError("Parquet page-index columns have different row counts")
                count = min(len(array) - offset for array, offset in zip(arrays, offsets))
                remaining -= count
                if count <= 0 or remaining < 0:
                    raise ValueError("Invalid Parquet page-index result length")
                yield pa.RecordBatch.from_arrays(
                    [arrays[index].slice(offsets[index], count) for index in projection],
                    schema=schema)
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
