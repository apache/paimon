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

from abc import abstractmethod
from typing import Iterator, Optional, TypeVar

import polars
import pyarrow as pa
from pyarrow import RecordBatch, Table

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.offset_row import OffsetRow

T = TypeVar('T')


def _polars_arrow_array(array):
    """Represent MAP as LIST<ROW<key, value>> without losing its null mask."""
    data_type = array.type
    if pa.types.is_struct(data_type):
        children = [_polars_arrow_array(array.field(i)) for i in range(data_type.num_fields)]
        fields = [field.with_type(child.type) for field, child in zip(data_type, children)]
        if pa.struct(fields) == data_type:
            return array
        return pa.StructArray.from_arrays(children, fields=fields, mask=array.is_null())
    if (pa.types.is_map(data_type) or pa.types.is_list(data_type)
            or pa.types.is_large_list(data_type) or pa.types.is_fixed_size_list(data_type)):
        if pa.types.is_map(data_type):
            keys, items = _polars_arrow_array(array.keys), _polars_arrow_array(array.items)
            fields = [data_type.key_field.with_type(keys.type), data_type.item_field.with_type(items.type)]
            child = pa.StructArray.from_arrays([keys, items], fields=fields)
            value = pa.field('entries', child.type, nullable=False)
        else:
            child = _polars_arrow_array(array.values)
            value = data_type.value_field.with_type(child.type)
        if pa.types.is_large_list(data_type):
            target = pa.large_list(value)
        elif pa.types.is_fixed_size_list(data_type):
            target = pa.list_(value, data_type.list_size)
        else:
            target = pa.list_(value)
        if target == data_type:
            return array
        if pa.types.is_fixed_size_list(data_type):
            child = child.slice(array.offset * data_type.list_size, len(array) * data_type.list_size)
            return pa.Array.from_buffers(target, len(array), [array.is_valid().buffers()[1]],
                                         null_count=array.null_count, children=[child])
        return pa.Array.from_buffers(target, len(array), array.buffers()[:data_type.num_buffers],
                                     null_count=array.null_count, offset=array.offset, children=[child])
    return array


def _polars_arrow_table(batch):
    arrays = [_polars_arrow_array(column) for column in batch.columns]
    schema = pa.schema([field.with_type(array.type) for field, array in zip(batch.schema, arrays)],
                       metadata=batch.schema.metadata)
    # Polars' direct MAP conversion turns null maps into empty lists. Arrow's
    # equivalent list representation preserves the parent bitmap, including
    # MAPs nested inside ROWs or arrays. Reuse buffers without relying on MAP
    # casts, which older supported Arrow releases do not provide.
    return Table.from_batches([batch]) if schema == batch.schema else Table.from_arrays(arrays, schema=schema)


_MIN_TIMESTAMP_NS = -(1 << 63)


class _PhysicalNsTimestamp(object):
    """Comparable stand-in when pandas reserves INT64_MIN ns as NaT.

    Preserves the Arrow physical nanosecond value and timezone name so
    keytime ``>`` comparisons and nested MAP/STRUCT materialization keep
    the minimum TIMESTAMP(9) / TIMESTAMP_LTZ(9) sentinel.
    """

    __slots__ = ('value', 'tzinfo')

    def __init__(self, value, tz=None):
        self.value = int(value)
        self.tzinfo = tz

    def __int__(self):
        # PythonTableRead._emit_overflow_safe_batches builds columns with
        # ``pyarrow.array(values, type=timestamp('ns'))``; exposing the
        # physical int keeps INT64_MIN valid instead of failing conversion.
        return self.value

    @property
    def nanosecond(self):
        rem = self.value % 1_000_000_000
        return rem + 1_000_000_000 if rem < 0 else rem

    @staticmethod
    def _as_ns(other):
        if isinstance(other, _PhysicalNsTimestamp):
            return other.value
        value = getattr(other, 'value', None)
        if isinstance(value, int):
            return value
        if isinstance(other, pa.TimestampScalar) and other.is_valid:
            return int(other.value)
        raise TypeError(
            "Cannot compare preserved ns timestamp with {}".format(type(other)))

    def __eq__(self, other):
        try:
            return self.value == self._as_ns(other)
        except TypeError:
            return NotImplemented

    def __ne__(self, other):
        equal = self.__eq__(other)
        return equal if equal is NotImplemented else not equal

    def __lt__(self, other):
        try:
            return self.value < self._as_ns(other)
        except TypeError:
            return NotImplemented

    def __le__(self, other):
        try:
            return self.value <= self._as_ns(other)
        except TypeError:
            return NotImplemented

    def __gt__(self, other):
        try:
            return self.value > self._as_ns(other)
        except TypeError:
            return NotImplemented

    def __ge__(self, other):
        try:
            return self.value >= self._as_ns(other)
        except TypeError:
            return NotImplemented

    def __repr__(self):
        return "_PhysicalNsTimestamp(value={}, tzinfo={!r})".format(
            self.value, self.tzinfo)


def _arrow_scalar_to_py(scalar):
    """Convert one Arrow scalar without turning INT64_MIN ns into NaT."""
    if not isinstance(scalar, pa.Scalar):
        return scalar
    if not scalar.is_valid:
        return None
    data_type = scalar.type
    if pa.types.is_timestamp(data_type) and data_type.unit == 'ns':
        if int(scalar.value) == _MIN_TIMESTAMP_NS:
            return _PhysicalNsTimestamp(scalar.value, data_type.tz)
        return scalar.as_py()
    if pa.types.is_struct(data_type):
        return {
            data_type.field(i).name: _arrow_scalar_to_py(scalar[data_type.field(i).name])
            for i in range(data_type.num_fields)
        }
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)
            or pa.types.is_fixed_size_list(data_type)):
        return [_arrow_scalar_to_py(scalar[i]) for i in range(len(scalar))]
    if pa.types.is_map(data_type):
        # Native MAP scalar: list of (key, value) pairs.
        entries = []
        values = scalar.values
        for i in range(len(values)):
            entry = values[i]
            entries.append((
                _arrow_scalar_to_py(entry['key']),
                _arrow_scalar_to_py(entry['value']),
            ))
        return entries
    return scalar.as_py()


def _iter_arrow_columns_rows(columns) -> Iterator[tuple]:
    num_rows = len(columns[0])
    for row_index in range(num_rows):
        yield tuple(_arrow_scalar_to_py(column[row_index]) for column in columns)


def _iter_arrow_table_rows(table: Table) -> Iterator[tuple]:
    """Yield row tuples from an Arrow table without Polars ``iter_rows``.

    Polars converts ``Datetime(time_unit='ns')`` values to microsecond
    ``datetime.datetime``, collapsing distinct TIMESTAMP(9) /
    TIMESTAMP_LTZ(9) tails. Walk Arrow scalars instead of
    ``Table.to_pylist()`` so the INT64_MIN nanosecond sentinel stays
    comparable (``to_pylist`` / pandas map it to NaT). Nested MAP/STRUCT
    timestamps use the same path. Older tables without columnar access
    still iterate ``to_batches()`` scalars — never Polars row iteration.
    """
    num_columns = table.num_columns
    if num_columns == 0:
        for _ in range(table.num_rows):
            yield ()
        return

    column = getattr(table, 'column', None)
    if callable(column):
        columns = [table.column(i) for i in range(num_columns)]
        for row in _iter_arrow_columns_rows(columns):
            yield row
        return

    for batch in table.to_batches():
        if batch.num_columns == 0:
            for _ in range(batch.num_rows):
                yield ()
            continue
        columns = [batch.column(i) for i in range(batch.num_columns)]
        for row in _iter_arrow_columns_rows(columns):
            yield row


class RecordBatchReader(RecordReader):
    """
    The reader that reads the pyarrow batches of records.
    """

    file_io = None
    blob_field_indices = None
    descriptor_field_indices = None
    blob_view_lookup = None
    vector_field_indices = None

    def _adopt_metadata(self, reader: "RecordBatchReader") -> None:
        self.file_io = reader.file_io
        self.blob_field_indices = reader.blob_field_indices
        self.descriptor_field_indices = getattr(
            reader, 'descriptor_field_indices', None)
        self.blob_view_lookup = getattr(reader, 'blob_view_lookup', None)
        self.vector_field_indices = reader.vector_field_indices

    @abstractmethod
    def read_arrow_batch(self) -> Optional[RecordBatch]:
        """
        Reads one batch. The method should return null when reaching the end of the input.
        """

    def return_batch_pos(self) -> int:
        """
        Returns the current batch position in the file.
        """

    def read_next_df(self) -> Optional[polars.DataFrame]:
        arrow_batch = self.read_arrow_batch()
        if arrow_batch is None:
            return None
        # Older Polars versions accept Arrow Table but not RecordBatch.
        return polars.from_arrow(_polars_arrow_table(arrow_batch))

    def tuple_iterator(self) -> Optional[Iterator[tuple]]:
        arrow_batch = self.read_arrow_batch()
        if arrow_batch is None:
            return None
        # InternalRow / KeyValue paths must not round-trip through Polars
        # iter_rows(), which drops nanosecond tails from TIMESTAMP(9).
        return _iter_arrow_table_rows(_polars_arrow_table(arrow_batch))

    def read_batch(self) -> Optional[RecordIterator[InternalRow]]:
        arrow_batch = self.read_arrow_batch()
        if arrow_batch is None:
            return None
        table = _polars_arrow_table(arrow_batch)
        return InternalRowWrapperIterator(
            _iter_arrow_table_rows(table), table.num_columns, self.file_io,
            self.blob_field_indices, self.vector_field_indices,
            self.descriptor_field_indices, self.blob_view_lookup)

    @staticmethod
    def _iter_df_rows(df) -> Iterator[tuple]:
        iter_rows = getattr(df, "iter_rows", None)
        if iter_rows is not None:
            return iter_rows()
        # Older Polars exposes eager row iteration as rows().
        return iter(df.rows())


class InternalRowWrapperIterator(RecordIterator[InternalRow]):
    def __init__(self, iterator: Iterator[tuple], width: int,
                 file_io=None, blob_field_indices=None,
                 vector_field_indices=None,
                 descriptor_field_indices=None,
                 blob_view_lookup=None):
        self._iterator = iterator
        self._reused_row = OffsetRow(None, 0, width,
                                     file_io=file_io,
                                     blob_field_indices=blob_field_indices,
                                     vector_field_indices=vector_field_indices,
                                     descriptor_field_indices=descriptor_field_indices,
                                     blob_view_lookup=blob_view_lookup)

    def next(self) -> Optional[InternalRow]:
        row_tuple = next(self._iterator, None)
        if row_tuple is None:
            return None
        self._reused_row.replace(row_tuple)
        return self._reused_row


class RowPositionReader(RecordBatchReader):

    def __init__(self, data_reader: RecordBatchReader):
        self._data_reader = data_reader
        self._row_iterator = RowPositionRecordIterator()
        self.batch_pos = 0
        self._adopt_metadata(data_reader)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batch = self._data_reader.read_arrow_batch()
        if batch is None:
            return None
        self._refresh_blob_view_lookup(self._data_reader)
        self.batch_pos += batch.num_rows
        return batch

    def return_batch_pos(self) -> int:
        return self.batch_pos

    def tuple_iterator(self) -> Optional[RecordIterator]:
        return self._row_iterator.replace_iterator(self._data_reader.tuple_iterator())

    def close(self):
        self._data_reader.close()


class RowPositionRecordIterator(RecordIterator[tuple]):

    def __init__(self):
        self.reused_iterator: Optional[Iterator[tuple]] = None
        self.pos = -1

    def next(self) -> Optional[tuple]:
        row_tuple = next(self.reused_iterator, None)
        if row_tuple is None:
            return None
        self.pos += 1
        return row_tuple

    def return_pos(self) -> int:
        return self.pos

    def replace_iterator(self, iterator: Iterator[tuple]) -> Optional[RecordIterator[tuple]]:
        self.reused_iterator = iterator
        if self.reused_iterator is None:
            return None
        return self

    def __next__(self):
        return self.next()


class EmptyRecordBatchReader(RecordBatchReader):

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        return None

    def close(self):
        return None
