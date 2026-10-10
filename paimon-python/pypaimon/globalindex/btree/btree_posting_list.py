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

"""Decode BTree postings into a query result bitmap."""

from pypaimon.globalindex.btree.btree_file_footer import BTreeFileFooter
from pypaimon.globalindex.memory_slice_input import MemorySliceInput
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


_SINGLE = 0
_DELTA_LIST = 1
_ROARING = 2
_LONG_MAX_VALUE = (1 << 63) - 1


def _read_row_id(input_: MemorySliceInput) -> int:
    row_id = input_.read_var_len_long()
    if row_id > _LONG_MAX_VALUE:
        raise ValueError(f"BTree row id exceeds Long.MAX_VALUE: {row_id}")
    return row_id


def add_row_ids(data: bytes, version: int, target: RoaringBitmap64) -> None:
    """Accumulate a posting; malformed input raises instead of returning a result."""
    input_ = MemorySliceInput(data)
    if version == BTreeFileFooter.VERSION_1:
        count = input_.read_var_len_int()
        if count <= 0:
            raise ValueError(f"Invalid row id length: {count}")
        for _ in range(count):
            target.add(input_.read_var_len_long())
        return

    if version != BTreeFileFooter.VERSION_2:
        raise ValueError(f"Unsupported BTree index file version: {version}")

    type_ = input_.read_unsigned_byte()
    if type_ == _SINGLE:
        target.add(_read_row_id(input_))
    elif type_ == _DELTA_LIST:
        count = input_.read_var_len_int()
        if count <= 1 or count > (1 << 31) - 1:
            raise ValueError(f"Invalid delta BTree posting list length: {count}")
        row_id = _read_row_id(input_)
        target.add(row_id)
        for _ in range(count - 1):
            delta = _read_row_id(input_)
            if delta == 0:
                raise ValueError("Invalid non-positive BTree row id delta: 0")
            row_id += delta
            if row_id > _LONG_MAX_VALUE:
                raise ValueError(f"BTree row id exceeds Long.MAX_VALUE: {row_id}")
            target.add(row_id)
    elif type_ == _ROARING:
        bitmap = RoaringBitmap64.deserialize(input_.read_slice(input_.available()))
        if bitmap.is_empty():
            raise ValueError("Invalid empty Roaring BTree posting list")
        if bitmap.max() > _LONG_MAX_VALUE:
            raise ValueError("BTree row id exceeds Long.MAX_VALUE")
        target.or_inplace(bitmap)
    else:
        raise ValueError(f"Unknown BTree posting list type: {type_}")
