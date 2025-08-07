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

from typing import Any, Optional

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.key_value import KeyValue
from pypaimon.table.row.offset_row import OffsetRow


class KeyValueUnwrapRecordReader(RecordReader[InternalRow]):
    """
    A RecordReader that converts a KeyValue type record reader into an InternalRow type reader
    Corresponds to the KeyValueTableRead$1 in Java version.
    """

    def __init__(self, kv_reader: RecordReader[KeyValue]):
        self.kv_reader = kv_reader

    def read_batch(self) -> Optional[RecordIterator[InternalRow]]:
        batch = self.kv_reader.read_batch()
        if batch is None:
            return None

        return KeyValueUnwrapIterator(batch)

    def close(self) -> None:
        self.kv_reader.close()


class KeyValueUnwrapIterator(RecordIterator[InternalRow]):
    """
    An Iterator that converts a KeyValue into an InternalRow
    """

    def __init__(self, batch: RecordIterator[KeyValue]):
        self.batch = batch

    def next(self) -> Optional[Any]:
        kv = self.batch.next()
        if kv is None:
            return None

        row: OffsetRow = kv.value
        row.set_row_kind_byte(kv.value_row_kind_byte)
        return row
