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

from typing import Iterator, Optional, Union

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.key_value import KeyValue


class KeyValueWrapReader(RecordReader[KeyValue]):
    """
    RecordReader for reading KeyValue data files.
    Corresponds to the KeyValueDataFileRecordReader in Java version.
    """

    def __init__(self, data_reader: RecordBatchReader, key_arity, value_arity):
        self.data_reader = data_reader
        self.key_arity = key_arity
        self.value_arity = value_arity
        self.reused_kv = KeyValue(self.key_arity, self.value_arity)

    def read_batch(self) -> Optional[RecordIterator[KeyValue]]:
        iterator = self.data_reader.tuple_iterator()
        if iterator is None:
            return None
        return KeyValueWrapIterator(iterator, self.reused_kv)

    def close(self):
        self.data_reader.close()


class KeyValueWrapIterator(RecordIterator[KeyValue]):
    """
    An Iterator that converts an PrimaryKey InternalRow into a KeyValue
    """

    def __init__(
            self,
            iterator: Union[Iterator, RecordIterator],
            reused_kv: KeyValue
    ):
        self.iterator = iterator
        self.reused_kv = reused_kv

    def next(self) -> Optional[KeyValue]:
        row_tuple = next(self.iterator, None)
        if row_tuple is None:
            return None
        self.reused_kv.replace(row_tuple)
        return self.reused_kv

    def return_pos(self) -> int:
        return self.iterator.return_pos()
