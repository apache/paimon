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

from typing import Optional

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.key_value import KeyValue


class DropDeleteRecordReader(RecordReader):
    """
    A RecordReader which drops KeyValue that does not meet RowKind. isAdd from the wrapped reader.
    """

    def __init__(self, kv_reader: RecordReader[KeyValue]):
        self.kv_reader = kv_reader

    def read_batch(self) -> Optional[RecordIterator]:
        batch = self.kv_reader.read_batch()
        if batch is None:
            return None

        return DropDeleteIterator(batch)

    def close(self) -> None:
        self.kv_reader.close()


class DropDeleteIterator(RecordIterator[KeyValue]):
    """
    An iterator that drops KeyValue that does not meet RowKind.
    """

    def __init__(self, batch: RecordIterator[KeyValue]):
        self.batch = batch

    def next(self) -> Optional[KeyValue]:
        while True:
            kv = self.batch.next()
            if kv is None:
                return None
            if kv.is_add():
                return kv
