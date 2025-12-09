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

from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from pypaimon.read.reader.iface.record_iterator import RecordIterator

T = TypeVar('T')


class RecordReader(Generic[T], ABC):
    """
    The reader that reads the batches of records.
    """

    @abstractmethod
    def read_batch(self) -> Optional[RecordIterator[T]]:
        """
        Reads one batch as a RecordIterator. The method should return null when reaching the end of the input.
        """

    @abstractmethod
    def close(self):
        """
        Closes the reader and should release all resources.
        """
