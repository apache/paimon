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
#################################################################################

from abc import ABC, abstractmethod

from pypaimon.read.read_builder import ReadBuilder
from pypaimon.write.write_builder import BatchWriteBuilder, StreamWriteBuilder


class Table(ABC):
    """A table provides basic abstraction for table read and write."""

    @abstractmethod
    def new_read_builder(self) -> ReadBuilder:
        """Return a builder for building table scan and table read."""

    @abstractmethod
    def new_batch_write_builder(self) -> BatchWriteBuilder:
        """Returns a builder for building batch table write and table commit."""

    @abstractmethod
    def new_stream_write_builder(self) -> StreamWriteBuilder:
        """Returns a builder for building stream table write and table commit."""
