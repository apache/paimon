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

from typing import Optional, List

import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.api import Predicate
from pypaimon.pynative.common.file_io import FileIO
from pypaimon.pynative.read.reader.iface.record_batch_reader import RecordBatchReader

from pypaimon.pynative.util.predicate_converter import convert_predicate


class FormatPyArrowReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Parquet or ORC file using PyArrow,
    and filters it based on the provided predicate and projection.
    """

    def __init__(self, file_io: FileIO, file_format: str, file_path: str, primary_keys: List[str],
                 fields: List[str], predicate: Predicate, batch_size: int = 4096):

        if primary_keys:
            # TODO: utilize predicate to improve performance
            predicate = None

        if predicate is not None:
            predicate = convert_predicate(predicate)

        self.dataset = ds.dataset(file_path, format=file_format, filesystem=file_io.filesystem)
        self.reader = self.dataset.scanner(
            columns=fields,
            filter=predicate,
            batch_size=batch_size
        ).to_reader()

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            return self.reader.read_next_batch()
        except StopIteration:
            return None

    def close(self):
        if self.reader is not None:
            self.reader.close()
            self.reader = None
