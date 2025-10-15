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

import logging
from typing import Tuple

from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter

logger = logging.getLogger(__name__)


class BlobWriter(AppendOnlyDataWriter):
    """
    A specialized writer for blob data that extends AppendOnlyDataWriter.
    The only difference is that it uses "blob" as the file format.
    """

    def __init__(self, table, partition: Tuple, bucket: int, max_seq_number: int, blob_column: str):
        """
        Initialize BlobWriter.

        Args:
            table: The table to write to
            partition: The partition tuple
            bucket: The bucket number
            max_seq_number: The maximum sequence number
        """
        # Call parent constructor
        super().__init__(table, partition, bucket, max_seq_number, [blob_column])

        # Override file format to "blob"
        self.file_format = "blob"

        logger.info("Initialized BlobWriter with blob file format")

    @staticmethod
    def _get_column_stats(record_batch, column_name: str):
        """Override to not generate min/max values for blob columns."""
        column_array = record_batch.column(column_name)
        if column_array.null_count == len(column_array):
            return {
                "min_values": None,
                "max_values": None,
                "null_counts": column_array.null_count,
            }

        # For blob data, don't generate min/max values
        return {
            "min_values": None,
            "max_values": None,
            "null_counts": column_array.null_count,
        }
