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

"""Construct format helpers for explicitly selected Python FileIO methods.

Native readers and writers use Core FileIO and never enter these adapters.
"""


def new_row_writer(output_stream, fields, zstd_level=1):
    from pypaimon.execution.python.write.writer.format_row_writer import FormatRowWriter
    return FormatRowWriter(output_stream, fields, zstd_level=zstd_level)


def new_blob_writer(output_stream):
    from pypaimon.execution.python.write.blob_format_writer import BlobFormatWriter
    return BlobFormatWriter(output_stream)


def vortex_location(file_io, path):
    from pypaimon.execution.python.read.reader.vortex_utils import to_vortex_specified
    return to_vortex_specified(file_io, path)
