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

import struct


FOOTER_SIZE = 32
MAGIC = 0x524F5753
VERSION_PLAIN = 1
VERSION_WITH_SCHEMA = 2


def read_row_footer(stream, file_size):
    if file_size < FOOTER_SIZE:
        raise IOError(f"Invalid row file: {file_size} bytes hold no {FOOTER_SIZE}-byte footer")

    stream.seek(file_size - FOOTER_SIZE)
    footer = stream.read(FOOTER_SIZE)
    if len(footer) != FOOTER_SIZE:
        raise IOError("Invalid row file: cannot read footer")

    magic = struct.unpack_from('<I', footer, 28)[0]
    if magic != MAGIC:
        raise IOError(f"Invalid row file magic: expected 0x{MAGIC:08X}, got 0x{magic:08X}")
    return footer
