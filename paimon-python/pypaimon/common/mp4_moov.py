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

"""Locate a top-level moov box while copying encoded video bytes."""

import struct


class MoovLocator:
    def __init__(self):
        self.position = 0
        self.next_box = 0
        self.header = bytearray()
        self.header_size = 8
        self.found = None
        self.valid = True

    def feed(self, data):
        end = self.position + len(data)
        while self.valid and self.next_box is not None and self.next_box < end:
            start = max(0, self.next_box + len(self.header) - self.position)
            needed = self.header_size - len(self.header)
            self.header.extend(data[start:start + needed])
            if len(self.header) < self.header_size:
                break
            size, kind = struct.unpack_from(">I4s", self.header)
            if size == 1:
                self.header_size = 16
                if len(self.header) < 16:
                    continue
                size = struct.unpack_from(">Q", self.header, 8)[0]
            if size == 0 and self.header_size == 16:
                self.valid = False
                break
            if size == 0:
                # A zero-sized box extends to EOF, resolved by finish().
                if kind == b"moov" and self.found is None:
                    self.found = (self.next_box, 0)
                elif kind == b"moov":
                    self.valid = False
                self.next_box = None
                break
            if size < self.header_size:
                self.valid = False
                break
            if kind == b"moov":
                if self.found is not None:
                    self.valid = False
                    break
                self.found = (self.next_box, size)
            self.next_box += size
            self.header.clear()
            self.header_size = 8
        self.position = end

    def finish(self):
        if (not self.valid or self.found is None
                or (self.next_box is not None
                    and (self.next_box != self.position or self.header))):
            return (0, 0)
        offset, size = self.found
        return offset, size or self.position - offset
