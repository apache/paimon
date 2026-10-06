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

"""File-scoped I/O and decoding recovery, as in Java DataFileRecordReader."""

import logging
import struct

import pyarrow as pa

from pypaimon.read.reader.iface.record_batch_reader import (
    EmptyRecordBatchReader, RecordBatchReader)

logger = logging.getLogger(__name__)
_PROTECTED_ARROW_ERRORS = tuple(
    error_type for error_type in (
        getattr(pa, 'ArrowMemoryError', None), getattr(pa, 'ArrowCapacityError', None),
        getattr(pa, 'ArrowNotImplementedError', None)) if error_type is not None)
_FILE_ERRORS = (OSError, EOFError, UnicodeError, struct.error, pa.ArrowException)


class FileReadRecovery:
    def __init__(self, file_io, path, options):
        self._file_io = file_io
        self._path = path
        self._ignore_lost = options.scan_ignore_lost_file()
        self._ignore_corrupt = options.scan_ignore_corrupt_file()

    def create_reader(self, factory):
        try:
            reader = factory()
        except _FILE_ERRORS as error:
            if isinstance(error, _PROTECTED_ARROW_ERRORS):
                raise
            if not (self._ignore_lost or self._ignore_corrupt):
                raise
            # Permission/status errors must propagate; ignore-corrupt alone
            # never skips an absent file during reader creation.
            exists = self._file_io.exists(self._path)
            if (exists and self._ignore_corrupt) or (not exists and self._ignore_lost):
                logger.warning('Skipping unreadable data file %s: %s', self._path, error)
                return EmptyRecordBatchReader()
            raise
        return _RecoveringFormatReader(reader, self._path) if self._ignore_corrupt else reader


class _RecoveringFormatReader(RecordBatchReader):
    def __init__(self, inner, path):
        self._inner = inner
        self._path = path
        self._exhausted = False
        self._adopt_metadata(inner)

    def read_arrow_batch(self):
        if self._exhausted:
            return None
        try:
            return self._inner.read_arrow_batch()
        except _FILE_ERRORS as error:
            if isinstance(error, _PROTECTED_ARROW_ERRORS):
                raise
            logger.warning('Stopping unreadable data file %s: %s', self._path, error)
            self._exhausted = True
            return None

    def close(self):
        self._exhausted = True
        self._inner.close()
