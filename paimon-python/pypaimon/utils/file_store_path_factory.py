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
from datetime import date, timezone
from decimal import Decimal
from typing import List, Optional, Tuple

from pypaimon.casting.row_to_string import cast_value_to_string, _format_timestamp, _is_unsupported
from pypaimon.common.external_path_provider import ExternalPathProvider
from pypaimon.schema.data_types import DataType
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.table.row.generic_row import _is_ltz_type, _normalize_ltz, _parse_type_precision_scale


def _is_null_or_whitespace_only(value) -> bool:
    if value is None:
        return True
    s = str(value)
    return len(s) == 0 or s.isspace()


def _escape_partition_component(value: str) -> str:
    # Java PartitionPathUtils.CHAR_TO_ESCAPE (spaces and Unicode stay as-is).
    escape_chars = "\"#%'*/:=?\\{}[]^"
    return ''.join('%{:02X}'.format(ord(char))
                   if ord(char) < 32 or ord(char) == 127 or char in escape_chars else char
                   for char in value)


def _floating_partition_string(value, single_precision: bool) -> str:
    # Use a shortest round-tripping form for the initial lookup. Older JVMs
    # can use different digits; the read fallback matches their stored values.
    encoding = '>f' if single_precision else '>d'
    bits = struct.pack(encoding, value)
    value = struct.unpack(encoding, bits)[0]
    for precision in range(2, 10 if single_precision else 18):
        text = format(value, '.{}g'.format(precision))
        try:
            rounded = struct.pack(encoding, float(text))
        except OverflowError:
            continue
        if rounded != bits:
            continue
        decimal = Decimal(text)
        if not decimal.is_finite():
            return str(value)
        if decimal.is_zero() or Decimal('0.001') <= abs(decimal) < Decimal('1e7'):
            text = format(decimal, 'f')
            if '.' in text:
                text = text.rstrip('0').rstrip('.')
            if '.' not in text:
                text += '.0'
        else:
            mantissa, exponent = format(decimal, 'e').split('e')
            mantissa = mantissa.rstrip('0').rstrip('.') if '.' in mantissa else mantissa
            if '.' not in mantissa:
                mantissa += '.0'
            text = '{}E{}'.format(mantissa, int(exponent))
        return text
    return str(value)


class FileStorePathFactory:
    MANIFEST_PATH = "manifest"
    MANIFEST_PREFIX = "manifest-"
    MANIFEST_LIST_PREFIX = "manifest-list-"
    INDEX_MANIFEST_PREFIX = "index-manifest-"

    INDEX_PATH = "index"
    INDEX_PREFIX = "index-"

    STATISTICS_PATH = "statistics"
    STATISTICS_PREFIX = "stat-"

    BUCKET_PATH_PREFIX = "bucket-"

    def __init__(
        self,
        root: str,
        partition_keys: List[str],
        default_part_value: str,
        format_identifier: str,
        data_file_prefix: str,
        changelog_file_prefix: str,
        legacy_partition_name: bool,
        file_suffix_include_compression: bool,
        file_compression: str,
        data_file_path_directory: Optional[str] = None,
        external_paths: Optional[List[str]] = None,
        external_path_strategy: str = "round-robin",
        external_path_weights: Optional[List[int]] = None,
        index_file_in_data_file_dir: bool = False,
        global_index_external_path: Optional[str] = None,
        partition_types: Optional[List[DataType]] = None,
    ):
        self._root = root.rstrip('/')
        self.partition_keys = partition_keys
        self.partition_types = partition_types
        self.default_part_value = default_part_value
        self.format_identifier = format_identifier
        self.data_file_prefix = data_file_prefix
        self.changelog_file_prefix = changelog_file_prefix
        self.file_suffix_include_compression = file_suffix_include_compression
        self.file_compression = file_compression
        self.data_file_path_directory = data_file_path_directory
        self.external_paths = external_paths or []
        self.external_path_strategy = external_path_strategy
        self.external_path_weights = external_path_weights
        self.index_file_in_data_file_dir = index_file_in_data_file_dir
        self.legacy_partition_name = legacy_partition_name
        self.global_index_external_path = (
            global_index_external_path.rstrip('/')
            if global_index_external_path else None)

    def root(self) -> str:
        return self._root

    def manifest_path(self) -> str:
        return f"{self._root}/{self.MANIFEST_PATH}"

    def index_path(self) -> str:
        return f"{self._root}/{self.INDEX_PATH}"

    def global_index_root_path(self) -> str:
        return self.global_index_external_path or self.index_path()

    def statistics_path(self) -> str:
        return f"{self._root}/{self.STATISTICS_PATH}"

    def data_file_path(self) -> str:
        if self.data_file_path_directory:
            return f"{self._root}/{self.data_file_path_directory}"
        return self._root

    def relative_bucket_path(self, partition: Tuple, bucket: int, canonical_partition: bool = False) -> str:
        if canonical_partition and partition:
            partition = self._canonical_partition(partition)
        return self._relative_bucket_path(partition, bucket, canonical_partition)

    def _canonical_partition(self, partition: Tuple) -> Tuple[str, ...]:
        values = []
        for i, value in enumerate(partition):
            data_type = self.partition_types[i] if self.partition_types is not None else None
            type_name = str(data_type).split('(', 1)[0].split()[0]
            if value is not None and _is_ltz_type(str(data_type).upper()):
                # Legacy Timestamp.toString() uses UTC fields. Java's non-legacy
                # cast uses TimeZone.getDefault(); neither includes an offset.
                value = _normalize_ltz(value)
                if not self.legacy_partition_name:
                    value = value.replace(tzinfo=timezone.utc).astimezone().replace(tzinfo=None)
            if _is_null_or_whitespace_only(value):
                text = self.default_part_value
            elif type_name in ('FLOAT', 'REAL', 'DOUBLE'):
                text = _floating_partition_string(value, type_name != 'DOUBLE')
            elif self.legacy_partition_name and type_name == 'DATE':
                text = str((value - date(1970, 1, 1)).days)
            elif self.legacy_partition_name and type_name.startswith('TIMESTAMP'):
                text = value.isoformat(timespec='minutes')
                if value.second or value.microsecond:
                    text = value.isoformat(timespec='microseconds' if value.microsecond else 'seconds')
                    if value.microsecond and value.microsecond % 1000 == 0:
                        text = text[:-3]
            elif type_name.startswith('TIMESTAMP'):
                precision, _ = _parse_type_precision_scale(data_type)
                text = _format_timestamp(value, precision)
            elif self.legacy_partition_name and type_name.startswith('TIME'):
                text = str(((value.hour * 60 + value.minute) * 60 + value.second) * 1000
                           + value.microsecond // 1000)
            elif data_type is not None and not _is_unsupported(data_type):
                text = cast_value_to_string(value, data_type)
            else:
                text = str(value).lower() if isinstance(value, bool) else str(value)
            values.append(text)
        return tuple(values)

    def _relative_bucket_path(self, partition: Tuple, bucket: int, canonical_partition: bool) -> str:
        bucket_name = str(bucket)
        if bucket == BucketMode.POSTPONE_BUCKET.value:
            bucket_name = "postpone"

        relative_parts = [f"{self.BUCKET_PATH_PREFIX}{bucket_name}"]

        # Add partition path
        if partition:
            partition_parts = []
            for i, field_name in enumerate(self.partition_keys):
                val = partition[i]
                if _is_null_or_whitespace_only(val):
                    val = self.default_part_value
                else:
                    val = str(val).lower() if canonical_partition and isinstance(val, bool) else str(val)
                if canonical_partition:
                    field_name = _escape_partition_component(field_name)
                    val = _escape_partition_component(val)
                partition_parts.append(f"{field_name}={val}")
            if partition_parts:
                relative_parts = partition_parts + relative_parts

        # Add data file path directory if specified
        if self.data_file_path_directory:
            relative_parts = [self.data_file_path_directory] + relative_parts

        return "/".join(relative_parts)

    def bucket_path(self, partition: Tuple, bucket: int, canonical_partition: bool = False) -> str:
        relative_path = self.relative_bucket_path(partition, bucket, canonical_partition)
        return f"{self._root}/{relative_path}"

    def create_external_path_provider(
        self, partition: Tuple, bucket: int
    ) -> Optional[ExternalPathProvider]:
        if not self.external_paths:
            return None

        relative_bucket_path = self.relative_bucket_path(partition, bucket)
        return ExternalPathProvider.create(
            self.external_path_strategy,
            self.external_paths,
            relative_bucket_path,
            self.external_path_weights,
        )

    def global_index_path_factory(self) -> 'IndexPathFactory':
        return IndexPathFactory(
            self.index_path(),
            self.global_index_root_path(),
            self.global_index_external_path is not None,
        )

    def new_bucket_index_path(self, partition: Tuple, bucket: int, file_name: str) -> Tuple[str, bool]:
        """Return a new bucket index's path and whether to persist its external location."""
        if self.index_file_in_data_file_dir:
            external = self.create_external_path_provider(partition, bucket)
            if external is not None:
                return external.get_next_external_data_path(file_name), True
            # Python data directories historically use str(value) without
            # escaping. Record the actual location when Java renders it
            # differently, so its readers can find the DV beside those files.
            return (f"{self.bucket_path(partition, bucket)}/{file_name}",
                    self._partition_path_requires_explicit_location(partition))
        factory = self.global_index_path_factory()
        return factory.to_path(file_name), factory.is_external_path()

    def _partition_path_requires_explicit_location(self, partition: Tuple) -> bool:
        # FLOAT/DOUBLE spellings also vary between JVM versions, so persist
        # their actual Python location even if one Java spelling matches it.
        return (any(isinstance(value, float) for value in partition)
                or self.relative_bucket_path(partition, 0) != self.relative_bucket_path(partition, 0, True))

    def bucket_index_path(self, partition: Tuple, bucket: int, index_file, file_io=None) -> str:
        """Resolve an existing bucket index, including the legacy Python DV layout."""
        if index_file.external_path:
            return index_file.external_path
        legacy_path = f"{self.index_path()}/{index_file.file_name}"
        if not self.index_file_in_data_file_dir:
            return legacy_path
        path = f"{self.bucket_path(partition, bucket, True)}/{index_file.file_name}"
        # Older Python DV writers ignored the option. Prefer the Java location
        # when present, and use the old directory only for an existing DV file.
        if file_io is not None and index_file.index_type == 'DELETION_VECTORS' and not file_io.exists(path):
            python_path = f"{self.bucket_path(partition, bucket)}/{index_file.file_name}"
            alternate = self._find_floating_bucket_index(partition, bucket, index_file.file_name, file_io, python_path)
            if alternate is not None:
                return alternate
            if python_path != path and file_io.exists(python_path):
                return python_path
            if file_io.exists(legacy_path):
                return legacy_path
        return path

    def _find_floating_bucket_index(self, partition, bucket, file_name, file_io, python_path):
        floating = [str(data_type).split()[0] in ('FLOAT', 'REAL', 'DOUBLE')
                    for data_type in self.partition_types or []]
        if not any(is_float and value is not None for is_float, value in zip(floating, partition)):
            return None
        # Float/Double.toString changed across JDK releases. Only if the usual
        # path is missing, inspect floating partition components and compare
        # their exact encoded values (including the sign of zero).
        paths = [self.data_file_path()]
        for i, text in enumerate(self._canonical_partition(partition)):
            prefix = _escape_partition_component(self.partition_keys[i]) + '='
            if not floating[i] or partition[i] is None:
                paths = [path + '/' + prefix + _escape_partition_component(text) for path in paths]
                continue
            encoding = '>d' if str(self.partition_types[i]).split()[0] == 'DOUBLE' else '>f'
            expected = struct.pack(encoding, partition[i])
            matched = []
            for path in paths:
                if not file_io.exists(path):
                    continue
                for status in file_io.list_status(path):
                    name = status.base_name
                    if not name.startswith(prefix):
                        continue
                    try:
                        if struct.pack(encoding, float(name[len(prefix):])) == expected:
                            matched.append(path + '/' + name)
                    except (ValueError, OverflowError):
                        continue
            paths = sorted(matched)
        bucket_name = 'postpone' if bucket == BucketMode.POSTPONE_BUCKET.value else str(bucket)
        for path in paths:
            candidate = '{}/{}{}/{}'.format(path, self.BUCKET_PATH_PREFIX, bucket_name, file_name)
            if candidate != python_path and file_io.exists(candidate):
                return candidate
        return None


class IndexPathFactory:

    def __init__(
        self,
        index_path: str,
        global_index_root_path: Optional[str] = None,
        external_path: bool = False,
    ):
        self._index_path = index_path
        self._global_index_root_path = global_index_root_path or index_path
        self._external_path = external_path
        self._file_count = 0

    def index_path(self) -> str:
        """Return the table index path used as a read fallback."""
        return self._index_path

    def global_index_root_path(self) -> str:
        """Return the root path for newly written global index files."""
        return self._global_index_root_path

    def to_path(self, file_name: str) -> str:
        """Convert a file name to a full path."""
        return f"{self._global_index_root_path}/{file_name}"

    def new_path(self, prefix: str = "index-") -> str:
        """Create a new unique index file path."""
        import uuid
        unique_id = str(uuid.uuid4())
        self._file_count += 1
        return self.to_path(f"{prefix}{unique_id}-{self._file_count}")

    def is_external_path(self) -> bool:
        """Return whether this is an external path."""
        return self._external_path
