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

from typing import List, Optional, BinaryIO

from pypaimon.schema.data_types import DataField, AtomicType
from .blob_format_reader import BlobFormatReader
from .blob_format_writer import BlobFormatWriter


class BlobFileFormatFactory:
    """Factory to create BlobFileFormat instances."""

    IDENTIFIER = "blob"

    def identifier(self) -> str:
        """Get the format identifier."""
        return self.IDENTIFIER

    def create(self, format_context=None) -> 'BlobFileFormat':
        """
        Create a new BlobFileFormat instance.

        Args:
            format_context: Format context (unused for blob format)

        Returns:
            New BlobFileFormat instance
        """
        return BlobFileFormat()


class BlobFileFormat:
    """
    Python implementation of Paimon's BlobFileFormat.

    This file format is designed specifically for storing blob data efficiently.
    It supports:
    - Single blob field per row
    - Efficient storage with minimal overhead
    - Index-based random access
    - Optional record selection
    """

    def __init__(self):
        """Initialize BlobFileFormat."""
        self.identifier = BlobFileFormatFactory.IDENTIFIER

    @staticmethod
    def is_blob_file(file_name: str) -> bool:
        """
        Check if a file is a blob file based on its extension.

        Args:
            file_name: Name of the file to check

        Returns:
            True if the file has blob extension
        """
        return file_name.endswith(f".{BlobFileFormatFactory.IDENTIFIER}")

    def create_reader_factory(self,
                              data_schema_fields: Optional[List[DataField]] = None,
                              projected_fields: Optional[List[DataField]] = None,
                              filters: Optional[List] = None) -> 'BlobFormatReaderFactory':
        """
        Create a reader factory for blob files.

        Args:
            data_schema_fields: Data schema fields (unused for blob format)
            projected_fields: Projected fields (unused for blob format)
            filters: Filters (unused for blob format)

        Returns:
            BlobFormatReaderFactory instance
        """
        return BlobFormatReaderFactory()

    def create_writer_factory(self, fields: List[DataField]) -> 'BlobFormatWriterFactory':
        """
        Create a writer factory for blob files.

        Args:
            fields: Row type fields

        Returns:
            BlobFormatWriterFactory instance
        """
        return BlobFormatWriterFactory()

    def validate_data_fields(self, fields: List[DataField]) -> None:
        """
        Validate that the data fields are compatible with blob format.

        Args:
            fields: List of data fields to validate

        Raises:
            ValueError: If fields are not compatible with blob format
        """
        if len(fields) != 1:
            raise ValueError("BlobFileFormat only supports one field")

        field = fields[0]
        if not isinstance(field.type, AtomicType) or field.type.type.upper() != "BLOB":
            raise ValueError("BlobFileFormat only supports blob type")

    def create_stats_extractor(self, fields: List[DataField]) -> Optional['EmptyStatsExtractor']:
        """
        Create a statistics extractor for blob files.

        Args:
            fields: Row type fields

        Returns:
            Empty stats extractor (blob files don't need statistics)
        """
        return EmptyStatsExtractor()


class BlobFormatWriterFactory:
    """Factory for creating blob format writers."""

    def create(self, output_stream: BinaryIO, compression: Optional[str] = None) -> BlobFormatWriter:
        """
        Create a blob format writer.

        Args:
            output_stream: Output stream to write to
            compression: Compression type (unused for blob format)

        Returns:
            BlobFormatWriter instance
        """
        return BlobFormatWriter(output_stream)


class BlobFormatReaderFactory:
    """Factory for creating blob format readers."""

    def create_reader(self, context: 'FormatReaderContext') -> BlobFormatReader:
        """
        Create a blob format reader.

        Args:
            context: Reader context containing file information

        Returns:
            BlobFormatReader instance
        """
        return BlobFormatReader(
            context.file_path,
            context.file_size,
            context.selection
        )


class FormatReaderContext:
    """Context for format readers."""

    def __init__(self, file_path: str, file_size: int, selection: Optional[List[int]] = None):
        """
        Initialize format reader context.

        Args:
            file_path: Path to the file to read
            file_size: Size of the file in bytes
            selection: Optional list of record indices to select
        """
        self.file_path = file_path
        self.file_size = file_size
        self.selection = selection


class EmptyStatsExtractor:
    """Empty statistics extractor for blob files."""

    def extract(self, row) -> None:
        """Extract statistics (no-op for blob files)."""
        pass

    def result(self) -> None:
        """Get statistics result (no-op for blob files)."""
        return None
