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

from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.uri_reader import UriReader, UriReaderFactory


class BlobDescriptorReaderFactory:
    """Creates the URI reader factory used for descriptor-backed write input."""

    @staticmethod
    def create(table):
        source_table = table.options.blob_descriptor_source_table()
        if source_table is None:
            descriptor_options = BlobDescriptorReaderFactory._descriptor_options(
                table.options)
            if descriptor_options is None:
                return table.file_io.uri_reader_factory
            factory = UriReaderFactory(descriptor_options)
            # Static blob-descriptor.* options declare descriptor bytes as the
            # write input contract, including v1 descriptors without magic.
            factory.force_descriptor_bytes = True
            factory._blob_descriptor_owned = True
            return factory
        return BlobDescriptorReaderFactory._from_source_table(table, source_table)

    @staticmethod
    def _descriptor_options(options):
        """Return an isolated FileIO configuration for descriptor input.

        This mirrors Java's ``BlobDescriptorUtils.getCatalogContext``: once any
        ``blob-descriptor.*`` option is present, only options under that prefix
        participate in descriptor reads and the prefix is removed before the
        FileIO is created. ``source-table`` is handled before this method and
        therefore always takes precedence.
        """
        prefix = "blob-descriptor."
        raw_options = options.options.to_map()
        descriptor_options = {
            key[len(prefix):]: value
            for key, value in raw_options.items()
            if key is not None and key.startswith(prefix)
        }
        return Options(descriptor_options) if descriptor_options else None

    @staticmethod
    def _from_source_table(table, source_table: str):
        catalog_environment = table.catalog_environment
        catalog_loader = catalog_environment.catalog_loader
        if catalog_loader is None:
            raise ValueError(
                "Option '%s' is not supported for tables without a catalog loader, "
                "including external tables in REST catalogs."
                % CoreOptions.BLOB_DESCRIPTOR_SOURCE_TABLE.key()
            )

        catalog_context = catalog_environment.catalog_context()
        dependency_context = catalog_environment.dependency_read_context()
        catalog = None
        try:
            if dependency_context is catalog_context or dependency_context is None:
                catalog = catalog_loader.load()
            else:
                from pypaimon.catalog.catalog_factory import CatalogFactory
                catalog = CatalogFactory.create_from_context(
                    dependency_context, config_required=False)

            source_identifier = Identifier.from_string(source_table)
            source = catalog.get_table(source_identifier)
            source_file_io = source.file_io
            # Some credential-bearing FileIO implementations initialize their
            # serializable token state lazily on the first capability check.
            initialize = getattr(source_file_io, "is_object_store", None)
            if callable(initialize):
                initialize()
            else:
                # RESTTokenFileIO currently exposes token initialization
                # directly rather than through is_object_store(). Materialize
                # it while the source catalog/auth context is still available.
                initialize = getattr(source_file_io, "valid_token", None)
                if callable(initialize):
                    initialize()
            return _FileIOUriReaderFactory(source_file_io)
        except Exception as e:
            raise RuntimeError(
                "Failed to load BLOB descriptor source table '%s'." % source_table
            ) from e
        finally:
            close = getattr(catalog, "close", None)
            if callable(close):
                close()


class _FileIOUriReaderFactory:
    """A UriReaderFactory which always uses one table-scoped FileIO."""

    # Configuring blob-descriptor.source-table makes serialized descriptor
    # bytes the declared input contract. This lets the PK externalizer use the
    # non-heuristic parser, which is required for v1 descriptors without a
    # magic header.
    force_descriptor_bytes = True
    _blob_descriptor_owned = True

    def __init__(self, file_io):
        self._file_io = file_io

    def create(self, _input_uri: str):
        return UriReader.from_file(self._file_io)

    def close(self):
        close = getattr(self._file_io, "close", None)
        if callable(close):
            close()
