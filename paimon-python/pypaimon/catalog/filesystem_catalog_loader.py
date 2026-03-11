#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""
FileSystemCatalogLoader implementation for pypaimon.

This module provides the FileSystemCatalogLoader class which implements the CatalogLoader
interface to create and load FileSystemCatalog instances.
"""

from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.catalog_loader import CatalogLoader
from pypaimon.catalog.filesystem_catalog import FileSystemCatalog


class FileSystemCatalogLoader(CatalogLoader):
    """
    Loader to create FileSystemCatalog instances.

    This class implements the CatalogLoader interface and is responsible for
    creating and configuring FileSystemCatalog instances based on the provided
    CatalogContext.
    """

    def __init__(self, context: CatalogContext):
        """
        Initialize FileSystemCatalogLoader with a CatalogContext.

        Args:
            context: The CatalogContext containing configuration options
        """
        self._context = context

    def context(self) -> CatalogContext:
        """
        Get the CatalogContext associated with this loader.

        Returns:
            The CatalogContext instance
        """
        return self._context

    def load(self) -> FileSystemCatalog:
        """
        Load and return a new FileSystemCatalog instance.

        This method creates a new FileSystemCatalog instance using the stored
        CatalogContext.

        Returns:
            A new FileSystemCatalog instance
        """
        return FileSystemCatalog(self._context.options())
