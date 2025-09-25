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

"""
RESTCatalogLoader implementation for pypaimon.

This module provides the RESTCatalogLoader class which implements the CatalogLoader
interface to create and load RESTCatalog instances.
"""
from pypaimon.api.options import Options
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.catalog_loader import CatalogLoader
from pypaimon.catalog.rest.rest_catalog import RESTCatalog


class RESTCatalogLoader(CatalogLoader):
    """
    Loader to create RESTCatalog instances.

    This class implements the CatalogLoader interface and is responsible for
    creating and configuring RESTCatalog instances based on the provided
    CatalogContext.
    """

    def __init__(self, context: CatalogContext):
        """
        Initialize RESTCatalogLoader with a CatalogContext.

        Args:
            context: The CatalogContext containing configuration options
        """
        self._context = context
        self._options = context.options

    def context(self) -> CatalogContext:
        return self._context

    def options(self) -> Options:
        return self._options

    def load(self) -> RESTCatalog:
        """
        Load and return a new RESTCatalog instance.

        This method creates a new RESTCatalog instance using the stored
        CatalogContext, with config_required set to False to avoid
        redundant configuration validation.

        Returns:
            A new RESTCatalog instance
        """
        return RESTCatalog(self._context, config_required=False)
