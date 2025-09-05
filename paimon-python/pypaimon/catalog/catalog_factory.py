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
from typing import Dict

from pypaimon.api.options import Options
from pypaimon.catalog.catalog import Catalog
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.config import CatalogOptions


class CatalogFactory:

    CATALOG_REGISTRY = {
        "filesystem": FileSystemCatalog,
        "rest": RESTCatalog,
    }

    @staticmethod
    def create(catalog_options: Dict) -> Catalog:
        identifier = catalog_options.get(CatalogOptions.METASTORE, "filesystem")
        catalog_class = CatalogFactory.CATALOG_REGISTRY.get(identifier)
        if catalog_class is None:
            raise ValueError("Unknown catalog identifier: {}. "
                             "Available types: {}".format(identifier, list(CatalogFactory.CATALOG_REGISTRY.keys())))
        return catalog_class(
            CatalogContext.create_from_options(Options(catalog_options))) if identifier == "rest" else catalog_class(
            catalog_options)
