# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from types import SimpleNamespace
from unittest import mock

from pypaimon.api.rest_api import RESTApi
from pypaimon.api.rest_util import RESTUtil
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.catalog.catalog_environment import CatalogEnvironment
from pypaimon.catalog.filesystem_catalog_loader import FileSystemCatalogLoader
from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader
from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import JSON
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.utils.blob_view_lookup import BlobViewLookup


class CatalogEnvironmentTest(unittest.TestCase):

    _READ_VIA_OPTION = RESTApi.HEADER_PREFIX + RESTApi.READ_VIA_HEADER

    def test_dependency_read_context_for_rest_catalog(self):
        root = Identifier.create("db", "root", branch="dev")
        options = Options({"other-option": "value"})
        context = CatalogContext.create_from_options(options)
        environment = CatalogEnvironment(
            identifier=root,
            catalog_loader=RESTCatalogLoader(context),
        )

        dependency_context = environment.dependency_read_context()

        self.assertIsNot(dependency_context, context)
        self.assertFalse(context.options.contains_key(self._READ_VIA_OPTION))
        self.assertEqual(
            dependency_context.options.get(CatalogOptions.METASTORE), "rest")
        self.assertEqual(
            dependency_context.options.to_map()["other-option"], "value")
        read_via = JSON.from_json(
            RESTUtil.decode_string(
                dependency_context.options.to_map()[self._READ_VIA_OPTION]),
            Identifier,
        )
        self.assertEqual(read_via, root)

    def test_dependency_read_context_preserves_outermost_table(self):
        outermost = Identifier.create("db", "outermost")
        read_via = RESTUtil.encode_string(
            JSON.to_json(outermost, separators=(",", ":")))
        context = CatalogContext.create_from_options(Options({
            CatalogOptions.METASTORE.key(): "rest",
            self._READ_VIA_OPTION: read_via,
        }))
        environment = CatalogEnvironment(
            identifier=Identifier.create("db", "intermediate"),
            catalog_loader=RESTCatalogLoader(context),
        )

        self.assertIs(environment.dependency_read_context(), context)
        self.assertEqual(
            context.options.to_map()[self._READ_VIA_OPTION], read_via)

    def test_dependency_read_context_does_not_affect_other_catalogs(self):
        context = CatalogContext.create_from_options(Options({}))
        environment = CatalogEnvironment(
            identifier=Identifier.create("db", "table"),
            catalog_loader=FileSystemCatalogLoader(context),
        )

        self.assertIs(environment.dependency_read_context(), context)
        self.assertFalse(context.options.contains_key(self._READ_VIA_OPTION))

    def test_dependency_read_context_for_external_rest_table(self):
        context = CatalogContext.create_from_options(Options({
            CatalogOptions.METASTORE.key(): "rest",
        }))
        environment = CatalogEnvironment(
            identifier=Identifier.create("db", "external"),
            catalog_loader=FileSystemCatalogLoader(context),
        )

        self.assertIsNot(environment.dependency_read_context(), context)

    def test_dependency_read_context_preserves_custom_rest_metastore(self):
        context = CatalogContext.create_from_options(Options({
            CatalogOptions.METASTORE.key(): "custom-rest",
        }))
        environment = CatalogEnvironment(
            identifier=Identifier.create("db", "table"),
            catalog_loader=RESTCatalogLoader(context),
        )

        dependency_context = environment.dependency_read_context()

        self.assertIsNot(dependency_context, context)
        self.assertEqual(
            dependency_context.options.get(CatalogOptions.METASTORE),
            "custom-rest",
        )

    def test_catalog_factory_creates_rest_from_context(self):
        context = CatalogContext.create_from_options(Options({
            CatalogOptions.METASTORE.key(): "rest",
        }))
        dependency_catalog = mock.sentinel.dependency_catalog
        rest_catalog = mock.Mock(return_value=dependency_catalog)

        with mock.patch.dict(
                CatalogFactory.CATALOG_REGISTRY, {"rest": rest_catalog}):
            result = CatalogFactory.create_from_context(
                context, config_required=False)

        rest_catalog.assert_called_once_with(context, config_required=False)
        self.assertIs(result, dependency_catalog)

    def test_blob_view_lookup_loads_dependency_catalog(self):
        root = Identifier.create("db", "root")
        target = Identifier.create("db", "target")
        context = CatalogContext.create_from_options(Options({
            CatalogOptions.METASTORE.key(): "rest",
        }))
        original_loader = RESTCatalogLoader(context)
        environment = CatalogEnvironment(
            identifier=root,
            catalog_loader=original_loader,
        )
        table = SimpleNamespace(catalog_environment=environment)
        dependency_catalog = mock.MagicMock()
        dependency_table = mock.sentinel.dependency_table
        dependency_catalog.get_table.return_value = dependency_table

        with mock.patch.object(
                CatalogFactory,
                "create_from_context",
                return_value=dependency_catalog) as create_catalog:
            result = BlobViewLookup(table)._load_table(target)

        dependency_context = create_catalog.call_args.args[0]
        create_catalog.assert_called_once_with(
            dependency_context, config_required=False)
        self.assertTrue(
            dependency_context.options.contains_key(self._READ_VIA_OPTION))
        dependency_catalog.get_table.assert_called_once_with(target)
        self.assertIs(result, dependency_table)

    def test_blob_view_lookup_preserves_custom_rest_catalog(self):
        target = Identifier.create("db", "target")
        dependency_table = mock.sentinel.dependency_table

        class CustomRESTCatalog:
            context = None

            def __init__(self, context):
                CustomRESTCatalog.context = context

            def get_table(self, identifier):
                self.identifier = identifier
                return dependency_table

        context = CatalogContext.create_from_options(Options({
            CatalogOptions.METASTORE.key(): "custom-rest",
        }))
        environment = CatalogEnvironment(
            identifier=Identifier.create("db", "root"),
            catalog_loader=RESTCatalogLoader(context),
        )
        table = SimpleNamespace(catalog_environment=environment)

        with mock.patch.dict(
                CatalogFactory.CATALOG_REGISTRY,
                {"custom-rest": CustomRESTCatalog}):
            result = BlobViewLookup(table)._load_table(target)

        dependency_context = CustomRESTCatalog.context
        self.assertEqual(
            dependency_context.options.get(CatalogOptions.METASTORE),
            "custom-rest",
        )
        self.assertTrue(
            dependency_context.options.contains_key(self._READ_VIA_OPTION))
        self.assertIs(result, dependency_table)


if __name__ == "__main__":
    unittest.main()
