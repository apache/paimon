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

import shutil
import tempfile
import unittest
import uuid

from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.api.rest_api import RESTApi, IllegalArgumentError
from pypaimon.catalog.catalog_exception import (
    ResourceNotExistException,
    ResourceAlreadyExistException,
)
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.resource.resource import FileResource
from pypaimon.resource.resource_change import ResourceChange
from pypaimon.resource.resource_type import ResourceType
from pypaimon.tests.rest.rest_server import RESTCatalogServer


def _mock_resource(identifier: Identifier) -> FileResource:
    return FileResource(
        identifier=identifier,
        comment="comment",
        uri="/a/b/c.txt",
        size=100,
        last_modified_time=1,
        file_io=None,
    )


class RESTResourceTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="resource_test_")
        self.config = ConfigResponse(defaults={"prefix": "mock-test"})
        self.token = str(uuid.uuid4())
        self.server = RESTCatalogServer(
            data_path=self.temp_dir,
            auth_provider=BearTokenAuthProvider(self.token),
            config=self.config,
            warehouse="warehouse",
        )
        self.server.start()

        options = Options({
            "metastore": "rest",
            "uri": f"http://localhost:{self.server.port}",
            "warehouse": "warehouse",
            "token.provider": "bear",
            "token": self.token,
        })
        self.catalog = RESTCatalog(CatalogContext.create_from_options(options))

    def tearDown(self):
        self.server.shutdown()
        import gc
        gc.collect()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_resource_type(self):
        self.assertEqual(ResourceType.from_value("FILE"), ResourceType.FILE)
        self.assertEqual(ResourceType.from_value("jar"), ResourceType.JAR)
        self.assertEqual(ResourceType.PY.get_value(), "py")
        with self.assertRaises(ValueError):
            ResourceType.from_value("unknown")

    def test_resource(self):
        self.catalog.create_database("rest_catalog_db", True)

        identifier = Identifier.from_string("rest_catalog_db.resource_na_me-01")
        resource = _mock_resource(identifier)

        self.catalog.drop_resource(identifier, True)
        self.catalog.create_resource(identifier, resource, True)
        with self.assertRaises(ResourceAlreadyExistException):
            self.catalog.create_resource(identifier, resource, False)

        self.assertIn(resource.name(),
                      self.catalog.list_resources(identifier.get_database_name()))

        get_resource = self.catalog.get_resource(identifier)
        self.assertEqual(get_resource.name(), resource.name())
        self.assertEqual(get_resource.uri(), resource.uri())
        self.assertEqual(get_resource.comment(), resource.comment())
        self.assertEqual(get_resource.resource_type(), ResourceType.FILE)

        self.catalog.drop_resource(identifier, True)
        self.assertNotIn(resource.name(),
                         self.catalog.list_resources(identifier.get_database_name()))

        with self.assertRaises(ResourceNotExistException):
            self.catalog.drop_resource(identifier, False)
        with self.assertRaises(ResourceNotExistException):
            self.catalog.get_resource(identifier)

    def test_alter_resource(self):
        identifier = Identifier.create("rest_catalog_db", "alter_resource_name")
        self.catalog.create_database(identifier.get_database_name(), True)
        self.catalog.drop_resource(identifier, True)

        with self.assertRaises(ResourceNotExistException):
            self.catalog.alter_resource(
                identifier, [ResourceChange.update_comment("c")], False)

        self.catalog.create_resource(identifier, _mock_resource(identifier), True)

        new_comment = "new comment"
        self.catalog.alter_resource(
            identifier, [ResourceChange.update_comment(new_comment)], False)
        self.assertEqual(self.catalog.get_resource(identifier).comment(), new_comment)

        new_uri = "/x/y/z.txt"
        self.catalog.alter_resource(
            identifier, [ResourceChange.update_uri(new_uri)], False)
        self.assertEqual(self.catalog.get_resource(identifier).uri(), new_uri)

    def test_list_resources(self):
        db1 = "db_rest_catalog_db"
        db2 = "db2_rest_catalog"
        identifier = Identifier.create(db1, "list_resource")
        identifier1 = Identifier.create(db1, "resource")
        identifier2 = Identifier.create(db2, "list_resource")
        identifier3 = Identifier.create(db2, "resource")

        self.catalog.create_database(db1, True)
        self.catalog.create_database(db2, True)
        self.catalog.create_resource(identifier, _mock_resource(identifier), True)
        self.catalog.create_resource(identifier1, _mock_resource(identifier1), True)
        self.catalog.create_resource(identifier2, _mock_resource(identifier2), True)
        self.catalog.create_resource(identifier3, _mock_resource(identifier3), True)

        result = self.catalog.list_resources_paged(db1, None, None, None)
        self.assertEqual(
            set(result.elements),
            {identifier.get_object_name(), identifier1.get_object_name()},
        )

        result = self.catalog.list_resources_paged(db1, None, None, "res%")
        self.assertEqual(result.elements, [identifier1.get_object_name()])

        result = self.catalog.list_resources_paged_globally("db2_rest%", "res%", None, None)
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(result.elements[0].get_full_name(), identifier3.get_full_name())

        result = self.catalog.list_resource_details_paged(db2, 4, None, "res%")
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(result.elements[0].full_name(), identifier3.get_full_name())


if __name__ == "__main__":
    unittest.main()

