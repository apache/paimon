"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import shutil
import tempfile
import unittest
import uuid

from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.api.rest_api import RESTApi, IllegalArgumentError
from pypaimon.catalog.catalog_exception import (
    FunctionNotExistException,
    FunctionAlreadyExistException,
    DefinitionAlreadyExistException,
    DefinitionNotExistException,
)
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.function.function import FunctionImpl
from pypaimon.function.function_change import FunctionChange
from pypaimon.function.function_definition import (
    FunctionDefinition, FunctionFileResource,
)
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.tests.rest.rest_server import RESTCatalogServer


def _mock_function(identifier: Identifier) -> FunctionImpl:
    input_params = [
        DataField(0, "length", AtomicType("DOUBLE")),
        DataField(1, "width", AtomicType("DOUBLE")),
    ]
    return_params = [
        DataField(0, "area", AtomicType("DOUBLE")),
    ]
    flink_function = FunctionDefinition.file(
        file_resources=[FunctionFileResource("jar", "/a/b/c.jar")],
        language="java",
        class_name="className",
        function_name="eval",
    )
    spark_function = FunctionDefinition.lambda_def(
        "(Double length, Double width) -> length * width", "java"
    )
    trino_function = FunctionDefinition.sql("length * width")
    definitions = {
        "flink": flink_function,
        "spark": spark_function,
        "trino": trino_function,
    }
    return FunctionImpl(
        identifier=identifier,
        input_params=input_params,
        return_params=return_params,
        deterministic=False,
        definitions=definitions,
        comment="comment",
        options={},
    )


class RESTFunctionTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="function_test_")
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

    def test_function(self):
        self.catalog.create_database("rest_catalog_db", True)

        identifier_with_slash = Identifier.create("rest_catalog_db", "function/")
        with self.assertRaises(IllegalArgumentError):
            self.catalog.create_function(
                identifier_with_slash,
                _mock_function(identifier_with_slash),
                False,
            )
        with self.assertRaises(FunctionNotExistException):
            self.catalog.get_function(identifier_with_slash)
        with self.assertRaises(IllegalArgumentError):
            self.catalog.drop_function(identifier_with_slash, True)

        identifier_without_alphabet = Identifier.create("rest_catalog_db", "-")
        with self.assertRaises(IllegalArgumentError):
            self.catalog.create_function(
                identifier_without_alphabet,
                _mock_function(identifier_without_alphabet),
                False,
            )
        with self.assertRaises(FunctionNotExistException):
            self.catalog.get_function(identifier_without_alphabet)
        with self.assertRaises(IllegalArgumentError):
            self.catalog.drop_function(identifier_without_alphabet, True)

        identifier = Identifier.from_string("rest_catalog_db.function.na_me-01")
        function = _mock_function(identifier)

        # Drop first to ensure fresh data (not stale from a previous failed run)
        self.catalog.drop_function(identifier, True)
        self.catalog.create_function(identifier, function, True)
        with self.assertRaises(FunctionAlreadyExistException):
            self.catalog.create_function(identifier, function, False)

        self.assertIn(function.name(), self.catalog.list_functions(identifier.get_database_name()))

        get_function = self.catalog.get_function(identifier)
        self.assertEqual(get_function.name(), function.name())
        for dialect in function.definitions().keys():
            self.assertEqual(get_function.definition(dialect), function.definition(dialect))

        self.catalog.drop_function(identifier, True)
        self.assertNotIn(function.name(), self.catalog.list_functions(identifier.get_database_name()))

        with self.assertRaises(FunctionNotExistException):
            self.catalog.drop_function(identifier, False)
        with self.assertRaises(FunctionNotExistException):
            self.catalog.get_function(identifier)

    def test_list_functions(self):
        db1 = "db_rest_catalog_db"
        db2 = "db2_rest_catalog"
        identifier = Identifier.create(db1, "list_function")
        identifier1 = Identifier.create(db1, "function")
        identifier2 = Identifier.create(db2, "list_function")
        identifier3 = Identifier.create(db2, "function")

        self.catalog.create_database(db1, True)
        self.catalog.create_database(db2, True)
        self.catalog.create_function(identifier, _mock_function(identifier), True)
        self.catalog.create_function(identifier1, _mock_function(identifier1), True)
        self.catalog.create_function(identifier2, _mock_function(identifier2), True)
        self.catalog.create_function(identifier3, _mock_function(identifier3), True)

        result = self.catalog.list_functions_paged(db1, None, None, None)
        self.assertEqual(
            set(result.elements),
            {identifier.get_object_name(), identifier1.get_object_name()},
        )

        result = self.catalog.list_functions_paged(db1, 1, None, None)
        self.assertEqual(len(result.elements), 1)
        self.assertIn(
            result.elements[0],
            [identifier.get_object_name(), identifier1.get_object_name()],
        )

        result = self.catalog.list_functions_paged(
            db1, 1, identifier1.get_object_name(), None)
        self.assertEqual(
            result.elements,
            [identifier.get_object_name()],
        )

        result = self.catalog.list_functions_paged(db1, None, None, "func%")
        self.assertEqual(result.elements, [identifier1.get_object_name()])

        result = self.catalog.list_functions_paged_globally("db2_rest%", "func%", None, None)
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(result.elements[0].get_full_name(), identifier3.get_full_name())

        result = self.catalog.list_functions_paged_globally(
            "db2_rest%", None, 1, None)
        self.assertEqual(len(result.elements), 1)
        self.assertIn(
            result.elements[0].get_full_name(),
            [identifier2.get_full_name(), identifier3.get_full_name()],
        )

        result = self.catalog.list_functions_paged_globally(
            "db2_rest%", None, 1, identifier3.get_full_name())
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(
            result.elements[0].get_full_name(),
            identifier2.get_full_name(),
        )

        result = self.catalog.list_function_details_paged(db1, 1, None, None)
        self.assertEqual(len(result.elements), 1)
        self.assertIn(
            result.elements[0].full_name(),
            [identifier.get_full_name(), identifier1.get_full_name()],
        )

        result = self.catalog.list_function_details_paged(db2, 4, None, "func%")
        self.assertEqual(len(result.elements), 1)
        self.assertEqual(
            result.elements[0].full_name(), identifier3.get_full_name())

        result = self.catalog.list_function_details_paged(
            db2, 1, identifier3.get_object_name(), None)
        full_names = [f.full_name() for f in result.elements]
        self.assertIn(identifier2.get_full_name(), full_names)

    def test_alter_function(self):
        identifier = Identifier.create("rest_catalog_db", "alter_function_name")
        self.catalog.create_database(identifier.get_database_name(), True)
        self.catalog.drop_function(identifier, True)
        function = _mock_function(identifier)
        definition = FunctionDefinition.sql("x * y + 1")
        add_definition = FunctionChange.add_definition("flink_1", definition)

        self.catalog.alter_function(identifier, [add_definition], True)

        with self.assertRaises(FunctionNotExistException):
            self.catalog.alter_function(identifier, [add_definition], False)

        self.catalog.create_function(identifier, function, True)

        key = str(uuid.uuid4())
        value = str(uuid.uuid4())
        set_option = FunctionChange.set_option(key, value)
        self.catalog.alter_function(identifier, [set_option], False)
        catalog_function = self.catalog.get_function(identifier)
        self.assertEqual(catalog_function.options().get(key), value)

        self.catalog.alter_function(identifier, [FunctionChange.remove_option(key)], False)
        catalog_function = self.catalog.get_function(identifier)
        self.assertNotIn(key, catalog_function.options())

        new_comment = "new comment"
        self.catalog.alter_function(
            identifier, [FunctionChange.update_comment(new_comment)], False
        )
        catalog_function = self.catalog.get_function(identifier)
        self.assertEqual(catalog_function.comment(), new_comment)

        self.catalog.alter_function(identifier, [add_definition], False)
        catalog_function = self.catalog.get_function(identifier)
        self.assertEqual(
            catalog_function.definition(add_definition.name),
            add_definition.definition,
        )

        with self.assertRaises(DefinitionAlreadyExistException):
            self.catalog.alter_function(identifier, [add_definition], False)

        update_definition = FunctionChange.update_definition("flink_1", definition)
        self.catalog.alter_function(identifier, [update_definition], False)
        catalog_function = self.catalog.get_function(identifier)
        self.assertEqual(
            catalog_function.definition(update_definition.name),
            update_definition.definition,
        )

        with self.assertRaises(DefinitionNotExistException):
            self.catalog.alter_function(
                identifier,
                [FunctionChange.update_definition("no_exist", definition)],
                False,
            )

        drop_definition = FunctionChange.drop_definition(update_definition.name)
        self.catalog.alter_function(identifier, [drop_definition], False)
        catalog_function = self.catalog.get_function(identifier)
        self.assertIsNone(catalog_function.definition(update_definition.name))

        with self.assertRaises(DefinitionNotExistException):
            self.catalog.alter_function(identifier, [drop_definition], False)

    def test_validate_function_name(self):
        self.assertTrue(RESTApi.is_valid_function_name("a"))
        self.assertTrue(RESTApi.is_valid_function_name("a1_"))
        self.assertTrue(RESTApi.is_valid_function_name("a-b_c"))
        self.assertTrue(RESTApi.is_valid_function_name("a-b_c.1"))

        self.assertFalse(RESTApi.is_valid_function_name("a\\/b"))
        self.assertFalse(RESTApi.is_valid_function_name("a$?b"))
        self.assertFalse(RESTApi.is_valid_function_name("a@b"))
        self.assertFalse(RESTApi.is_valid_function_name("a*b"))
        self.assertFalse(RESTApi.is_valid_function_name("123"))
        self.assertFalse(RESTApi.is_valid_function_name("_-"))
        self.assertFalse(RESTApi.is_valid_function_name(""))
        self.assertFalse(RESTApi.is_valid_function_name(None))

        with self.assertRaises(IllegalArgumentError):
            RESTApi.check_function_name("a\\/b")
        with self.assertRaises(IllegalArgumentError):
            RESTApi.check_function_name("123")
        with self.assertRaises(IllegalArgumentError):
            RESTApi.check_function_name("")
        with self.assertRaises(IllegalArgumentError):
            RESTApi.check_function_name(None)


if __name__ == "__main__":
    unittest.main()
