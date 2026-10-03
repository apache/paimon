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


import unittest

from pypaimon.api.resource_paths import ResourcePaths


class ResourcePathsTest(unittest.TestCase):

    def test_url_encode(self):
        database = "test_db"
        object_name = "test_table$snapshot"
        resource_paths = ResourcePaths("paimon")
        self.assertEqual(
            "/v1/paimon/databases/test_db/tables/test_table%24snapshot",
            resource_paths.table(database, object_name))
        resource_paths = ResourcePaths("paimon/aaaa")
        self.assertEqual(
            "/v1/paimon%2Faaaa/databases/test_db/tables/test_table%24snapshot",
            resource_paths.table(database, object_name))

    def test_database_and_table_names_are_url_encoded(self):
        paths = ResourcePaths("catalog/id")
        self.assertEqual(
            "/v1/catalog%2Fid/databases/sales+db/tables/orders%2Fall",
            paths.table("sales db", "orders/all"))
        self.assertEqual(
            "/v1/catalog%2Fid/databases/sales+db/table-details", paths.table_details("sales db"))


if __name__ == '__main__':
    unittest.main()
