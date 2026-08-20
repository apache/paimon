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

import json
import unittest

from pypaimon.api.api_request import CreatePartitionsRequest
from pypaimon.api.api_response import CreatePartitionsResponse
from pypaimon.api.resource_paths import ResourcePaths
from pypaimon.common.json_util import JSON


class CreatePartitionsRequestSerdeTest(unittest.TestCase):

    def test_to_json_uses_java_field_names(self):
        request = CreatePartitionsRequest(
            partition_specs=[{"dt": "20260807", "hour": "01"}],
        )
        parsed = json.loads(JSON.to_json(request))
        self.assertEqual(parsed["partitionSpecs"], [{"dt": "20260807", "hour": "01"}])
        self.assertEqual(set(parsed.keys()), {"partitionSpecs", "ignoreIfExists"})
        self.assertNotIn("partition_specs", parsed)
        self.assertNotIn("ignore_if_exists", parsed)

    def test_ignore_if_exists_defaults_to_true(self):
        request = CreatePartitionsRequest(partition_specs=[{"dt": "20260807"}])
        self.assertIs(json.loads(JSON.to_json(request))["ignoreIfExists"], True)

    def test_ignore_if_exists_false_is_serialized(self):
        request = CreatePartitionsRequest(
            partition_specs=[{"dt": "20260807"}],
            ignore_if_exists=False,
        )
        self.assertIs(json.loads(JSON.to_json(request))["ignoreIfExists"], False)

    def test_explicit_null_ignore_if_exists_reads_as_true(self):
        request = JSON.from_json(
            json.dumps({"partitionSpecs": [{"dt": "20260807"}], "ignoreIfExists": None}),
            CreatePartitionsRequest,
        )
        self.assertIs(request.ignore_if_exists, True)

    def test_multiple_specs_keep_order_and_all_keys(self):
        specs = [
            {"dt": "20260807", "hour": "01"},
            {"dt": "20260807", "hour": "02"},
            {"dt": "20260808", "hour": "00"},
        ]
        parsed = json.loads(JSON.to_json(CreatePartitionsRequest(partition_specs=specs)))
        self.assertEqual(parsed["partitionSpecs"], specs)


class CreatePartitionsResponseSerdeTest(unittest.TestCase):

    def test_from_json_splits_created_and_existed(self):
        response = JSON.from_json(
            json.dumps({
                "created": [{"dt": "20260807"}],
                "existed": [{"dt": "20260806"}],
            }),
            CreatePartitionsResponse,
        )
        self.assertEqual(response.created, [{"dt": "20260807"}])
        self.assertEqual(response.existed, [{"dt": "20260806"}])

    def test_from_json_tolerates_missing_fields(self):
        response = JSON.from_json(json.dumps({"created": []}), CreatePartitionsResponse)
        self.assertEqual(response.created, [])
        self.assertIsNone(response.existed)


class ResourcePathsPartitionsTest(unittest.TestCase):

    def test_partitions_collection_url(self):
        paths = ResourcePaths(prefix="mock")
        self.assertEqual(
            paths.partitions("db", "tbl"),
            "/v1/mock/databases/db/tables/tbl/partitions",
        )

    def test_partitions_url_url_encodes_names(self):
        paths = ResourcePaths(prefix="mock")
        self.assertEqual(
            paths.partitions("my db", "my tbl"),
            "/v1/mock/databases/my%20db/tables/my%20tbl/partitions",
        )

if __name__ == "__main__":
    unittest.main()
