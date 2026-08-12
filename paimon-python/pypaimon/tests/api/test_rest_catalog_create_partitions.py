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

from pypaimon.api.rest_exception import (AlreadyExistsException, BadRequestException,
                                         ForbiddenException, NoSuchResourceException)
from pypaimon.catalog.catalog_exception import (IllegalArgumentError, IllegalStateError,
                                                TableNoPermissionException,
                                                TableNotExistException)
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier


class _RecordingApi:
    """Stub RESTApi: records create_partitions calls, optionally raises."""

    def __init__(self, raises=None):
        self.calls = []
        self._raises = raises

    def create_partitions(self, identifier, partitions, ignore_if_exists):
        self.calls.append((identifier, partitions, ignore_if_exists))
        if self._raises is not None:
            raise self._raises
        return None


def _catalog_with(api):
    catalog = object.__new__(RESTCatalog)
    catalog.rest_api = api
    return catalog


class CreatePartitionsBehaviourTest(unittest.TestCase):

    def test_specs_and_flag_reach_the_api_unchanged(self):
        api = _RecordingApi()
        specs = [{"dt": "20260807", "hour": "01"}]
        _catalog_with(api).create_partitions("db.tbl", specs)

        self.assertEqual(len(api.calls), 1)
        identifier, sent, ignore_if_exists = api.calls[0]
        self.assertEqual(identifier.get_full_name(), "db.tbl")
        self.assertEqual(sent, specs)
        self.assertTrue(ignore_if_exists)

    def test_ignore_if_exists_false_is_passed_through(self):
        api = _RecordingApi()
        _catalog_with(api).create_partitions(
            Identifier.from_string("db.tbl"), [{"dt": "20260807"}], ignore_if_exists=False)
        self.assertFalse(api.calls[0][2])

    def test_empty_partitions_are_still_sent(self):
        api = _RecordingApi()
        self.assertIsNone(_catalog_with(api).create_partitions("db.tbl", []))
        self.assertEqual(api.calls[0][1], [])

    def test_string_identifier_is_parsed(self):
        api = _RecordingApi()
        _catalog_with(api).create_partitions("db.tbl", [{"dt": "1"}])
        self.assertIsInstance(api.calls[0][0], Identifier)


class CreatePartitionsErrorMappingTest(unittest.TestCase):
    """Mirrors the four handlers in Java ``RESTCatalog.createPartitions``."""

    def _raise_and_assert(self, rest_error, expected):
        catalog = _catalog_with(_RecordingApi(raises=rest_error))
        with self.assertRaises(expected):
            catalog.create_partitions("db.tbl", [{"dt": "20260807"}])

    def test_no_such_resource_becomes_table_not_exist(self):
        self._raise_and_assert(
            NoSuchResourceException(None, None, "no table"), TableNotExistException)

    def test_forbidden_becomes_table_no_permission(self):
        self._raise_and_assert(ForbiddenException("denied"), TableNoPermissionException)

    def test_already_exists_becomes_illegal_state(self):
        self._raise_and_assert(AlreadyExistsException(None, None, "exists"), IllegalStateError)

    def test_bad_request_becomes_illegal_argument(self):
        self._raise_and_assert(BadRequestException("bad"), IllegalArgumentError)

if __name__ == "__main__":
    unittest.main()
