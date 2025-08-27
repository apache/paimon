#!/usr/bin/env python3

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

import time
import unittest
from unittest.mock import Mock, patch

from pypaimon.api.api_response import CommitTableResponse
from pypaimon.api.options import Options
from pypaimon.api.rest_exception import NoSuchResourceException
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.catalog_exception import TableNotExistException
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import PartitionStatistics


class TestRESTCatalogCommitSnapshot(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures."""
        # Create mock options for testing
        self.test_options = {
            "warehouse": "s3://test-bucket/warehouse",
            "uri": "http://localhost:8080",
            "authentication.type": "none"
        }

        # Create mock CatalogContext
        self.catalog_context = CatalogContext.create_from_options(Options(self.test_options))

        # Create test identifier
        self.identifier = Identifier.create("test_db", "test_table")

        # Create test snapshot
        self.test_snapshot = Snapshot(
            version=3,
            id=1,
            schema_id=0,
            base_manifest_list="manifest-list-1",
            delta_manifest_list="manifest-list-1",
            commit_user="test_user",
            commit_identifier=12345,
            commit_kind="APPEND",
            time_millis=int(time.time() * 1000),
            log_offsets={}
        )

        # Create test statistics
        self.test_statistics = [
            PartitionStatistics.create({"partition": "2024-01-01"}, 1000, 5)
        ]

    def test_rest_catalog_supports_version_management(self):
        """Test that RESTCatalog supports version management."""
        with patch('pypaimon.catalog.rest.rest_catalog.RESTApi') as mock_rest_api:
            # Configure mock
            mock_api_instance = Mock()
            mock_api_instance.options = self.test_options
            mock_rest_api.return_value = mock_api_instance

            # Create RESTCatalog
            catalog = RESTCatalog(self.catalog_context)

            # Verify supports version management
            self.assertTrue(catalog.supports_version_management())

    def test_rest_catalog_commit_snapshot_success(self):
        """Test successful snapshot commit."""
        with patch('pypaimon.catalog.rest.rest_catalog.RESTApi') as mock_rest_api:
            # Configure mock
            mock_api_instance = Mock()
            mock_api_instance.options = self.test_options
            mock_api_instance.commit_snapshot.return_value = True
            mock_rest_api.return_value = mock_api_instance

            # Create RESTCatalog
            catalog = RESTCatalog(self.catalog_context)

            # Test commit snapshot
            result = catalog.commit_snapshot(
                self.identifier,
                "test-uuid",
                self.test_snapshot,
                self.test_statistics
            )

            # Verify result
            self.assertTrue(result)

            # Verify API was called correctly
            mock_api_instance.commit_snapshot.assert_called_once_with(
                self.identifier,
                "test-uuid",
                self.test_snapshot,
                self.test_statistics
            )

    def test_rest_catalog_commit_snapshot_table_not_exist(self):
        """Test snapshot commit when table doesn't exist."""
        with patch('pypaimon.catalog.rest.rest_catalog.RESTApi') as mock_rest_api:
            # Configure mock to raise NoSuchResourceException
            mock_api_instance = Mock()
            mock_api_instance.options = self.test_options
            mock_api_instance.commit_snapshot.side_effect = NoSuchResourceException("Table", None, "not found")
            mock_rest_api.return_value = mock_api_instance

            # Create RESTCatalog
            catalog = RESTCatalog(self.catalog_context)

            # Test commit snapshot with table not exist
            with self.assertRaises(TableNotExistException):
                catalog.commit_snapshot(
                    self.identifier,
                    "test-uuid",
                    self.test_snapshot,
                    self.test_statistics
                )

    def test_rest_catalog_commit_snapshot_api_error(self):
        """Test snapshot commit with API error."""
        with patch('pypaimon.catalog.rest.rest_catalog.RESTApi') as mock_rest_api:
            # Configure mock to raise generic exception
            mock_api_instance = Mock()
            mock_api_instance.options = self.test_options
            mock_api_instance.commit_snapshot.side_effect = RuntimeError("API Error")
            mock_rest_api.return_value = mock_api_instance

            # Create RESTCatalog
            catalog = RESTCatalog(self.catalog_context)

            # Test commit snapshot with API error
            with self.assertRaises(RuntimeError) as context:
                catalog.commit_snapshot(
                    self.identifier,
                    "test-uuid",
                    self.test_snapshot,
                    self.test_statistics
                )

            # Verify error message contains table name
            self.assertIn("test_db.test_table", str(context.exception))

    def test_commit_table_request_creation(self):
        """Test CommitTableRequest creation."""
        from pypaimon.api.api_request import CommitTableRequest

        request = CommitTableRequest(
            table_uuid="test-uuid",
            snapshot=self.test_snapshot,
            statistics=self.test_statistics
        )

        # Verify request fields
        self.assertEqual(request.table_uuid, "test-uuid")
        self.assertEqual(request.snapshot, self.test_snapshot)
        self.assertEqual(request.statistics, self.test_statistics)

    def test_commit_table_response_creation(self):
        """Test CommitTableResponse creation."""
        from pypaimon.api.api_response import CommitTableResponse

        # Test successful response
        success_response = CommitTableResponse(success=True)
        self.assertTrue(success_response.is_success())

        # Test failed response
        failed_response = CommitTableResponse(success=False)
        self.assertFalse(failed_response.is_success())

    def test_rest_api_commit_snapshot(self):
        """Test RESTApi commit_snapshot method."""
        from pypaimon.api.rest_api import RESTApi

        with patch('pypaimon.api.client.HttpClient') as mock_client_class:
            # Configure mock
            mock_client = Mock()
            mock_response = CommitTableResponse(success=True)
            mock_client.post_with_response_type.return_value = mock_response
            mock_client_class.return_value = mock_client

            # Create RESTApi with mocked client
            with patch('pypaimon.api.auth.AuthProviderFactory.create_auth_provider'):
                api = RESTApi(self.test_options, config_required=False)
                api.client = mock_client

                # Test commit snapshot
                result = api.commit_snapshot(
                    self.identifier,
                    "test-uuid",
                    self.test_snapshot,
                    self.test_statistics
                )

                # Verify result
                self.assertTrue(result)

                # Verify client was called correctly
                mock_client.post_with_response_type.assert_called_once()


if __name__ == '__main__':
    unittest.main()
