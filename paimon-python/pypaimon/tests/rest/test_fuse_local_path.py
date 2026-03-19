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
#  limitations under the License.
################################################################################
import unittest
from unittest.mock import MagicMock, patch

from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.options import Options
from pypaimon.common.options.config import FuseOptions


class TestFuseLocalPath(unittest.TestCase):
    """Test cases for FUSE local path functionality."""

    def _create_catalog_with_fuse(
        self,
        enabled: bool = True,
        root: str = "/mnt/fuse/warehouse",
        validation_mode: str = "strict"
    ) -> RESTCatalog:
        """Helper to create a mock RESTCatalog with FUSE configuration."""
        options = Options({
            "uri": "http://localhost:8080",
            "warehouse": "oss://catalog/warehouse",
            FuseOptions.FUSE_LOCAL_PATH_ENABLED.key(): str(enabled).lower(),
            FuseOptions.FUSE_LOCAL_PATH_ROOT.key(): root,
            FuseOptions.FUSE_LOCAL_PATH_VALIDATION_MODE.key(): validation_mode,
        })

        # Create a mock catalog directly without going through __init__
        catalog = MagicMock(spec=RESTCatalog)
        catalog.fuse_local_path_enabled = enabled
        catalog.fuse_local_path_root = root
        catalog.fuse_validation_mode = validation_mode
        catalog._fuse_validation_state = None
        catalog.data_token_enabled = False
        catalog.rest_api = MagicMock()
        catalog.context = MagicMock()
        catalog.context.options = options

        # Bind actual methods to the mock
        catalog._resolve_fuse_local_path = RESTCatalog._resolve_fuse_local_path.__get__(catalog)
        catalog._validate_fuse_path = RESTCatalog._validate_fuse_path.__get__(catalog)
        catalog._handle_validation_error = RESTCatalog._handle_validation_error.__get__(catalog)
        catalog.file_io_for_data = RESTCatalog.file_io_for_data.__get__(catalog)
        catalog.file_io_from_options = MagicMock(return_value=MagicMock())

        return catalog

    # ========== _resolve_fuse_local_path Tests ==========

    def test_resolve_fuse_local_path_basic(self):
        """Test basic path conversion."""
        catalog = self._create_catalog_with_fuse()

        result = catalog._resolve_fuse_local_path("oss://catalog/db1/table1")
        self.assertEqual(result, "/mnt/fuse/warehouse/db1/table1")

    def test_resolve_fuse_local_path_with_trailing_slash(self):
        """Test fuse_root with trailing slash."""
        catalog = self._create_catalog_with_fuse(root="/mnt/fuse/warehouse/")

        result = catalog._resolve_fuse_local_path("oss://catalog/db1/table1")
        self.assertEqual(result, "/mnt/fuse/warehouse/db1/table1")

    def test_resolve_fuse_local_path_deep_path(self):
        """Test deep path with multiple levels."""
        catalog = self._create_catalog_with_fuse()

        result = catalog._resolve_fuse_local_path(
            "oss://catalog/db1/table1/partition1/file.parquet"
        )
        self.assertEqual(
            result,
            "/mnt/fuse/warehouse/db1/table1/partition1/file.parquet"
        )

    def test_resolve_fuse_local_path_without_scheme(self):
        """Test path without scheme."""
        catalog = self._create_catalog_with_fuse()

        result = catalog._resolve_fuse_local_path("catalog/db1/table1")
        self.assertEqual(result, "/mnt/fuse/warehouse/db1/table1")

    def test_resolve_fuse_local_path_missing_root(self):
        """Test error when root is not configured."""
        catalog = self._create_catalog_with_fuse(root=None)

        with self.assertRaises(ValueError) as context:
            catalog._resolve_fuse_local_path("oss://catalog/db1/table1")

        self.assertIn("fuse.local-path.root is not configured", str(context.exception))

    # ========== Validation Tests ==========

    def test_validation_mode_none_skips_validation(self):
        """Test none mode skips validation."""
        catalog = self._create_catalog_with_fuse(validation_mode="none")

        catalog._validate_fuse_path()

        self.assertTrue(catalog._fuse_validation_state)

    def test_validation_mode_strict_raises_on_failure(self):
        """Test strict mode raises exception on validation failure."""
        catalog = self._create_catalog_with_fuse(validation_mode="strict")

        # Mock default database with location
        mock_db = MagicMock()
        mock_db.location = "oss://catalog/default"
        catalog.rest_api.get_database.return_value = mock_db

        # Mock LocalFileIO to return False for exists
        with patch('pypaimon.catalog.rest.rest_catalog.LocalFileIO') as mock_local_io:
            mock_instance = MagicMock()
            mock_instance.exists.return_value = False
            mock_local_io.return_value = mock_instance

            with self.assertRaises(ValueError) as context:
                catalog._validate_fuse_path()

            self.assertIn("FUSE local path validation failed", str(context.exception))

    def test_validation_mode_warn_fallback_on_failure(self):
        """Test warn mode falls back to default FileIO on validation failure."""
        catalog = self._create_catalog_with_fuse(validation_mode="warn")

        # Mock default database with location
        mock_db = MagicMock()
        mock_db.location = "oss://catalog/default"
        catalog.rest_api.get_database.return_value = mock_db

        # Mock LocalFileIO to return False for exists
        with patch('pypaimon.catalog.rest.rest_catalog.LocalFileIO') as mock_local_io:
            mock_instance = MagicMock()
            mock_instance.exists.return_value = False
            mock_local_io.return_value = mock_instance

            # Should not raise, just set state to False
            catalog._validate_fuse_path()

            self.assertFalse(catalog._fuse_validation_state)

    def test_validation_passes_when_local_exists(self):
        """Test validation passes when local path exists."""
        catalog = self._create_catalog_with_fuse(validation_mode="strict")

        # Mock default database with location
        mock_db = MagicMock()
        mock_db.location = "oss://catalog/default"
        catalog.rest_api.get_database.return_value = mock_db

        # Mock LocalFileIO to return True for exists
        with patch('pypaimon.catalog.rest.rest_catalog.LocalFileIO') as mock_local_io:
            mock_instance = MagicMock()
            mock_instance.exists.return_value = True
            mock_local_io.return_value = mock_instance

            catalog._validate_fuse_path()

            self.assertTrue(catalog._fuse_validation_state)

    def test_validation_skips_when_no_location(self):
        """Test validation skips when default database has no location."""
        catalog = self._create_catalog_with_fuse(validation_mode="strict")

        # Mock default database without location
        mock_db = MagicMock()
        mock_db.location = None
        catalog.rest_api.get_database.return_value = mock_db

        catalog._validate_fuse_path()

        self.assertTrue(catalog._fuse_validation_state)

    # ========== file_io_for_data Tests ==========

    def test_file_io_for_data_disabled_fuse(self):
        """Test that disabled FUSE uses default FileIO."""
        catalog = self._create_catalog_with_fuse(enabled=False)
        catalog.data_token_enabled = False

        from pypaimon.common.identifier import Identifier
        identifier = Identifier.create("db1", "table1")

        _ = catalog.file_io_for_data("oss://catalog/db1/table1", identifier)
        catalog.file_io_from_options.assert_called_once()

    def test_file_io_for_data_uses_local_when_validated(self):
        """Test that validated FUSE uses LocalFileIO."""
        catalog = self._create_catalog_with_fuse(enabled=True, validation_mode="none")
        catalog._fuse_validation_state = True  # Already validated

        from pypaimon.common.identifier import Identifier
        identifier = Identifier.create("db1", "table1")

        with patch('pypaimon.catalog.rest.rest_catalog.LocalFileIO') as mock_local_io:
            mock_local_io.return_value = MagicMock()
            _ = catalog.file_io_for_data("oss://catalog/db1/table1", identifier)
            mock_local_io.assert_called_once()

    def test_file_io_for_data_fallback_when_validation_failed(self):
        """Test that failed validation falls back to default FileIO."""
        catalog = self._create_catalog_with_fuse(enabled=True, validation_mode="warn")
        catalog._fuse_validation_state = False  # Validation failed
        catalog.data_token_enabled = False

        from pypaimon.common.identifier import Identifier
        identifier = Identifier.create("db1", "table1")

        _ = catalog.file_io_for_data("oss://catalog/db1/table1", identifier)
        catalog.file_io_from_options.assert_called_once()


if __name__ == '__main__':
    unittest.main()
