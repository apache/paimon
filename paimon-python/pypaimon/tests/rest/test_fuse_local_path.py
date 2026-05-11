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

from pypaimon.catalog.rest.fuse_support import FusePathResolver
from pypaimon.catalog.rest.rest_catalog import RESTCatalog
from pypaimon.common.options import Options
from pypaimon.common.options.config import FuseOptions


class TestFuseLocalPath(unittest.TestCase):
    """Test cases for FUSE local path functionality."""

    def _create_resolver(
        self,
        root="/mnt/fuse/warehouse",
        validation_mode="strict",
        mode="pvfs"
    ):
        """Helper to create a FusePathResolver with given config."""
        options = Options({
            "uri": "http://localhost:8080",
            "warehouse": "oss://catalog/warehouse",
            FuseOptions.FUSE_ENABLED.key(): "true",
            FuseOptions.FUSE_ROOT.key(): root,
            FuseOptions.FUSE_VALIDATION_MODE.key(): validation_mode,
            FuseOptions.FUSE_MODE.key(): mode,
        })
        rest_api = MagicMock()
        return FusePathResolver(options, rest_api), options, rest_api

    def _create_catalog_with_fuse(
        self,
        enabled=True,
        root="/mnt/fuse/warehouse",
        validation_mode="strict",
        mode="pvfs"
    ):
        """Helper to create a mock RESTCatalog with FUSE configuration."""
        options = Options({
            "uri": "http://localhost:8080",
            "warehouse": "oss://catalog/warehouse",
            FuseOptions.FUSE_ENABLED.key(): str(enabled).lower(),
            FuseOptions.FUSE_ROOT.key(): root,
            FuseOptions.FUSE_VALIDATION_MODE.key(): validation_mode,
            FuseOptions.FUSE_MODE.key(): mode,
        })

        catalog = MagicMock(spec=RESTCatalog)
        catalog.fuse_enabled = enabled
        catalog.data_token_enabled = False
        catalog.rest_api = MagicMock()
        catalog.context = MagicMock()
        catalog.context.options = options

        if enabled:
            resolver = FusePathResolver(options, catalog.rest_api)
            catalog._fuse_resolver = resolver
        else:
            catalog._fuse_resolver = None

        catalog.file_io_for_data = RESTCatalog.file_io_for_data.__get__(catalog)
        catalog.file_io_from_options = MagicMock(return_value=MagicMock())

        return catalog

    # ========== _resolve_fuse_local_path Tests ==========

    # --- pvfs mode tests ---

    def test_resolve_pvfs_mode_with_identifier(self):
        """Test pvfs mode uses identifier logical names."""
        from pypaimon.common.identifier import Identifier
        resolver, _, _ = self._create_resolver(mode="pvfs")
        identifier = Identifier.create("my_db", "my_table")

        result = resolver.resolve_local_path(
            "oss://clg-paimon-xxx/db-xxx/tbl-xxx", identifier
        )
        self.assertEqual(result, "/mnt/fuse/warehouse/my_db/my_table")

    def test_resolve_pvfs_mode_with_trailing_slash(self):
        """Test pvfs mode with trailing slash on root."""
        from pypaimon.common.identifier import Identifier
        resolver, _, _ = self._create_resolver(mode="pvfs", root="/mnt/fuse/warehouse/")
        identifier = Identifier.create("my_db", "my_table")

        result = resolver.resolve_local_path(
            "oss://clg-paimon-xxx/db-xxx/tbl-xxx", identifier
        )
        self.assertEqual(result, "/mnt/fuse/warehouse/my_db/my_table")

    def test_resolve_pvfs_mode_without_identifier_raises(self):
        """Test pvfs mode raises ValueError when identifier is None."""
        resolver, _, _ = self._create_resolver(mode="pvfs")

        with self.assertRaises(ValueError) as context:
            resolver.resolve_local_path("oss://clg-paimon-xxx/db-xxx/tbl-xxx")

        self.assertIn("identifier is None", str(context.exception))

    # --- raw mode tests ---

    def test_resolve_raw_mode_basic(self):
        """Test raw mode basic path conversion."""
        resolver, _, _ = self._create_resolver(mode="raw")

        result = resolver.resolve_local_path("oss://catalog/db1/table1")
        self.assertEqual(result, "/mnt/fuse/warehouse/db1/table1")

    def test_resolve_raw_mode_with_trailing_slash(self):
        """Test raw mode with trailing slash on root."""
        resolver, _, _ = self._create_resolver(mode="raw", root="/mnt/fuse/warehouse/")

        result = resolver.resolve_local_path("oss://catalog/db1/table1")
        self.assertEqual(result, "/mnt/fuse/warehouse/db1/table1")

    def test_resolve_raw_mode_deep_path(self):
        """Test raw mode with deep path."""
        resolver, _, _ = self._create_resolver(mode="raw")

        result = resolver.resolve_local_path(
            "oss://catalog/db1/table1/partition1/file.parquet"
        )
        self.assertEqual(
            result,
            "/mnt/fuse/warehouse/db1/table1/partition1/file.parquet"
        )

    def test_resolve_raw_mode_without_scheme(self):
        """Test raw mode path without scheme skips first segment."""
        resolver, _, _ = self._create_resolver(mode="raw")

        result = resolver.resolve_local_path("catalog/db1/table1")
        self.assertEqual(result, "/mnt/fuse/warehouse/db1/table1")

    def test_resolve_raw_mode_ignores_identifier(self):
        """Test raw mode uses URI path even when identifier is provided."""
        from pypaimon.common.identifier import Identifier
        resolver, _, _ = self._create_resolver(mode="raw")
        identifier = Identifier.create("my_db", "my_table")

        result = resolver.resolve_local_path(
            "oss://catalog/db-uuid/tbl-uuid", identifier
        )
        self.assertEqual(result, "/mnt/fuse/warehouse/db-uuid/tbl-uuid")

    # --- common tests ---

    def test_resolve_fuse_local_path_missing_root(self):
        """Test error when root is not configured."""
        resolver, _, _ = self._create_resolver(root=None)

        with self.assertRaises(ValueError) as context:
            resolver.resolve_local_path("oss://catalog/db1/table1")

        self.assertIn("fuse.root is not configured", str(context.exception))

    # ========== Validation Tests ==========

    def test_validation_mode_none_skips_validation(self):
        """Test none mode skips validation."""
        resolver, _, _ = self._create_resolver(validation_mode="none")

        resolver.validate()

        self.assertTrue(resolver.validation_state)

    def test_validation_mode_strict_raises_on_failure(self):
        """Test strict mode raises exception on validation failure."""
        resolver, _, rest_api = self._create_resolver(validation_mode="strict")

        # Mock default database with location
        mock_db = MagicMock()
        mock_db.location = "oss://catalog/default"
        rest_api.get_database.return_value = mock_db

        # Mock LocalFileIO to return False for exists
        with patch('pypaimon.catalog.rest.fuse_support.LocalFileIO') as mock_local_io:
            mock_instance = MagicMock()
            mock_instance.exists.return_value = False
            mock_local_io.return_value = mock_instance

            with self.assertRaises(ValueError) as context:
                resolver.validate()

            self.assertIn("FUSE local path validation failed", str(context.exception))

    def test_validation_mode_warn_fallback_on_failure(self):
        """Test warn mode falls back to default FileIO on validation failure."""
        resolver, _, rest_api = self._create_resolver(validation_mode="warn")

        # Mock default database with location
        mock_db = MagicMock()
        mock_db.location = "oss://catalog/default"
        rest_api.get_database.return_value = mock_db

        # Mock LocalFileIO to return False for exists
        with patch('pypaimon.catalog.rest.fuse_support.LocalFileIO') as mock_local_io:
            mock_instance = MagicMock()
            mock_instance.exists.return_value = False
            mock_local_io.return_value = mock_instance

            # Should not raise, just set state to False
            resolver.validate()

            self.assertFalse(resolver.validation_state)

    def test_validation_passes_when_local_exists(self):
        """Test validation passes when local path exists."""
        resolver, _, rest_api = self._create_resolver(validation_mode="strict")

        # Mock default database with location
        mock_db = MagicMock()
        mock_db.location = "oss://catalog/default"
        rest_api.get_database.return_value = mock_db

        # Mock LocalFileIO to return True for exists
        with patch('pypaimon.catalog.rest.fuse_support.LocalFileIO') as mock_local_io:
            mock_instance = MagicMock()
            mock_instance.exists.return_value = True
            mock_local_io.return_value = mock_instance

            resolver.validate()

            self.assertTrue(resolver.validation_state)

    def test_validation_skips_when_no_location(self):
        """Test validation skips when default database has no location."""
        resolver, _, rest_api = self._create_resolver(validation_mode="strict")

        # Mock default database without location
        mock_db = MagicMock()
        mock_db.location = None
        rest_api.get_database.return_value = mock_db

        resolver.validate()

        self.assertTrue(resolver.validation_state)

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
        """Test that validated FUSE uses FuseLocalFileIO."""
        catalog = self._create_catalog_with_fuse(enabled=True, validation_mode="none")
        catalog._fuse_resolver.validation_state = True  # Already validated

        from pypaimon.common.identifier import Identifier
        identifier = Identifier.create("db1", "table1")

        with patch('pypaimon.catalog.rest.fuse_support.FuseLocalFileIO') as mock_fuse_io:
            mock_fuse_io.return_value = MagicMock()
            _ = catalog.file_io_for_data("oss://catalog/db1/table1", identifier)
            mock_fuse_io.assert_called_once()

    def test_file_io_for_data_fallback_when_validation_failed(self):
        """Test that failed validation falls back to default FileIO."""
        catalog = self._create_catalog_with_fuse(enabled=True, validation_mode="warn")
        catalog._fuse_resolver.validation_state = False  # Validation failed
        catalog.data_token_enabled = False

        from pypaimon.common.identifier import Identifier
        identifier = Identifier.create("db1", "table1")

        _ = catalog.file_io_for_data("oss://catalog/db1/table1", identifier)
        catalog.file_io_from_options.assert_called_once()

    # ========== Invalid Mode Tests ==========

    def test_resolve_invalid_mode_raises(self):
        """Test that an invalid mode raises ValueError."""
        resolver, _, _ = self._create_resolver(mode="invalid")

        with self.assertRaises(ValueError) as context:
            resolver.resolve_local_path("oss://catalog/db1/table1")

        self.assertIn("Invalid fuse.mode", str(context.exception))
        self.assertIn("invalid", str(context.exception))

    # ========== Raw Mode Validation Tests ==========

    def test_validation_raw_mode_strict_raises_on_failure(self):
        """Test strict validation in raw mode raises exception on failure."""
        resolver, _, rest_api = self._create_resolver(validation_mode="strict", mode="raw")

        mock_db = MagicMock()
        mock_db.location = "oss://catalog/default"
        rest_api.get_database.return_value = mock_db

        with patch('pypaimon.catalog.rest.fuse_support.LocalFileIO') as mock_local_io:
            mock_instance = MagicMock()
            mock_instance.exists.return_value = False
            mock_local_io.return_value = mock_instance

            with self.assertRaises(ValueError) as context:
                resolver.validate()

            self.assertIn("FUSE local path validation failed", str(context.exception))

    def test_validation_raw_mode_passes_when_local_exists(self):
        """Test validation passes in raw mode when local path exists."""
        resolver, _, rest_api = self._create_resolver(validation_mode="strict", mode="raw")

        mock_db = MagicMock()
        mock_db.location = "oss://catalog/default"
        rest_api.get_database.return_value = mock_db

        with patch('pypaimon.catalog.rest.fuse_support.LocalFileIO') as mock_local_io:
            mock_instance = MagicMock()
            mock_instance.exists.return_value = True
            mock_local_io.return_value = mock_instance

            resolver.validate()

            self.assertTrue(resolver.validation_state)


if __name__ == '__main__':
    unittest.main()
