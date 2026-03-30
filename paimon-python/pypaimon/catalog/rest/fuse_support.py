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
import logging
from urllib.parse import urlparse

from pypaimon.common.identifier import Identifier
from pypaimon.common.options.config import FuseOptions
from pypaimon.filesystem.local_file_io import LocalFileIO, FuseLocalFileIO

logger = logging.getLogger(__name__)


class FusePathResolver:
    """Resolves FUSE local paths and validates FUSE mount availability."""

    def __init__(self, options, rest_api):
        self.fuse_root = options.get(FuseOptions.FUSE_ROOT)
        self.validation_mode = options.get(FuseOptions.FUSE_VALIDATION_MODE, "strict")
        self.fuse_mode = options.get(FuseOptions.FUSE_MODE, "pvfs")
        self._validation_state = None  # None=not validated, True=passed, False=failed
        self._rest_api = rest_api
        self._options = options

    def resolve_local_path(self, original_path, identifier=None):
        """
        Resolve FUSE local path.

        In 'pvfs' mode, use database/table logical names from identifier to build the path.
        If identifier has no object name, returns database-level path (used for validation).
        In 'raw' mode, use URI path segments directly.

        Returns:
            Local path

        Raises:
            ValueError: If fuse.root is not configured or pvfs mode missing identifier
        """
        if not self.fuse_root:
            raise ValueError(
                "FUSE local path is enabled but fuse.root is not configured"
            )

        root = self.fuse_root.rstrip('/')

        if self.fuse_mode == "pvfs":
            if identifier is None:
                raise ValueError(
                    "FUSE path mode 'pvfs' requires an Identifier to resolve "
                    "the local path, but identifier is None."
                )
            db = identifier.get_database_name()
            obj = identifier.get_object_name()
            if obj:
                return "{}/{}/{}".format(root, db, obj)
            return "{}/{}".format(root, db)
        elif self.fuse_mode == "raw":
            # raw mode: use URI path segments directly
            uri = urlparse(original_path)
            path_part = uri.path.lstrip('/')
            if not uri.scheme:
                # No scheme means path like "catalog/db/table",
                # skip the first segment (catalog name) to align with scheme-based paths
                segments = path_part.split('/')
                if len(segments) > 1:
                    path_part = '/'.join(segments[1:])
            return "{}/{}".format(root, path_part)
        else:
            raise ValueError(
                "Invalid fuse.mode: '{}'. "
                "Supported modes are 'pvfs' and 'raw'.".format(self.fuse_mode)
            )

    def validate(self):
        """
        Validate FUSE local path is correctly mounted.

        Get default database's location, convert to local path and check if it exists.
        """
        if self.validation_mode == "none":
            self._validation_state = True
            return

        # Get default database details, API call failure raises exception directly
        db = self._rest_api.get_database("default")
        remote_location = db.location

        if not remote_location:
            logger.info("Default database has no location, skipping FUSE validation")
            self._validation_state = True
            return

        expected_local = self.resolve_local_path(
            remote_location, Identifier.create("default", None)
        )
        local_file_io = LocalFileIO(expected_local, self._options)

        # Only validate if local path exists, handle based on validation mode
        if not local_file_io.exists(expected_local):
            error_msg = (
                "FUSE local path validation failed: "
                "local path '{}' does not exist "
                "for default database location '{}'".format(expected_local, remote_location)
            )
            self._handle_validation_error(error_msg)
        else:
            self._validation_state = True
            logger.info("FUSE local path validation passed")

    def _handle_validation_error(self, error_msg):
        """Handle validation error based on validation mode."""
        if self.validation_mode == "strict":
            raise ValueError(error_msg)
        elif self.validation_mode == "warn":
            logger.warning("%s. Falling back to default FileIO.", error_msg)
            self._validation_state = False  # Mark validation failed, fallback to default FileIO

    def get_file_io(self, table_path, identifier, data_token_enabled,
                    rest_token_file_io_factory, default_file_io_factory):
        """
        Get FileIO for data access, supporting FUSE local path mapping.

        Args:
            table_path: The remote table path
            identifier: Table identifier
            data_token_enabled: Whether data token is enabled
            rest_token_file_io_factory: Factory callable for RESTTokenFileIO
            default_file_io_factory: Factory callable for default FileIO

        Returns:
            FileIO instance (FuseLocalFileIO or fallback)
        """
        # Configuration error raises exception directly
        local_path = self.resolve_local_path(table_path, identifier)

        # Perform validation (only once)
        if self._validation_state is None:
            self.validate()

        # Validation passed, return FUSE-aware local FileIO
        if self._validation_state:
            return FuseLocalFileIO(
                path=table_path.rstrip('/'),
                fuse_path=local_path.rstrip('/'),
                catalog_options=self._options,
            )

        # warn mode validation failed, fallback to default FileIO
        if data_token_enabled:
            return rest_token_file_io_factory()
        return default_file_io_factory()

    @property
    def validation_state(self):
        return self._validation_state

    @validation_state.setter
    def validation_state(self, value):
        self._validation_state = value
