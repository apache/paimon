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
import threading
import time
from typing import Optional

from pyarrow._fs import FileSystem

from pypaimon.api.rest_api import RESTApi
from pypaimon.catalog.rest.rest_token import RESTToken
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions, OssOptions


class RESTTokenFileIO(FileIO):

    def __init__(self, identifier: Identifier, path: str,
                 catalog_options: Optional[Options] = None):
        self.identifier = identifier
        self.path = path
        self.token: Optional[RESTToken] = None
        self.api_instance: Optional[RESTApi] = None
        self.lock = threading.Lock()
        self.log = logging.getLogger(__name__)
        super().__init__(path, catalog_options)

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove non-serializable objects
        state.pop('lock', None)
        state.pop('api_instance', None)
        # token can be serialized, but we'll refresh it on deserialization
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Recreate lock after deserialization
        self.lock = threading.Lock()
        # api_instance will be recreated when needed
        self.api_instance = None

    def _initialize_oss_fs(self, path) -> FileSystem:
        self.try_to_refresh_token()
        merged_token = self._merge_token_with_catalog_options(self.token.token)
        self.properties.data.update(merged_token)
        return super()._initialize_oss_fs(path)

    def _merge_token_with_catalog_options(self, token: dict) -> dict:
        """Merge token with catalog options, DLF OSS endpoint should override the standard OSS endpoint."""
        merged_token = dict(token)
        dlf_oss_endpoint = self.properties.get(CatalogOptions.DLF_OSS_ENDPOINT)
        if dlf_oss_endpoint and dlf_oss_endpoint.strip():
            merged_token[OssOptions.OSS_ENDPOINT.key()] = dlf_oss_endpoint
        return merged_token

    def new_output_stream(self, path: str):
        # Call parent class method to ensure path conversion and parent directory creation
        return super().new_output_stream(path)

    def try_to_refresh_token(self):
        if self.should_refresh():
            with self.lock:
                if self.should_refresh():
                    self.refresh_token()

    def should_refresh(self):
        if self.token is None:
            return True
        current_time = int(time.time() * 1000)
        return (self.token.expire_at_millis - current_time) < RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS

    def refresh_token(self):
        self.log.info(f"begin refresh data token for identifier [{self.identifier}]")
        if self.api_instance is None:
            self.api_instance = RESTApi(self.properties, False)

        response = self.api_instance.load_table_token(self.identifier)
        self.log.info(
            f"end refresh data token for identifier [{self.identifier}] expiresAtMillis [{response.expires_at_millis}]"
        )
        self.token = RESTToken(response.token, response.expires_at_millis)

    def valid_token(self):
        self.try_to_refresh_token()
        return self.token
