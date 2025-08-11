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
from pathlib import Path
from typing import Optional

from pyarrow._fs import FileSystem

from pypaimon.api import RESTApi
from pypaimon.catalog.rest.rest_token import RESTToken
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier


class RESTTokenFileIO(FileIO):

    def __init__(self, identifier: Identifier, path: Path,
                 catalog_options: Optional[dict] = None):
        self.identifier = identifier
        self.path = path
        self.token: Optional[RESTToken] = None
        self.api_instance: Optional[RESTApi] = None
        self.lock = threading.Lock()
        self.log = logging.getLogger(__name__)
        super().__init__(str(path), catalog_options)

    def _initialize_oss_fs(self) -> FileSystem:
        self.try_to_refresh_token()
        self.properties.update(self.token.token)
        return super()._initialize_oss_fs()

    def new_output_stream(self, path: Path):
        return self.filesystem.open_output_stream(str(path))

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
