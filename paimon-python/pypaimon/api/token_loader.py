#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException

from pypaimon.api.client import ExponentialRetry
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.common.json_util import JSON, json_field


@dataclass
class DLFToken:
    TOKEN_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    access_key_id: str = json_field('AccessKeyId')
    access_key_secret: str = json_field('AccessKeySecret')
    security_token: Optional[str] = json_field('SecurityToken')
    expiration: Optional[str] = json_field('Expiration')
    expiration_at_millis: Optional[int] = json_field('ExpirationAt', default=None)

    @staticmethod
    def parse_expiration_to_millis(expiration: str) -> int:
        date_time = datetime.strptime(expiration, DLFToken.TOKEN_DATE_FORMAT)
        utc_datetime = date_time.replace(tzinfo=timezone.utc)
        expiration_at_millis = int(utc_datetime.timestamp() * 1000)
        return expiration_at_millis

    def __init__(self, access_key_id: str, access_key_secret: str,
                 security_token: str, expiration: str = None, expiration_at_millis: int = None):
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.security_token = security_token
        self.expiration = expiration
        if expiration_at_millis is not None:
            self.expiration_at_millis = expiration_at_millis
        if expiration is not None:
            self.expiration_at_millis = self.parse_expiration_to_millis(expiration)

    @classmethod
    def from_options(cls, options: Options) -> Optional['DLFToken']:
        from pypaimon.common.options.config import CatalogOptions
        if (options.get(CatalogOptions.DLF_ACCESS_KEY_ID) is None
                or options.get(CatalogOptions.DLF_ACCESS_KEY_SECRET) is None):
            return None
        else:
            return cls(
                access_key_id=options.get(CatalogOptions.DLF_ACCESS_KEY_ID),
                access_key_secret=options.get(CatalogOptions.DLF_ACCESS_KEY_SECRET),
                security_token=options.get(CatalogOptions.DLF_ACCESS_SECURITY_TOKEN)
            )


class DLFTokenLoader(ABC):

    @abstractmethod
    def load_token(self) -> DLFToken:
        pass

    @abstractmethod
    def description(self) -> str:
        pass


class HTTPClient:
    """HTTP client with retry and timeout configuration"""

    def __init__(self, connect_timeout: int = 180, read_timeout: int = 180,
                 max_retries: int = 3):
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.session = requests.Session()

        # Add retry adapter
        retry_interceptor = ExponentialRetry(max_retries=3)
        adapter = HTTPAdapter(max_retries=retry_interceptor.adapter)

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set timeouts
        self.session.timeout = (connect_timeout, read_timeout)

    def get(self, url: str, **kwargs) -> requests.Response:
        """Make GET request with configured timeouts and retries"""
        return self.session.get(url, timeout=(self.connect_timeout, self.read_timeout), **kwargs)

    def close(self):
        """Close the session"""
        self.session.close()


class DLFECSTokenLoader(DLFTokenLoader):
    """
    DLF ECS Token Loader implementation

    This class loads DLF tokens from ECS metadata service.
    """

    # Class-level HTTP client (equivalent to static in Java)
    _http_client: Optional[HTTPClient] = None

    @classmethod
    def get_http_client(cls) -> HTTPClient:
        """Get or create the shared HTTP client"""
        if cls._http_client is None:
            cls._http_client = HTTPClient(
                connect_timeout=180,  # 3 minutes
                read_timeout=180,  # 3 minutes
                max_retries=3
            )
        return cls._http_client

    def __init__(self, ecs_metadata_url: str, role_name: Optional[str] = None):
        """
        Initialize DLF ECS Token Loader

        Args:
            ecs_metadata_url: ECS metadata service URL
            role_name: Optional role name. If None, will be fetched from metadata service
        """
        self.ecs_metadata_url = ecs_metadata_url
        self.role_name = role_name

    def load_token(self) -> DLFToken:
        try:
            if self.role_name is None:
                self.role_name = self._get_role(self.ecs_metadata_url)

            token_url = urljoin(self.ecs_metadata_url.rstrip('/') + '/', self.role_name)
            return self._get_token(token_url)

        except Exception as e:
            raise RuntimeError("Token loading failed: {}".format(e)) from e

    def description(self) -> str:
        return self.ecs_metadata_url

    def _get_role(self, url: str) -> str:
        try:
            return self._get_response_body(url)
        except Exception as e:
            raise RuntimeError("Get role failed, error: {}".format(e)) from e

    def _get_token(self, url: str) -> DLFToken:
        try:
            token_json = self._get_response_body(url)
            return JSON.from_json(token_json, DLFToken)
        except OSError as e:
            # Python equivalent of UncheckedIOException
            raise OSError("IO error while getting token: {}".format(e)) from e
        except Exception as e:
            raise RuntimeError("Get token failed, error: {}".format(e)) from e

    def _get_response_body(self, url: str) -> str:
        try:
            http_client = self.get_http_client()
            response = http_client.get(url)

            if response is None:
                raise RuntimeError("Get response failed, response is None")

            if not response.ok:
                raise RuntimeError("Get response failed, response: {} {}".format(
                    response.status_code, response.reason
                ))

            response_body = response.text
            if response_body is None:
                raise RuntimeError("Get response failed, response body is None")
            return response_body

        except RuntimeError:
            # Re-raise RuntimeError as-is
            raise
        except RequestException as e:
            raise RuntimeError("Request failed: {}".format(e)) from e
        except Exception as e:
            raise RuntimeError("Get response failed, error: {}".format(e)) from e


# Factory and utility functions
class DLFTokenLoaderFactory:
    """Factory for creating DLF token loaders"""

    @staticmethod
    def create_token_loader(options: Options) -> Optional['DLFTokenLoader']:
        """Create ECS token loader"""
        loader = options.get(CatalogOptions.DLF_TOKEN_LOADER)
        if loader == 'ecs':
            ecs_metadata_url = options.get(
                CatalogOptions.DLF_TOKEN_ECS_METADATA_URL,
                'http://100.100.100.200/latest/meta-data/Ram/security-credentials/'
            )
            role_name = options.get(CatalogOptions.DLF_TOKEN_ECS_ROLE_NAME)
            return DLFECSTokenLoader(ecs_metadata_url, role_name)
        return None
