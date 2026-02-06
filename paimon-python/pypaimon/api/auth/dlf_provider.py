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

import logging
import re
import time
from datetime import datetime, timezone
from typing import Dict

from pypaimon.api.auth.base import AuthProvider
from pypaimon.api.auth.dlf_signer import (
    DLFDefaultSigner,
    DLFOpenApiSigner,
    DLFRequestSigner,
)
from pypaimon.api.token_loader import DLFToken, DLFTokenLoader
from pypaimon.api.typedef import RESTAuthParameter


class DLFAuthProvider(AuthProvider):
    DLF_AUTHORIZATION_HEADER_KEY = "Authorization"
    TOKEN_EXPIRATION_SAFE_TIME_MILLIS = 3_600_000

    def __init__(self,
                 uri: str,
                 region: str,
                 signing_algorithm: str,
                 token: DLFToken = None,
                 token_loader: DLFTokenLoader = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        if token is None and token_loader is None:
            raise ValueError("Either token or token_loader must be provided")
        self.token = token
        self.token_loader = token_loader
        self.uri = uri
        self.region = region
        self.signing_algorithm = signing_algorithm
        self.signer = self._create_signer(signing_algorithm)

    def _create_signer(self, signing_algorithm: str) -> DLFRequestSigner:
        if signing_algorithm == DLFOpenApiSigner.IDENTIFIER:
            return DLFOpenApiSigner()
        else:
            return DLFDefaultSigner(self.region)

    @staticmethod
    def extract_host(uri: str) -> str:
        # Remove protocol (http:// or https://)
        without_protocol = re.sub(r'^https?://', '', uri)

        # Remove path (everything after '/')
        path_index = without_protocol.find('/')
        return without_protocol[:path_index] if path_index >= 0 else without_protocol

    def get_token(self) -> DLFToken:
        if self.token_loader is not None:
            if self.token is None:
                self.token = self.token_loader.load_token()
            elif self.token is not None and self.token.expiration_at_millis is not None:
                if self.token.expiration_at_millis - int(time.time() * 1000) < self.TOKEN_EXPIRATION_SAFE_TIME_MILLIS:
                    self.token = self.token_loader.load_token()
        if self.token is None:
            raise ValueError("Either token or token_loader must be provided")
        return self.token

    def merge_auth_header(
            self, base_header: Dict[str, str], rest_auth_parameter: RESTAuthParameter
    ) -> Dict[str, str]:
        try:
            token = self.get_token()
            now = datetime.now(timezone.utc)
            host = self.extract_host(self.uri)

            sign_headers = self.signer.sign_headers(
                rest_auth_parameter.data,
                now,
                token.security_token,
                host
            )

            authorization = self.signer.authorization(
                rest_auth_parameter,
                token,
                host,
                sign_headers
            )

            headers_with_auth = base_header.copy()
            headers_with_auth.update(sign_headers)
            headers_with_auth[self.DLF_AUTHORIZATION_HEADER_KEY] = authorization

            return headers_with_auth

        except Exception as e:
            raise RuntimeError(f"Failed to merge auth header: {e}")
