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

from typing import Optional

from pypaimon.api.auth.base import AuthProvider
from pypaimon.api.auth.bearer import BearTokenAuthProvider
from pypaimon.api.auth.dlf_provider import DLFAuthProvider
from pypaimon.api.token_loader import DLFToken, DLFTokenLoaderFactory
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions


class DLFAuthProviderFactory:

    OPENAPI_IDENTIFIER = "openapi"
    DEFAULT_IDENTIFIER = "default"

    REGION_PATTERN = r'(?:pre-)?([a-z]+-[a-z]+(?:-\d+)?)'

    @staticmethod
    def parse_region_from_uri(uri: Optional[str]) -> Optional[str]:
        import re

        if not uri:
            return None

        try:
            pattern = re.compile(DLFAuthProviderFactory.REGION_PATTERN)
            match = pattern.search(uri)
            if match:
                return match.group(1)
        except Exception:
            pass

        return None

    @staticmethod
    def parse_signing_algo_from_uri(uri: Optional[str]) -> str:
        if not uri:
            return DLFAuthProviderFactory.DEFAULT_IDENTIFIER

        host = uri.lower()
        if host.startswith("http://"):
            host = host[7:]
        elif host.startswith("https://"):
            host = host[8:]

        host = host.split('/')[0].split(':')[0]

        if host.startswith("dlfnext") or "openapi" in host:
            return DLFAuthProviderFactory.OPENAPI_IDENTIFIER

        return DLFAuthProviderFactory.DEFAULT_IDENTIFIER


class AuthProviderFactory:

    @staticmethod
    def create_auth_provider(options: Options) -> AuthProvider:
        provider = options.get(CatalogOptions.TOKEN_PROVIDER)
        if provider == 'bear':
            token = options.get(CatalogOptions.TOKEN)
            return BearTokenAuthProvider(token)
        elif provider == 'dlf':
            uri = options.get(CatalogOptions.URI)

            region = options.get(CatalogOptions.DLF_REGION)
            if not region:
                region = DLFAuthProviderFactory.parse_region_from_uri(uri)
                if not region:
                    raise ValueError(
                        "Could not get region from config or URI. "
                        "Please set 'dlf.region' or use a standard DLF endpoint URI."
                    )

            # Get signing algorithm from options, or auto-detect from URI
            signing_algorithm = options.get(CatalogOptions.DLF_SIGNING_ALGORITHM)
            if not signing_algorithm or signing_algorithm == "default":
                # Auto-detect based on URI
                signing_algorithm = DLFAuthProviderFactory.parse_signing_algo_from_uri(uri)

            return DLFAuthProvider(
                uri=uri,
                region=region,
                signing_algorithm=signing_algorithm,
                token=DLFToken.from_options(options),
                token_loader=DLFTokenLoaderFactory.create_token_loader(options)
            )
        raise ValueError('Unknown auth provider')
