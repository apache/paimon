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

import base64
import hashlib
import hmac
import logging
import threading
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict


@dataclass
class RESTAuthParameter:
    method: str
    path: str
    data: str
    parameters: Dict[str, str]


@dataclass
class DLFToken:
    access_key_id: str
    access_key_secret: str
    security_token: Optional[str] = None

    def __init__(self, options: Dict[str, str]):
        from . import RESTCatalogOptions
        self.access_key_id = options.get(RESTCatalogOptions.DLF_ACCESS_KEY_ID)
        self.access_key_secret = options.get(RESTCatalogOptions.DLF_ACCESS_KEY_SECRET)
        self.security_token = options.get(RESTCatalogOptions.DLF_ACCESS_SECURITY_TOKEN)


class AuthProvider(ABC):

    @abstractmethod
    def merge_auth_header(self, base_header: Dict[str, str], parammeter: RESTAuthParameter) -> Dict[str, str]:
        """Merge authorization header into header."""


class RESTAuthFunction:

    def __init__(self, init_header: Dict[str, str], auth_provider: AuthProvider):
        self.init_header = init_header.copy() if init_header else {}
        self.auth_provider = auth_provider

    def __call__(self, rest_auth_parameter: RESTAuthParameter) -> Dict[str, str]:
        return self.auth_provider.merge_auth_header(self.init_header, rest_auth_parameter)

    def apply(self, rest_auth_parameter: RESTAuthParameter) -> Dict[str, str]:
        return self.__call__(rest_auth_parameter)


class DLFAuthProvider(AuthProvider):
    DLF_AUTHORIZATION_HEADER_KEY = "Authorization"
    DLF_CONTENT_MD5_HEADER_KEY = "Content-MD5"
    DLF_CONTENT_TYPE_KEY = "Content-Type"
    DLF_DATE_HEADER_KEY = "x-dlf-date"
    DLF_SECURITY_TOKEN_HEADER_KEY = "x-dlf-security-token"
    DLF_AUTH_VERSION_HEADER_KEY = "x-dlf-version"
    DLF_CONTENT_SHA56_HEADER_KEY = "x-dlf-content-sha256"
    DLF_CONTENT_SHA56_VALUE = "UNSIGNED-PAYLOAD"

    AUTH_DATE_TIME_FORMAT = "%Y%m%dT%H%M%SZ"
    MEDIA_TYPE = "application/json"

    def __init__(self,
                 token: DLFToken,
                 region: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.token = token
        self.region = region
        self._lock = threading.Lock()

    def merge_auth_header(self,
                          base_header: Dict[str, str],
                          rest_auth_parameter: RESTAuthParameter) -> Dict[str, str]:
        try:
            date_time = base_header.get(
                self.DLF_DATE_HEADER_KEY.lower(),
                datetime.now(timezone.utc).strftime(self.AUTH_DATE_TIME_FORMAT)
            )
            date = date_time[:8]

            sign_headers = self.generate_sign_headers(
                rest_auth_parameter.data,
                date_time,
                self.token.security_token
            )

            authorization = DLFAuthSignature.get_authorization(
                rest_auth_parameter=rest_auth_parameter,
                dlf_token=self.token,
                region=self.region,
                headers=sign_headers,
                date_time=date_time,
                date=date
            )

            headers_with_auth = base_header.copy()
            headers_with_auth.update(sign_headers)
            headers_with_auth[self.DLF_AUTHORIZATION_HEADER_KEY] = authorization

            return headers_with_auth

        except Exception as e:
            raise RuntimeError(f"Failed to merge auth header: {e}")

    @classmethod
    def generate_sign_headers(cls,
                              data: Optional[str],
                              date_time: str,
                              security_token: Optional[str]) -> Dict[str, str]:
        sign_headers = {}

        sign_headers[cls.DLF_DATE_HEADER_KEY] = date_time
        sign_headers[cls.DLF_CONTENT_SHA56_HEADER_KEY] = cls.DLF_CONTENT_SHA56_VALUE
        sign_headers[cls.DLF_AUTH_VERSION_HEADER_KEY] = "v1"  # DLFAuthSignature.VERSION

        if data is not None and data != "":
            sign_headers[cls.DLF_CONTENT_TYPE_KEY] = cls.MEDIA_TYPE
            sign_headers[cls.DLF_CONTENT_MD5_HEADER_KEY] = DLFAuthSignature.md5(data)

        if security_token is not None:
            sign_headers[cls.DLF_SECURITY_TOKEN_HEADER_KEY] = security_token
        return sign_headers


class DLFAuthSignature:
    VERSION = "v1"
    SIGNATURE_ALGORITHM = "DLF4-HMAC-SHA256"
    PRODUCT = "DlfNext"
    HMAC_SHA256 = "sha256"
    REQUEST_TYPE = "aliyun_v4_request"
    SIGNATURE_KEY = "Signature"
    NEW_LINE = "\n"
    SIGNED_HEADERS = [
        DLFAuthProvider.DLF_CONTENT_MD5_HEADER_KEY.lower(),
        DLFAuthProvider.DLF_CONTENT_TYPE_KEY.lower(),
        DLFAuthProvider.DLF_CONTENT_SHA56_HEADER_KEY.lower(),
        DLFAuthProvider.DLF_DATE_HEADER_KEY.lower(),
        DLFAuthProvider.DLF_AUTH_VERSION_HEADER_KEY.lower(),
        DLFAuthProvider.DLF_SECURITY_TOKEN_HEADER_KEY.lower()
    ]

    @classmethod
    def get_authorization(cls,
                          rest_auth_parameter: RESTAuthParameter,
                          dlf_token: DLFToken,
                          region: str,
                          headers: Dict[str, str],
                          date_time: str,
                          date: str) -> str:
        try:
            canonical_request = cls.get_canonical_request(rest_auth_parameter, headers)

            string_to_sign = cls.NEW_LINE.join([
                cls.SIGNATURE_ALGORITHM,
                date_time,
                f"{date}/{region}/{cls.PRODUCT}/{cls.REQUEST_TYPE}",
                cls._sha256_hex(canonical_request)
            ])

            date_key = cls._hmac_sha256(f"aliyun_v4{dlf_token.access_key_secret}".encode('utf-8'), date)
            date_region_key = cls._hmac_sha256(date_key, region)
            date_region_service_key = cls._hmac_sha256(date_region_key, cls.PRODUCT)
            signing_key = cls._hmac_sha256(date_region_service_key, cls.REQUEST_TYPE)

            result = cls._hmac_sha256(signing_key, string_to_sign)
            signature = cls._hex_encode(result)

            credential = (f"{cls.SIGNATURE_ALGORITHM} "
                          f"Credential={dlf_token.access_key_id}/{date}/{region}/{cls.PRODUCT}/{cls.REQUEST_TYPE}")
            signature_part = f"{cls.SIGNATURE_KEY}={signature}"

            return f"{credential},{signature_part}"

        except Exception as e:
            raise RuntimeError(f"Failed to generate authorization: {e}")

    @classmethod
    def md5(cls, raw: str) -> str:
        try:
            md5_hash = hashlib.md5(raw.encode('utf-8')).digest()
            return base64.b64encode(md5_hash).decode('utf-8')
        except Exception as e:
            raise RuntimeError(f"Failed to calculate MD5: {e}")

    @classmethod
    def _hmac_sha256(cls, key: bytes, data: str) -> bytes:
        try:
            return hmac.new(key, data.encode('utf-8'), hashlib.sha256).digest()
        except Exception as e:
            raise RuntimeError(f"Failed to calculate HMAC-SHA256: {e}")

    @classmethod
    def get_canonical_request(cls,
                              rest_auth_parameter: RESTAuthParameter,
                              headers: Dict[str, str]) -> str:
        canonical_request = cls.NEW_LINE.join([
            rest_auth_parameter.method,
            rest_auth_parameter.path
        ])

        canonical_query_string = cls._build_canonical_query_string(rest_auth_parameter.parameters)
        canonical_request = cls.NEW_LINE.join([canonical_request, canonical_query_string])

        sorted_signed_headers_map = cls._build_sorted_signed_headers_map(headers)
        for key, value in sorted_signed_headers_map.items():
            canonical_request = cls.NEW_LINE.join([
                canonical_request,
                f"{key}:{value}"
            ])

        content_sha256 = headers.get(
            DLFAuthProvider.DLF_CONTENT_SHA56_HEADER_KEY,
            DLFAuthProvider.DLF_CONTENT_SHA56_VALUE
        )

        return cls.NEW_LINE.join([canonical_request, content_sha256])

    @classmethod
    def _build_canonical_query_string(cls, parameters: Optional[Dict[str, str]]) -> str:
        if not parameters:
            return ""

        sorted_params = OrderedDict(sorted(parameters.items()))

        query_parts = []
        for key, value in sorted_params.items():
            key = cls._trim(key)
            if value is not None and value != "":
                value = cls._trim(value)
                query_parts.append(f"{key}={value}")
            else:
                query_parts.append(key)

        return "&".join(query_parts)

    @classmethod
    def _build_sorted_signed_headers_map(cls, headers: Optional[Dict[str, str]]) -> OrderedDict:
        sorted_headers = OrderedDict()

        if headers:
            for key, value in headers.items():
                lower_key = key.lower()
                if lower_key in cls.SIGNED_HEADERS:
                    sorted_headers[lower_key] = cls._trim(value)

        return OrderedDict(sorted(sorted_headers.items()))

    @classmethod
    def _sha256_hex(cls, raw: str) -> str:
        try:
            sha256_hash = hashlib.sha256(raw.encode('utf-8')).digest()
            return cls._hex_encode(sha256_hash)
        except Exception as e:
            raise RuntimeError(f"Failed to calculate SHA256: {e}")

    @classmethod
    def _hex_encode(cls, raw: bytes) -> str:
        if raw is None:
            return None

        return raw.hex()

    @classmethod
    def _trim(cls, value: str) -> str:
        return value.strip() if value else ""
