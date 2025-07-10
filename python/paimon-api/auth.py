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

from requests import PreparedRequest
from requests.auth import AuthBase

@dataclass
class RESTAuthParameter:
    """REST 认证参数"""
    method: str
    path: str
    data: str
    parameters: Dict[str, str]



@dataclass
class DLFToken:
    """DLF Token 信息"""
    access_key_id: str
    access_key_secret: str
    security_token: Optional[str] = None


class DLFAuthProvider:
    """DLF 认证提供者常量"""
    DLF_AUTH_VERSION_HEADER_KEY = "x-dlf-version"
    DLF_CONTENT_MD5_HEADER_KEY = "content-md5"
    DLF_CONTENT_SHA56_HEADER_KEY = "x-dlf-content-sha256"
    DLF_CONTENT_TYPE_KEY = "content-type"
    DLF_DATE_HEADER_KEY = "x-dlf-date"
    DLF_SECURITY_TOKEN_HEADER_KEY = "x-dlf-security-token"
    DLF_CONTENT_SHA56_VALUE = "UNSIGNED-PAYLOAD"


class DLFAuthSignature:
    """生成阿里云 DLF 的授权签名"""

    VERSION = "v1"

    # 常量定义
    SIGNATURE_ALGORITHM = "DLF4-HMAC-SHA256"
    PRODUCT = "DlfNext"
    HMAC_SHA256 = "sha256"
    REQUEST_TYPE = "aliyun_v4_request"
    SIGNATURE_KEY = "Signature"
    NEW_LINE = "\n"

    # 需要签名的头部列表
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
            # 构建规范请求
            canonical_request = cls.get_canonical_request(rest_auth_parameter, headers)

            # 构建待签名字符串
            string_to_sign = cls.NEW_LINE.join([
                cls.SIGNATURE_ALGORITHM,
                date_time,
                f"{date}/{region}/{cls.PRODUCT}/{cls.REQUEST_TYPE}",
                cls._sha256_hex(canonical_request)
            ])

            # 计算签名密钥
            date_key = cls._hmac_sha256(f"aliyun_v4{dlf_token.access_key_secret}".encode('utf-8'), date)
            date_region_key = cls._hmac_sha256(date_key, region)
            date_region_service_key = cls._hmac_sha256(date_region_key, cls.PRODUCT)
            signing_key = cls._hmac_sha256(date_region_service_key, cls.REQUEST_TYPE)

            # 计算最终签名
            result = cls._hmac_sha256(signing_key, string_to_sign)
            signature = cls._hex_encode(result)

            # 构建授权字符串
            credential = f"{cls.SIGNATURE_ALGORITHM} Credential={dlf_token.access_key_id}/{date}/{region}/{cls.PRODUCT}/{cls.REQUEST_TYPE}"
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

class AuthProvider(ABC):

    @abstractmethod
    def merge_auth_header(self, base_header: Dict[str, str], parammeter: RESTAuthParameter) -> Dict[str, str]:
        """Merge authorization header into header."""

class DLFAuthProvider(AuthProvider):
    """阿里云 DLF 认证提供者"""

    # 常量定义
    DLF_AUTHORIZATION_HEADER_KEY = "Authorization"
    DLF_CONTENT_MD5_HEADER_KEY = "Content-MD5"
    DLF_CONTENT_TYPE_KEY = "Content-Type"
    DLF_DATE_HEADER_KEY = "x-dlf-date"
    DLF_SECURITY_TOKEN_HEADER_KEY = "x-dlf-security-token"
    DLF_AUTH_VERSION_HEADER_KEY = "x-dlf-version"
    DLF_CONTENT_SHA56_HEADER_KEY = "x-dlf-content-sha256"
    DLF_CONTENT_SHA56_VALUE = "UNSIGNED-PAYLOAD"

    # 日期时间格式化器
    AUTH_DATE_TIME_FORMAT = "%Y%m%dT%H%M%SZ"
    MEDIA_TYPE = "application/json"

    def __init__(self,
                 token: DLFToken,
                 region: str = "cn-hangzhou"):
        """
        初始化 DLF 认证提供者

        Args:
            token_loader: Token 加载器
            token: 静态 Token
            region: 区域
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.token = token
        self.region = region
        self._lock = threading.Lock()

    def merge_auth_header(self,
                          base_header: Dict[str, str],
                          rest_auth_parameter: RESTAuthParameter) -> Dict[str, str]:
        """
        合并认证头部

        Args:
            base_header: 基础头部
            rest_auth_parameter: REST 认证参数

        Returns:
            包含认证信息的头部字典
        """
        try:
            # 获取或生成日期时间
            date_time = base_header.get(
                self.DLF_DATE_HEADER_KEY.lower(),
                datetime.now(timezone.utc).strftime(self.AUTH_DATE_TIME_FORMAT)
            )
            date = date_time[:8]  # 提取日期部分

            # 生成签名头部
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

            # 合并所有头部
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

        # 必需的头部
        sign_headers[cls.DLF_DATE_HEADER_KEY] = date_time
        sign_headers[cls.DLF_CONTENT_SHA56_HEADER_KEY] = cls.DLF_CONTENT_SHA56_VALUE
        sign_headers[cls.DLF_AUTH_VERSION_HEADER_KEY] = "v1"  # DLFAuthSignature.VERSION
        sign_headers[cls.DLF_CONTENT_TYPE_KEY] = cls.MEDIA_TYPE

        # 如果有数据，添加内容相关头部
        if data is not None and data != "":
            sign_headers[cls.DLF_CONTENT_MD5_HEADER_KEY] = DLFAuthSignature.md5(data)

        # 如果有安全令牌，添加安全令牌头部
        if security_token is not None:
            sign_headers[cls.DLF_SECURITY_TOKEN_HEADER_KEY] = security_token
        return sign_headers

# 方式2: 使用类实现函数对象
class RESTAuthFunction:
    """REST 认证函数实现类"""

    def __init__(self, init_header: Dict[str, str], auth_provider: AuthProvider):
        """
        初始化 REST 认证函数

        Args:
            init_header: 初始头部信息
            auth_provider: 认证提供者
        """
        self.init_header = init_header.copy() if init_header else {}
        self.auth_provider = auth_provider

    def __call__(self, rest_auth_parameter: RESTAuthParameter) -> Dict[str, str]:
        """
        应用认证函数

        Args:
            rest_auth_parameter: REST 认证参数

        Returns:
            包含认证信息的头部字典
        """
        return self.auth_provider.merge_auth_header(self.init_header, rest_auth_parameter)

    def apply(self, rest_auth_parameter: RESTAuthParameter) -> Dict[str, str]:
        """
        应用认证函数（与 Java 接口保持一致）

        Args:
            rest_auth_parameter: REST 认证参数

        Returns:
            包含认证信息的头部字典
        """
        return self.__call__(rest_auth_parameter)