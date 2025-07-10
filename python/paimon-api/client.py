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

import json
import logging
import time
import urllib.parse
from abc import ABC, abstractmethod
from typing import Dict, Optional, Type, TypeVar, Callable, Any
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from auth import RESTAuthParameter
from api import RESTApi
from response import RESTResponse

# 类型定义
T = TypeVar('T', bound='RESTResponse')


class RESTRequest(ABC):
    """REST 请求基类"""
    pass

@dataclass
class ErrorResponse(RESTResponse):
    """错误响应"""
    error_type: Optional[str] = None
    error_code: Optional[str] = None
    message: Optional[str] = None
    status_code: Optional[int] = None


class RESTException(Exception):
    """REST 异常"""

    def __init__(self, message: str, cause: Optional[Exception] = None):
        super().__init__(message)
        self.cause = cause


class ErrorHandler(ABC):
    """错误处理器抽象基类"""

    @abstractmethod
    def accept(self, error: ErrorResponse, request_id: str) -> None:
        """处理错误"""
        pass


class DefaultErrorHandler(ErrorHandler):
    """默认错误处理器"""

    _instance = None

    @classmethod
    def get_instance(cls) -> 'DefaultErrorHandler':
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def accept(self, error: ErrorResponse, request_id: str) -> None:
        """处理错误"""
        message = f"REST API error (request_id: {request_id}): {error.message}"
        if error.error_code:
            message += f" (code: {error.error_code})"
        if error.error_type:
            message += f" (type: {error.error_type})"

        raise RESTException(message)


class ExponentialRetryInterceptor:
    """指数退避重试拦截器"""

    def __init__(self, max_retries: int = 5):
        self.max_retries = max_retries
        self.logger = logging.getLogger(self.__class__.__name__)

    def create_retry_strategy(self) -> Retry:
        """创建重试策略"""
        return Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
            backoff_factor=1,
            raise_on_status=False
        )


class LoggingInterceptor:
    """日志拦截器"""

    REQUEST_ID_KEY = "X-Request-ID"
    DEFAULT_REQUEST_ID = "unknown"

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def log_request(self, method: str, url: str, headers: Dict[str, str]) -> None:
        """记录请求日志"""
        request_id = headers.get(self.REQUEST_ID_KEY, self.DEFAULT_REQUEST_ID)
        self.logger.debug(f"Request [{request_id}]: {method} {url}")

    def log_response(self, status_code: int, headers: Dict[str, str]) -> None:
        """记录响应日志"""
        request_id = headers.get(self.REQUEST_ID_KEY, self.DEFAULT_REQUEST_ID)
        self.logger.debug(f"Response [{request_id}]: {status_code}")



class RESTClient(ABC):
    """REST 客户端抽象基类"""

    @abstractmethod
    def get(self, path: str, response_type: Type[T],
            rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """GET 请求"""
        pass

    @abstractmethod
    def get_with_params(self, path: str, query_params: Dict[str, str],
                        response_type: Type[T],
                        rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """带参数的 GET 请求"""
        pass

    @abstractmethod
    def post(self, path: str, body: RESTRequest,
             rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """POST 请求"""
        pass

    @abstractmethod
    def post_with_response_type(self, path: str, body: RESTRequest, response_type: Type[T],
                                rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """带响应类型的 POST 请求"""
        pass

    @abstractmethod
    def delete(self, path: str,
               rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """DELETE 请求"""
        pass

    @abstractmethod
    def delete_with_body(self, path: str, body: RESTRequest,
                         rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """带请求体的 DELETE 请求"""
        pass


class HttpClient(RESTClient):
    """HTTP 客户端实现"""

    MEDIA_TYPE = "application/json"

    def __init__(self, uri: str):
        """
        初始化 HTTP 客户端

        Args:
            uri: 服务器 URI
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.uri = self._normalize_uri(uri)
        self.error_handler = DefaultErrorHandler.get_instance()
        self.logging_interceptor = LoggingInterceptor()

        # 创建 HTTP 会话
        self.session = requests.Session()

        # 配置重试策略
        retry_interceptor = ExponentialRetryInterceptor(max_retries=5)
        retry_strategy = retry_interceptor.create_retry_strategy()
        adapter = HTTPAdapter(max_retries=retry_strategy)

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # 设置超时
        self.session.timeout = (180, 180)  # (连接超时, 读取超时) 3分钟

        # 设置默认头部
        self.session.headers.update({
            'Content-Type': self.MEDIA_TYPE,
            'Accept': self.MEDIA_TYPE
        })

    def _normalize_uri(self, uri: str) -> str:
        """标准化 URI"""
        if not uri or uri.strip() == "":
            raise ValueError("uri is empty which must be defined.")

        server_uri = uri.strip()

        # 移除末尾的斜杠
        if server_uri.endswith("/"):
            server_uri = server_uri[:-1]

        # 添加协议前缀
        if not server_uri.startswith("http://") and not server_uri.startswith("https://"):
            server_uri = f"http://{server_uri}"

        return server_uri

    def set_error_handler(self, error_handler: ErrorHandler) -> None:
        """设置错误处理器（用于测试）"""
        self.error_handler = error_handler

    def get(self, path: str, response_type: Type[T],
            rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """GET 请求"""
        auth_headers = self._get_headers(path, "GET", {}, "", rest_auth_function)
        url = self._get_request_url(path, None)

        return self._execute_request("GET", url, headers=auth_headers,
                                     response_type=response_type)

    def get_with_params(self, path: str, query_params: Dict[str, str],
                        response_type: Type[T],
                        rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """带参数的 GET 请求"""
        auth_headers = self._get_headers(path, "GET", query_params, None, rest_auth_function)
        url = self._get_request_url(path, query_params)

        return self._execute_request("GET", url, headers=auth_headers,
                                     response_type=response_type)

    def post(self, path: str, body: RESTRequest,
             rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """POST 请求"""
        return self.post_with_response_type(path, body, None, rest_auth_function)

    def post_with_response_type(self, path: str, body: RESTRequest, response_type: Optional[Type[T]],
                                rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """带响应类型的 POST 请求"""
        try:
            body_str = RESTApi.to_json(body)
            auth_headers = self._get_headers(path, "POST", body_str, rest_auth_function)
            url = self._get_request_url(path, None)

            return self._execute_request("POST", url, data=body_str, headers=auth_headers,
                                         response_type=response_type)
        except json.JSONEncodeError as e:
            raise RESTException("build request failed.", e)

    def delete(self, path: str,
               rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """DELETE 请求"""
        auth_headers = self._get_headers(path, "DELETE", "", rest_auth_function)
        url = self._get_request_url(path, None)

        return self._execute_request("DELETE", url, headers=auth_headers, response_type=None)

    def delete_with_body(self, path: str, body: RESTRequest,
                         rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        """带请求体的 DELETE 请求"""
        try:
            body_str = RESTApi.to_json(body)
            auth_headers = self._get_headers(path, "DELETE", body_str, rest_auth_function)
            url = self._get_request_url(path, None)

            return self._execute_request("DELETE", url, data=body_str, headers=auth_headers,
                                         response_type=None)
        except json.JSONEncodeError as e:
            raise RESTException("build request failed.", e)

    def _get_request_url(self, path: str, query_params: Optional[Dict[str, str]]) -> str:
        """构建请求 URL"""
        if not path or path.strip() == "":
            full_path = self.uri
        else:
            full_path = self.uri + path

        if query_params:
            query_string = urllib.parse.urlencode(query_params)
            full_path = f"{full_path}?{query_string}"

        return full_path

    def get_uri(self) -> str:
        """获取 URI（用于测试）"""
        return self.uri

    def _execute_request(self, method: str, url: str,
                         data: Optional[str] = None,
                         headers: Optional[Dict[str, str]] = None,
                         response_type: Optional[Type[T]] = None) -> T:
        """执行 HTTP 请求"""
        try:
            # 记录请求日志
            if headers:
                self.logging_interceptor.log_request(method, url, headers)

            # 执行请求
            response = self.session.request(
                method=method,
                url=url,
                data=data.encode('utf-8') if data else None,
                headers=headers
            )

            # 记录响应日志
            response_headers = dict(response.headers)
            self.logging_interceptor.log_response(response.status_code, response_headers)

            # 获取响应体
            response_body_str = response.text if response.text else None

            # 处理错误响应
            if not response.ok:
                error = self._parse_error_response(response_body_str, response.status_code)
                request_id = response.headers.get(
                    LoggingInterceptor.REQUEST_ID_KEY,
                    LoggingInterceptor.DEFAULT_REQUEST_ID
                )
                self.error_handler.accept(error, request_id)

            # 解析成功响应
            if response_type is not None and response_body_str is not None:
                return json.loads(response_body_str)
            elif response_type is None:
                return None
            else:
                raise RESTException("response body is null.")

        except RESTException:
            raise
        except Exception as e:
            raise RESTException("rest exception", e)

    def _parse_error_response(self, response_body: Optional[str], status_code: int) -> ErrorResponse:
        """解析错误响应"""
        if response_body:
            try:
                return RESTApi.from_json(response_body, ErrorResponse)
            except json.JSONDecodeError:
                return ErrorResponse(
                    error_type=None,
                    error_code=None,
                    message=response_body,
                    status_code=status_code
                )
        else:
            return ErrorResponse(
                error_type=None,
                error_code=None,
                message="response body is null",
                status_code=status_code
            )

    def _get_headers(self, path: str, method: str, query_params: Dict[str, str], data: str,
                     header_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> Dict[str, str]:
        """获取请求头部"""
        return self._get_headers_with_params(path, query_params, method, data, header_function)

    def _get_headers_with_params(self, path: str, query_params: Dict[str, str],
                                 method: str, data: str,
                                 header_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> Dict[str, str]:
        """获取带参数的请求头部"""
        rest_auth_parameter = RESTAuthParameter(
            path=path,
            parameters=query_params,
            method=method,
            data=data
        )
        return header_function(rest_auth_parameter)
