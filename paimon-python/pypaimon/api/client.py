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
import traceback
import urllib.parse
from abc import ABC, abstractmethod
from typing import Dict, Optional, Type, TypeVar, Callable, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from .typedef import RESTAuthParameter
from .api_response import ErrorResponse
from .rest_json import JSON

T = TypeVar('T', bound='RESTResponse')


class RESTRequest(ABC):
    pass


class RESTException(Exception):
    def __init__(self, message: str = None, *args: Any, cause: Optional[Exception] = None):
        if message and args:
            try:
                formatted_message = message % args
            except (TypeError, ValueError):
                formatted_message = f"{message} {' '.join(str(arg) for arg in args)}"
        else:
            formatted_message = message or "REST API error occurred"

        super().__init__(formatted_message)
        self.__cause__ = cause

    def get_cause(self) -> Optional[Exception]:
        return self.__cause__

    def get_message(self) -> str:
        return str(self)

    def print_stack_trace(self) -> None:
        traceback.print_exception(type(self), self, self.__traceback__)

    def get_stack_trace(self) -> str:
        return ''.join(traceback.format_exception(type(self), self, self.__traceback__))

    def __repr__(self) -> str:
        if self.__cause__:
            return f"{self.__class__.__name__}('{self}', caused by {type(self.__cause__).__name__}: {self.__cause__})"
        return f"{self.__class__.__name__}('{self}')"


class BadRequestException(RESTException):

    def __init__(self, message: str = None, *args: Any):
        super().__init__(message, *args)


class NotAuthorizedException(RESTException):
    """Exception for not authorized (401)"""

    def __init__(self, message: str, *args: Any):
        super().__init__(message, *args)


class ForbiddenException(RESTException):
    """Exception for forbidden access (403)"""

    def __init__(self, message: str, *args: Any):
        super().__init__(message, *args)


class NoSuchResourceException(RESTException):
    """Exception for resource not found (404)"""

    def __init__(self, resource_type: Optional[str], resource_name: Optional[str],
                 message: str, *args: Any):
        self.resource_type = resource_type
        self.resource_name = resource_name
        super().__init__(message, *args)


class AlreadyExistsException(RESTException):
    """Exception for resource already exists (409)"""

    def __init__(self, resource_type: Optional[str], resource_name: Optional[str],
                 message: str, *args: Any):
        self.resource_type = resource_type
        self.resource_name = resource_name
        super().__init__(message, *args)


class ServiceFailureException(RESTException):
    """Exception for service failure (500)"""

    def __init__(self, message: str, *args: Any):
        super().__init__(message, *args)


class NotImplementedException(RESTException):
    """Exception for not implemented (501)"""

    def __init__(self, message: str, *args: Any):
        super().__init__(message, *args)


class ServiceUnavailableException(RESTException):
    """Exception for service unavailable (503)"""

    def __init__(self, message: str, *args: Any):
        super().__init__(message, *args)


class ErrorHandler(ABC):

    @abstractmethod
    def accept(self, error: ErrorResponse, request_id: str) -> None:
        pass


# DefaultErrorHandler implementation
class DefaultErrorHandler(ErrorHandler):
    """
    Default error handler that converts error responses to appropriate exceptions.

    This class implements the singleton pattern and handles various HTTP error codes
    by throwing corresponding exception types.
    """

    _instance: Optional['DefaultErrorHandler'] = None

    def __new__(cls) -> 'DefaultErrorHandler':
        """Implement singleton pattern"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'DefaultErrorHandler':
        """Get the singleton instance of DefaultErrorHandler"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def accept(self, error: ErrorResponse, request_id: str) -> None:
        """
        Handle an error response by throwing appropriate exception.

        Args:
            error: The error response to handle
            request_id: The request ID associated with the error

        Raises:
            Appropriate exception based on error code
        """
        code = error.code

        # Format message with request ID if not default
        if LoggingInterceptor.DEFAULT_REQUEST_ID == request_id:
            message = error.message
        else:
            # If we have a requestId, append it to the message
            message = f"{error.message} requestId:{request_id}"

        # Handle different error codes
        if code == 400:
            raise BadRequestException("%s", message)

        elif code == 401:
            raise NotAuthorizedException("Not authorized: %s", message)

        elif code == 403:
            raise ForbiddenException("Forbidden: %s", message)

        elif code == 404:
            raise NoSuchResourceException(
                error.resource_type,
                error.resource_name,
                "%s",
                message
            )

        elif code in [405, 406]:
            # These codes are handled but don't throw exceptions
            pass

        elif code == 409:
            raise AlreadyExistsException(
                error.resource_type,
                error.resource_name,
                "%s",
                message
            )

        elif code == 500:
            raise ServiceFailureException("Server error: %s", message)

        elif code == 501:
            raise NotImplementedException(message)

        elif code == 503:
            raise ServiceUnavailableException("Service unavailable: %s", message)

        else:
            # Default case for unhandled codes
            pass

        # If no specific exception was thrown, throw generic RESTException
        raise RESTException("Unable to process: %s", message)


class ExponentialRetry:

    adapter: HTTPAdapter

    def __init__(self, max_retries: int = 5):
        retry = self.__create_retry_strategy(max_retries)
        self.adapter = HTTPAdapter(max_retries=retry)

    @staticmethod
    def __create_retry_strategy(max_retries: int) -> Retry:
        retry_kwargs = {
            'total': max_retries,
            'read': max_retries,
            'connect': max_retries,
            'backoff_factor': 1,
            'status_forcelist': [429, 502, 503, 504],
            'raise_on_status': False,
            'raise_on_redirect': False,
        }
        retry_methods = ["GET", "HEAD", "PUT", "DELETE", "TRACE", "OPTIONS"]
        retry_instance = Retry()
        if hasattr(retry_instance, 'allowed_methods'):
            retry_kwargs['allowed_methods'] = retry_methods
        else:
            retry_kwargs['method_whitelist'] = retry_methods
        return Retry(**retry_kwargs)


class LoggingInterceptor:
    REQUEST_ID_KEY = "x-request-id"
    DEFAULT_REQUEST_ID = "unknown"

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def log_request(self, method: str, url: str, headers: Dict[str, str]) -> None:
        request_id = headers.get(self.REQUEST_ID_KEY, self.DEFAULT_REQUEST_ID)
        self.logger.debug(f"Request [{request_id}]: {method} {url}")

    def log_response(self, status_code: int, headers: Dict[str, str]) -> None:
        request_id = headers.get(self.REQUEST_ID_KEY, self.DEFAULT_REQUEST_ID)
        self.logger.debug(f"Response [{request_id}]: {status_code}")


class RESTClient(ABC):

    @abstractmethod
    def get(self, path: str, response_type: Type[T],
            rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        pass

    @abstractmethod
    def get_with_params(self, path: str, query_params: Dict[str, str],
                        response_type: Type[T],
                        rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        pass

    @abstractmethod
    def post(self, path: str, body: RESTRequest,
             rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        pass

    @abstractmethod
    def post_with_response_type(self, path: str, body: RESTRequest, response_type: Type[T],
                                rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        pass

    @abstractmethod
    def delete(self, path: str,
               rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        pass

    @abstractmethod
    def delete_with_body(self, path: str, body: RESTRequest,
                         rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        pass


def _normalize_uri(uri: str) -> str:
    if not uri or uri.strip() == "":
        raise ValueError("uri is empty which must be defined.")

    server_uri = uri.strip()

    if server_uri.endswith("/"):
        server_uri = server_uri[:-1]

    if not server_uri.startswith("http://") and not server_uri.startswith("https://"):
        server_uri = f"http://{server_uri}"

    return server_uri


def _parse_error_response(response_body: Optional[str], status_code: int) -> ErrorResponse:
    if response_body:
        try:
            return JSON.from_json(response_body, ErrorResponse)
        except Exception:
            return ErrorResponse(
                resource_type=None,
                resource_name=None,
                message=response_body,
                code=status_code
            )
    else:
        return ErrorResponse(
            resource_type=None,
            resource_name=None,
            message="response body is null",
            code=status_code
        )


def _get_headers_with_params(path: str, query_params: Dict[str, str],
                             method: str, data: str,
                             header_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> Dict[str, str]:
    rest_auth_parameter = RESTAuthParameter(
        path=path,
        parameters=query_params,
        method=method,
        data=data
    )
    return header_function(rest_auth_parameter)


def _get_headers(path: str, method: str, query_params: Dict[str, str], data: str,
                 header_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> Dict[str, str]:
    return _get_headers_with_params(path, query_params, method, data, header_function)


class HttpClient(RESTClient):

    def __init__(self, uri: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.uri = _normalize_uri(uri)
        self.error_handler = DefaultErrorHandler.get_instance()
        self.logging_interceptor = LoggingInterceptor()

        self.session = requests.Session()

        retry_interceptor = ExponentialRetry(max_retries=3)
        adapter = HTTPAdapter(max_retries=retry_interceptor.adapter)

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.session.timeout = (180, 180)

        self.session.headers.update({
            'Accept': 'application/json'
        })

    def set_error_handler(self, error_handler: ErrorHandler) -> None:
        self.error_handler = error_handler

    def get(self, path: str, response_type: Type[T],
            rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        auth_headers = _get_headers(path, "GET", {}, "", rest_auth_function)
        url = self._get_request_url(path, None)

        return self._execute_request("GET", url, headers=auth_headers,
                                     response_type=response_type)

    def get_with_params(self, path: str, query_params: Dict[str, str],
                        response_type: Type[T],
                        rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        auth_headers = _get_headers(path, "GET", query_params, None, rest_auth_function)
        url = self._get_request_url(path, query_params)

        return self._execute_request("GET", url, headers=auth_headers,
                                     response_type=response_type)

    def post(self, path: str, body: RESTRequest,
             rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        return self.post_with_response_type(path, body, None, rest_auth_function)

    def post_with_response_type(self, path: str, body: RESTRequest, response_type: Optional[Type[T]],
                                rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        try:
            body_str = JSON.to_json(body)
            auth_headers = _get_headers(path, "POST", None, body_str, rest_auth_function)
            url = self._get_request_url(path, None)
            return self._execute_request("POST", url, data=body_str, headers=auth_headers, response_type=response_type)
        except RESTException as e:
            raise e
        except Exception as e:
            raise RESTException("build request failed.", cause=e)

    def delete(self, path: str,
               rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        auth_headers = _get_headers(path, "DELETE", None, "", rest_auth_function)
        url = self._get_request_url(path, None)

        return self._execute_request("DELETE", url, headers=auth_headers, response_type=None)

    def delete_with_body(self, path: str, body: RESTRequest,
                         rest_auth_function: Callable[[RESTAuthParameter], Dict[str, str]]) -> T:
        try:
            body_str = JSON.to_json(body)
            auth_headers = _get_headers(path, "DELETE", body_str, rest_auth_function)
            url = self._get_request_url(path, None)

            return self._execute_request("DELETE", url, data=body_str, headers=auth_headers,
                                         response_type=None)
        except json.JSONEncodeError as e:
            raise RESTException("build request failed.", e)

    def _get_request_url(self, path: str, query_params: Optional[Dict[str, str]]) -> str:
        if not path or path.strip() == "":
            full_path = self.uri
        else:
            full_path = self.uri + path

        if query_params:
            query_string = urllib.parse.urlencode(query_params)
            full_path = f"{full_path}?{query_string}"

        return full_path

    def get_uri(self) -> str:
        return self.uri

    def _execute_request(self, method: str, url: str,
                         data: Optional[str] = None,
                         headers: Optional[Dict[str, str]] = None,
                         response_type: Optional[Type[T]] = None) -> T:
        try:
            if headers:
                self.logging_interceptor.log_request(method, url, headers)

            response = self.session.request(
                method=method,
                url=url,
                data=data.encode('utf-8') if data else None,
                headers=headers
            )

            response_headers = dict(response.headers)
            self.logging_interceptor.log_response(response.status_code, response_headers)

            response_body_str = response.text if response.text else None

            if not response.ok:
                error = _parse_error_response(response_body_str, response.status_code)
                request_id = response.headers.get(
                    LoggingInterceptor.REQUEST_ID_KEY,
                    LoggingInterceptor.DEFAULT_REQUEST_ID
                )
                self.error_handler.accept(error, request_id)

            if response_type is not None and response_body_str is not None:
                return JSON.from_json(response_body_str, response_type)
            elif response_type is None:
                return None
            else:
                raise RESTException("response body is null.")

        except RESTException as e:
            raise e
        except Exception as e:
            raise RESTException("rest exception", cause=e)
