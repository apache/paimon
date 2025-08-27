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

import traceback
from typing import Any, Optional


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
