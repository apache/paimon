# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Transport REST branch operations and structured errors to/from Rust core."""

import logging
import os
import re
import time
from email.message import Message

import requests
from requests.structures import CaseInsensitiveDict

from pypaimon.api.api_response import ErrorResponse
from pypaimon.api.auth import RESTAuthFunction
from pypaimon.api.auth.bearer import BearTokenAuthProvider
from pypaimon.api.auth.dlf_provider import DLFAuthProvider
from pypaimon.api.client import DefaultErrorHandler, HttpClient
from pypaimon.api.rest_api import RESTApi
from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.common.options.core_options import CoreOptions

logger = logging.getLogger(__name__)


def _native_headers(api):
    session = api.client.session
    if (type(session) is not requests.Session or session.cookies or session.auth is not None
            or session.cert is not None or session.verify is not True or session.proxies or session.params
            or not session.trust_env or any(session.hooks.values())
            or set(session.adapters) != {'http://', 'https://'}
            or any(type(adapter) is not requests.adapters.HTTPAdapter for adapter in session.adapters.values())
            or api.client._timeout != (180, 180)
            or os.environ.get('REQUESTS_CA_BUNDLE') or os.environ.get('CURL_CA_BUNDLE')):
        # Keep cookie selection/expiry and custom transport credentials in their
        # owning Session. Rebuilding a client cannot preserve those semantics.
        return None
    headers = CaseInsensitiveDict(session.headers)
    # Let each HTTP transport advertise only the codecs it can decode and manage
    # its own connections. An explicitly customized transport header stays Python.
    for name in ('Accept-Encoding', 'Connection'):
        if (headers.get(name) != requests.utils.default_headers().get(name)
                or any(key.lower() == name.lower() for key in api.rest_auth_function.init_header)):
            return None
        headers.pop(name, None)
    headers.update(api.rest_auth_function.init_header)
    return {key: value for key, value in headers.items() if value is not None}


def _native_catalog(catalog, operation):
    from pypaimon.catalog.rest.rest_catalog import RESTCatalog

    if (type(catalog) is not RESTCatalog or type(catalog.context) is not CatalogContext
            or type(catalog.rest_api) is not RESTApi
            or any(getattr(catalog.context, attr, None) is not None for attr in (
                'hadoop_conf', 'prefer_io_loader', 'fallback_io_loader'))):
        return None
    options = CoreOptions(catalog.context.options)
    enabled = options.native_read_enabled() if operation == 'list_branches' else options.native_write_enabled()
    if not enabled:
        return None
    api = catalog.rest_api
    if (type(api.client) is not HttpClient
            or type(api.client.error_handler) is not DefaultErrorHandler
            or type(api.rest_auth_function) is not RESTAuthFunction
            or type(api.rest_auth_function.auth_provider) not in (BearTokenAuthProvider, DLFAuthProvider)):
        return None
    headers = _native_headers(api)
    if headers is None:
        return None
    try:
        from pypaimon_rust.datafusion import CatalogError, PaimonCatalog
    except ImportError:
        return None
    from pypaimon.read.native_plan import _option_value_to_string, _raise_if_native_fork_safety_error
    resolved = {key: value for key, value in api.options.to_map().items()
                if not RESTApi._is_auth_option(key) and not key.startswith(RESTApi.HEADER_PREFIX)}
    resolved['uri'] = api.client.uri
    resolved.update({RESTApi.HEADER_PREFIX + key: value for key, value in headers.items()})
    resolved = {str(key): _option_value_to_string(value)
                for key, value in resolved.items() if value is not None}
    resolved['metastore'] = 'rest'
    try:
        provider = api.rest_auth_function.auth_provider
        if type(provider) is DLFAuthProvider:
            # The owning provider retains Java's STS cache/refresh policy. Pass
            # its current credentials for this call; Rust still signs the request.
            token = provider.get_token()
            resolved.update({
                'token.provider': 'dlf', 'dlf.region': provider.region,
                'dlf.signing-algorithm': provider.signing_algorithm,
                'dlf.access-key-id': token.access_key_id,
                'dlf.access-key-secret': token.access_key_secret,
            })
            if token.security_token is not None:
                resolved['dlf.security-token'] = token.security_token
        else:
            resolved.update({'token.provider': 'bear', 'token': provider.token})
        # Reuse the resolved configuration, as RESTCatalogLoader does. Keep no
        # opaque native state on the Python catalog across pickle or process fork.
        return PaimonCatalog(resolved, config_required=False, _capture_response_cookies=True), CatalogError
    except Exception as error:
        _raise_if_native_fork_safety_error(error)
        logger.debug('Native branch preparation failed: %s', error)
        return None


def _restore_response_cookies(session, updates):
    for url, cookies, received_at in updates:
        request = requests.cookies.MockRequest(requests.Request('GET', url).prepare())
        for value in cookies:
            # Max-Age is relative to receipt, not the end of GET retries or
            # response-body reads. Keep invalid attributes for CookieJar to
            # reject using its usual parser.
            elapsed = int(time.time()) - received_at

            def adjust_max_age(match):
                try:
                    age = int(match[2])
                    return match[1] + str(age - elapsed if age > 0 else age)
                except ValueError:
                    return match[0]

            headers = Message()
            headers.add_header('Set-Cookie', re.sub(
                r'(;\s*max-age\s*=\s*)([^;]*)', adjust_max_age, value, flags=re.IGNORECASE))
            session.cookies.extract_cookies(requests.cookies.MockResponse(headers), request)


def call_branch(catalog, operation, identifier, *args):
    # Both backends must reject a path that HTTP clients normalize to another
    # resource. CREATE's branch is in JSON; only DROP/FORWARD put it in the URL.
    path_branch = args[0] if operation in ('drop_branch', 'fast_forward') else None
    RESTApi._validate_branch_path(identifier, path_branch)
    if operation == 'create_branch' and (not args[0] or not args[0].strip()):
        raise ValueError('Branch name cannot be empty')
    prepared = _native_catalog(catalog, operation)
    if prepared is None:
        return getattr(catalog.rest_api, operation)(identifier, *args)
    native, error_type = prepared
    try:
        # Once an operation starts, never execute it again through Python:
        # the REST server may have published it before returning an error.
        return getattr(native, operation)((identifier.get_database_name(), identifier.get_object_name()), *args)
    except error_type as error:
        DefaultErrorHandler.get_instance().accept(ErrorResponse(
            resource_type=error.resource_type, resource_name=error.resource_name,
            message=error.message, code=error.code), HttpClient.DEFAULT_REQUEST_ID)
    finally:
        # A response may establish or revoke a resource-path session even when
        # its status/body fails. Retain it without replaying the operation.
        _restore_response_cookies(catalog.rest_api.client.session, native._take_response_cookies())
