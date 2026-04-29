# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from unittest import mock

from pypaimon.api.client import HttpClient, _parse_error_response


class HttpClientTest(unittest.TestCase):
    def test_parse_error_response_with_valid_json(self):
        """Test parsing a valid error response JSON"""
        response_body = (
            '{"message": "Table not found", "code": 404, '
            '"resourceType": "table", "resourceName": "my_table"}'
        )
        error = _parse_error_response(response_body, 404)

        self.assertEqual(error.message, "Table not found")
        self.assertEqual(error.code, 404)
        self.assertEqual(error.resource_type, "table")
        self.assertEqual(error.resource_name, "my_table")

    def test_parse_error_response_with_unparsable_json(self):
        # Test unparsable JSON with uppercase fields
        response_body = '{"Message":"Your request is denied as lack of ssl protect.","Code":"InvalidProtocol.NeedSsl"}'
        error = _parse_error_response(response_body, 403)
        self.assertEqual(error.message, response_body)
        self.assertEqual(error.code, 403)
        self.assertEqual(error.resource_type, '')
        self.assertEqual(error.resource_name, '')

        # Test plain text response
        response_body = "Internal Server Error: Database connection failed"
        error = _parse_error_response(response_body, 500)
        self.assertEqual(error.message, response_body)
        self.assertEqual(error.code, 500)
        self.assertEqual(error.resource_type, '')
        self.assertEqual(error.resource_name, '')

        # Test null body
        error = _parse_error_response(None, 500)
        self.assertEqual(error.message, "response body is null")
        self.assertEqual(error.code, 500)
        self.assertEqual(error.resource_type, '')
        self.assertEqual(error.resource_name, '')


class HttpClientTimeoutTest(unittest.TestCase):
    """HttpClient passes its timeout to ``Session.request``.

    ``Session.timeout`` was previously set as an attribute, which the
    requests library does not honour — only ``Session.request(timeout=
    ...)`` does. This test pins the fix in place: every outgoing call
    must carry the configured ``(connect, read)`` tuple, otherwise the
    process would hang forever on a slow upstream.
    """

    def test_session_request_receives_timeout_tuple(self):
        client = HttpClient("http://localhost:8080")
        # 3-minute connect / read timeouts are the conservative default
        # documented on ``HttpClient``; pin them here so the value is
        # caught if someone changes them silently.
        self.assertEqual(client._timeout, (180, 180))

        with mock.patch.object(client.session, 'request') as req:
            req.return_value = mock.Mock(
                status_code=200, text='{}', headers={},
                json=lambda: {})
            client._execute_request('GET', 'http://localhost:8080/x')

        self.assertTrue(req.called, "session.request must be invoked")
        kwargs = req.call_args.kwargs
        self.assertEqual(
            kwargs.get('timeout'), client._timeout,
            "request must carry the timeout; Session.timeout is dead code")


if __name__ == '__main__':
    unittest.main()
