#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import unittest
import re
import threading
from datetime import datetime, timezone

from pypaimon.api.auth import (
    DLFAuthProvider,
    DLFAuthProviderFactory,
    DLFDefaultSigner,
    DLFOpenApiSigner,
)
from pypaimon.api.token_loader import DLFToken
from pypaimon.api.typedef import RESTAuthParameter


class DLFSignerTest(unittest.TestCase):

    def test_default_signer(self):
        signer = DLFDefaultSigner("cn-hangzhou")
        token = DLFToken("AccessKeyId", "AccessKeySecret", "security-token", None)
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)
        body = '{"key":"value"}'

        # Test sign_headers
        headers = signer.sign_headers(body, now, token.security_token, "host")
        self.assertEqual("20250416T034446Z", headers.get("x-dlf-date"))
        self.assertEqual("security-token", headers.get("x-dlf-security-token"))
        self.assertEqual("v1", headers.get("x-dlf-version"))
        self.assertIn("Content-MD5", headers)

        # Test authorization format
        rest_param = RESTAuthParameter("POST", "/test/path", body, {})
        authorization = signer.authorization(rest_param, token, "host", headers)
        self.assertTrue(authorization.startswith("DLF4-HMAC-SHA256 Credential="))
        self.assertIn(",Signature=", authorization)

        # Test identifier
        self.assertEqual("default", signer.identifier())

    def test_openapi_signer(self):
        signer = DLFOpenApiSigner()
        token = DLFToken("AccessKeyId", "AccessKeySecret", "security-token", None)
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)
        host = "dlfnext.cn-beijing.aliyuncs.com"
        body = '{"CategoryName":"test"}'

        # Test sign_headers with body
        headers = signer.sign_headers(body, now, token.security_token, host)
        self.assertEqual("Wed, 16 Apr 2025 03:44:46 GMT", headers.get("Date"))
        self.assertEqual("application/json", headers.get("Accept"))
        self.assertEqual("security-token", headers.get("x-acs-security-token"))
        self.assertEqual("HMAC-SHA1", headers.get("x-acs-signature-method"))
        self.assertEqual("1.0", headers.get("x-acs-signature-version"))
        self.assertEqual("2026-01-18", headers.get("x-acs-version"))
        self.assertEqual(host, headers.get("Host"))
        self.assertIn("Content-MD5", headers)
        self.assertIn("x-acs-signature-nonce", headers)

        # Test sign_headers without body
        headers_no_body = signer.sign_headers(None, now, None, host)
        self.assertNotIn("Content-MD5", headers_no_body)
        self.assertNotIn("Content-Type", headers_no_body)

        # Test authorization format
        headers["x-acs-signature-nonce"] = "fixed-nonce"
        rest_param = RESTAuthParameter("POST", "/api/test", body, {})
        authorization = signer.authorization(rest_param, token, host, headers)
        self.assertTrue(authorization.startswith("acs AccessKeyId:"))

        # Test identifier
        self.assertEqual("openapi", signer.identifier())

    def test_get_authorization(self):
        """Test exact signature output matches."""
        region = "cn-hangzhou"
        data = '{"name":"database","options":{"a":"b"}}'
        parameters = {"k1": "v1", "k2": "v2"}
        token = DLFToken("access-key-id", "access-key-secret", "securityToken", None)
        now = datetime(2023, 12, 3, 12, 12, 12, tzinfo=timezone.utc)

        signer = DLFDefaultSigner(region)
        sign_headers = signer.sign_headers(data, now, "securityToken", "host")
        rest_param = RESTAuthParameter("POST", "/v1/paimon/databases", data, parameters)
        authorization = signer.authorization(rest_param, token, "host", sign_headers)

        expected = (
            "DLF4-HMAC-SHA256 "
            "Credential=access-key-id/20231203/cn-hangzhou/DlfNext/aliyun_v4_request,"
            "Signature=c72caf1d40b55b1905d891ee3e3de48a2f8bebefa7e39e4f277acc93c269c5e3"
        )
        self.assertEqual(expected, authorization)

    def test_dlf_auth_provider_merge_auth_header(self):
        token = DLFToken("ak", "sk", "security-token", None)
        provider = DLFAuthProvider(
            uri="https://cn-hangzhou-vpc.dlf.aliyuncs.com",
            region="cn-hangzhou",
            signing_algorithm="default",
            token=token
        )

        data = '{"key":"value"}'
        rest_param = RESTAuthParameter("POST", "/path", data, {"k1": "v1"})
        header = provider.merge_auth_header({}, rest_param)

        # Verify Authorization format
        self.assertTrue(header["Authorization"].startswith("DLF4-HMAC-SHA256 Credential="))
        self.assertIn(",Signature=", header["Authorization"])

        # Verify security token
        self.assertEqual("security-token", header.get("x-dlf-security-token"))

        # Verify required headers present
        self.assertIn("x-dlf-date", header)
        self.assertEqual("v1", header.get("x-dlf-version"))
        self.assertEqual("application/json", header.get("Content-Type"))
        self.assertIn("Content-MD5", header)
        self.assertEqual("UNSIGNED-PAYLOAD", header.get("x-dlf-content-sha256"))

    def test_parse_signing_algo_from_uri(self):
        parse = DLFAuthProviderFactory.parse_signing_algo_from_uri

        # dlfnext endpoints -> openapi
        self.assertEqual("openapi", parse("dlfnext.cn-hangzhou.aliyuncs.com"))
        self.assertEqual("openapi", parse("dlfnext-vpc.cn-hangzhou.aliyuncs.com"))
        self.assertEqual("openapi", parse("https://dlfnext.cn-hangzhou.aliyuncs.com"))

        # dlf vpc/intranet endpoints -> default
        self.assertEqual("default", parse("cn-hangzhou-vpc.dlf.aliyuncs.com"))
        self.assertEqual("default", parse("cn-hangzhou-intranet.dlf.aliyuncs.com"))
        self.assertEqual("default", parse("https://cn-hangzhou-vpc.dlf.aliyuncs.com"))

        # unknown/empty -> default
        self.assertEqual("default", parse("unknown.example.com"))
        self.assertEqual("default", parse("127.0.0.1"))
        self.assertEqual("default", parse(""))
        self.assertEqual("default", parse(None))

    def test_openapi_sign_headers_with_enhanced_nonce(self):
        """Test enhanced nonce generation."""
        signer = DLFOpenApiSigner()
        body = '{"CategoryName":"test","CategoryType":"UNSTRUCTURED"}'
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)
        host = "dlfnext.cn-beijing.aliyuncs.com"

        headers = signer.sign_headers(body, now, None, host)

        self.assertIsNotNone(headers.get("Date"))
        self.assertEqual("application/json", headers.get("Accept"))
        self.assertIsNotNone(headers.get("Content-MD5"))
        self.assertEqual("application/json", headers.get("Content-Type"))
        self.assertEqual(host, headers.get("Host"))
        self.assertEqual("HMAC-SHA1", headers.get("x-acs-signature-method"))

        nonce_value = headers.get("x-acs-signature-nonce")
        self.assertIsNotNone(nonce_value)

        uuid_pattern = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
        uuid_match = uuid_pattern.search(nonce_value)
        self.assertIsNotNone(uuid_match, f"No UUID pattern found in nonce: {nonce_value}")

        digit_pattern = re.compile(r'\d+')
        digit_matches = digit_pattern.findall(nonce_value)
        self.assertGreater(len(digit_matches), 0, f"No numeric parts found in nonce: {nonce_value}")

        timestamp_found = any(len(part) >= 10 for part in digit_matches)
        self.assertTrue(timestamp_found, f"No timestamp-like part found in nonce: {nonce_value}")

        self.assertEqual("1.0", headers.get("x-acs-signature-version"))
        self.assertEqual("2026-01-18", headers.get("x-acs-version"))

    def test_concurrent_nonce_generation(self):
        """Test nonce generation thread safety."""
        signer = DLFOpenApiSigner()
        body = '{"test":"data"}'
        now = datetime.now(timezone.utc)
        host = "test-host"
        thread_count = 10
        iterations_per_thread = 50

        nonces = set()

        def worker():
            for _ in range(iterations_per_thread):
                headers = signer.sign_headers(body, now, None, host)
                nonce = headers.get("x-acs-signature-nonce")
                nonces.add(nonce)

        threads = []
        for _ in range(thread_count):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        expected_total = thread_count * iterations_per_thread
        self.assertEqual(expected_total, len(nonces),
                         f"Expected {expected_total} unique nonces, but got {len(nonces)}. "
                         f"Possible duplicate nonces generated.")

    def test_parameter_validation(self):
        """Test parameter validation."""
        signer = DLFOpenApiSigner()
        
        with self.assertRaises(ValueError) as context:
            signer.sign_headers("body", None, "token", "host")
        self.assertIn("'now' cannot be None", str(context.exception))
        
        now = datetime.now(timezone.utc)
        with self.assertRaises(ValueError) as context:
            signer.sign_headers("body", now, "token", None)
        self.assertIn("'host' cannot be None", str(context.exception))
        
        token = DLFToken("ak", "sk", "token", None)
        rest_param = RESTAuthParameter("GET", "/", "", {})
        headers = signer.sign_headers("", now, "", "host")
        
        with self.assertRaises(ValueError) as context:
            signer.authorization(None, token, "host", headers)
        self.assertIn("'rest_auth_parameter' cannot be None", str(context.exception))
        
        with self.assertRaises(ValueError) as context:
            signer.authorization(rest_param, None, "host", headers)
        self.assertIn("'token' cannot be None", str(context.exception))
        
        with self.assertRaises(ValueError) as context:
            signer.authorization(rest_param, token, None, headers)
        self.assertIn("'host' cannot be None", str(context.exception))
        
        with self.assertRaises(ValueError) as context:
            signer.authorization(rest_param, token, "host", None)
        self.assertIn("'sign_headers' cannot be None", str(context.exception))


if __name__ == '__main__':
    unittest.main()
