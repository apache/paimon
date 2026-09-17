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

import unittest
import hashlib
import locale
import re
import threading
from datetime import datetime, timedelta, timezone

from pypaimon.api.auth import (
    DLFAuthProvider,
    DLFAuthProviderFactory,
    DLFDefaultSigner,
    DLFOpenApiSigner,
    DLFOpenApiV4Signer,
)
from pypaimon.api.auth.dlf_openapi_actions import OPERATIONS, resolve_action
from pypaimon.api.resource_paths import ResourcePaths
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

    def test_openapi_date_format_with_chinese_locale(self):
        """Date header must stay English RFC 1123 under zh_CN locale."""
        original = locale.setlocale(locale.LC_TIME, None)
        try:
            self._set_lc_time_or_skip(["zh_CN.UTF-8", "zh_CN.utf8", "zh_CN"])
            self._assert_openapi_date_in_english()
        finally:
            locale.setlocale(locale.LC_TIME, original)

    def test_openapi_date_format_with_japanese_locale(self):
        """Date header must stay English RFC 1123 under ja_JP locale."""
        original = locale.setlocale(locale.LC_TIME, None)
        try:
            self._set_lc_time_or_skip(["ja_JP.UTF-8", "ja_JP.utf8", "ja_JP"])
            self._assert_openapi_date_in_english()
        finally:
            locale.setlocale(locale.LC_TIME, original)

    def _set_lc_time_or_skip(self, candidates):
        for name in candidates:
            try:
                locale.setlocale(locale.LC_TIME, name)
                return
            except locale.Error:
                continue
        self.skipTest(f"None of the locales {candidates} is available on this system")

    def _assert_openapi_date_in_english(self):
        signer = DLFOpenApiSigner()
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)
        headers = signer.sign_headers(None, now, None, "dlfnext.cn-hangzhou.aliyuncs.com")
        self.assertEqual("Wed, 16 Apr 2025 03:44:46 GMT", headers.get("Date"))

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

    def test_rest_auth_parameter_encodes_values(self):
        param = RESTAuthParameter(
            method="GET", path="/test",
            data="", parameters={"functionNamePattern": "func%"})
        self.assertEqual(
            param.parameters["functionNamePattern"], "func%25")

    def test_parse_signing_algo_from_uri(self):
        parse = DLFAuthProviderFactory.parse_signing_algo_from_uri

        # dlfnext endpoints -> openapi (V4 is opt-in, never auto-selected)
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

    def test_openapi_v4_sign_headers_with_body(self):
        signer = DLFOpenApiV4Signer("cn-hangzhou")
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)
        host = "dlfnext.cn-beijing.aliyuncs.com"
        body = '{"CategoryName":"test","CategoryType":"UNSTRUCTURED"}'

        headers = signer.sign_headers(body, now, "security-token", host)

        self.assertEqual("2025-04-16T03:44:46Z", headers.get("x-acs-date"))
        self.assertEqual(host, headers.get("host"))
        self.assertNotEqual(
            DLFOpenApiV4Signer.EMPTY_BODY_SHA256, headers.get("x-acs-content-sha256"))
        self.assertEqual("application/json", headers.get("content-type"))
        self.assertIn("x-acs-signature-nonce", headers)
        self.assertEqual("2026-01-18", headers.get("x-acs-version"))
        self.assertEqual("security-token", headers.get("x-acs-security-token"))

        # The V1 ROA headers belong to DLFOpenApiSigner, not to this signer
        for absent in ("Date", "Accept", "Content-MD5",
                       "x-acs-signature-method", "x-acs-signature-version"):
            self.assertNotIn(absent, headers)

    def test_openapi_v4_sign_headers_without_body(self):
        signer = DLFOpenApiV4Signer("cn-hangzhou")
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)

        headers = signer.sign_headers(None, now, None, "dlfnext.cn-beijing.aliyuncs.com")

        self.assertEqual(
            DLFOpenApiV4Signer.EMPTY_BODY_SHA256, headers.get("x-acs-content-sha256"))
        self.assertNotIn("content-type", headers)

    def test_openapi_v4_date_is_rendered_in_utc(self):
        """x-acs-date stays UTC even when 'now' carries a non-UTC offset."""
        signer = DLFOpenApiV4Signer("cn-hangzhou")
        beijing = datetime(2025, 4, 16, 11, 44, 46, tzinfo=timezone(timedelta(hours=8)))

        headers = signer.sign_headers(None, beijing, None, "dlfnext.cn-hangzhou.aliyuncs.com")

        self.assertEqual("2025-04-16T03:44:46Z", headers.get("x-acs-date"))

    def test_openapi_v4_authorization_format(self):
        signer = DLFOpenApiV4Signer("cn-hangzhou")
        token = DLFToken("TestAKId", "TestAKSecret", "security-token", None)
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)
        host = "dlfnext.cn-beijing.aliyuncs.com"
        body = '{"CategoryName":"test"}'

        headers = signer.sign_headers(body, now, token.security_token, host)
        headers["x-acs-signature-nonce"] = "fixed-nonce"

        rest_param = RESTAuthParameter("POST", "/api/test", body, {})
        authorization = signer.authorization(rest_param, token, host, headers)

        self.assertTrue(authorization.startswith("ACS4-HMAC-SHA256 Credential="))
        self.assertIn("Credential=TestAKId/20250416/cn-hangzhou/DlfNext/aliyun_v4_request,", authorization)
        self.assertIn("SignedHeaders=", authorization)

        signature = authorization.split("Signature=")[1]
        self.assertEqual(64, len(signature))
        self.assertRegex(signature, r'^[0-9a-f]+$')

    def test_openapi_v4_signature_is_deterministic_and_covers_query(self):
        signer = DLFOpenApiV4Signer("cn-hangzhou")
        token = DLFToken("TestAKId", "TestAKSecret", None, None)
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)
        host = "dlfnext.cn-beijing.aliyuncs.com"

        headers = signer.sign_headers(None, now, None, host)
        headers["x-acs-signature-nonce"] = "fixed-nonce"

        without_query = RESTAuthParameter("GET", "/test/path", None, {})
        with_query = RESTAuthParameter("GET", "/test/path", None, {"k2": "v2", "k1": "v1"})

        auth1 = signer.authorization(without_query, token, host, headers)
        auth2 = signer.authorization(without_query, token, host, headers)
        self.assertEqual(auth1, auth2)

        self.assertNotEqual(
            auth1, signer.authorization(with_query, token, host, headers))

    def test_openapi_v4_signed_headers_scope(self):
        signer = DLFOpenApiV4Signer("cn-hangzhou")
        token = DLFToken("TestAKId", "TestAKSecret", None, None)
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)
        host = "dlfnext.cn-beijing.aliyuncs.com"
        body = '{"test":"data"}'

        headers = signer.sign_headers(body, now, None, host)
        headers["x-acs-signature-nonce"] = "fixed-nonce"

        rest_param = RESTAuthParameter("POST", "/test/path", body, {})
        authorization = signer.authorization(rest_param, token, host, headers)

        signed_headers = authorization.split("SignedHeaders=")[1].split(",Signature=")[0]
        self.assertIn("host", signed_headers)
        self.assertIn("content-type", signed_headers)
        self.assertIn("x-acs-content-sha256", signed_headers)
        self.assertIn("x-acs-date", signed_headers)

    def test_openapi_v4_identifier(self):
        self.assertEqual("openapi-v4", DLFOpenApiV4Signer("cn-hangzhou").identifier())

    def test_openapi_v4_parameter_validation(self):
        signer = DLFOpenApiV4Signer("cn-hangzhou")
        now = datetime.now(timezone.utc)
        token = DLFToken("ak", "sk", "token", None)
        rest_param = RESTAuthParameter("GET", "/", "", {})
        headers = signer.sign_headers("", now, "", "host")

        with self.assertRaises(ValueError) as context:
            signer.sign_headers("body", None, "token", "host")
        self.assertIn("'now' cannot be None", str(context.exception))

        with self.assertRaises(ValueError) as context:
            signer.sign_headers("body", now, "token", None)
        self.assertIn("'host' cannot be None", str(context.exception))

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

    def test_auth_provider_signs_with_acs4_when_configured(self):
        uri = "https://dlfnext.cn-hangzhou.aliyuncs.com"
        provider = DLFAuthProvider(
            uri=uri,
            region="cn-hangzhou",
            signing_algorithm=DLFOpenApiV4Signer.IDENTIFIER,
            token=DLFToken("akId", "akSecret", None, None)
        )

        header = provider.merge_auth_header(
            {}, RESTAuthParameter("GET", "/test/path", "", {}))

        self.assertTrue(
            header["Authorization"].startswith("ACS4-HMAC-SHA256 Credential=akId/"))
        self.assertIn("x-acs-date", header)

    def test_openapi_v4_known_signature_for_encoded_query_values(self):
        """RESTAuthParameter already encoded these values once; encoding them again would sign a
        canonical form the gateway cannot rebuild. Keys are inserted out of order, so dropping the
        sort breaks this too. The Java signer pins the same string."""
        signer = DLFOpenApiV4Signer("cn-hangzhou")
        token = DLFToken("TestAKId", "TestAKSecret", None, None)
        host = "dlfnext.cn-hangzhou.aliyuncs.com"
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)

        headers = signer.sign_headers(None, now, None, host)
        headers["x-acs-signature-nonce"] = "fixed-nonce-for-test"

        rest_param = RESTAuthParameter(
            "GET", "/v1/prefix/functions", None,
            {"pageToken": "a b/c", "functionNamePattern": "func%"})

        self.assertEqual(
            "ACS4-HMAC-SHA256 Credential=TestAKId/20250416/cn-hangzhou/DlfNext/aliyun_v4_request,"
            "SignedHeaders=host;x-acs-content-sha256;x-acs-date;x-acs-signature-nonce;"
            "x-acs-version,"
            "Signature=9812bacd92dd1152e7ea32aacfad0545835d6a18543852364f1a1ea0cba6219a",
            signer.authorization(rest_param, token, host, headers))

    def test_openapi_v4_percent_encode(self):
        """Mirrors the Java testPercentEncode, so the two cannot drift on escaping."""
        encode = DLFOpenApiV4Signer._percent_encode
        self.assertEqual("hello%20world", encode("hello world"))
        self.assertEqual("a%2Ab", encode("a*b"))
        self.assertEqual("a~b", encode("a~b"))
        self.assertEqual("a%2Fb", encode("a/b"))

    def test_openapi_v4_empty_body_hash_is_sha256_of_empty_string(self):
        """Derived independently, so a typo in the constant cannot pass."""
        self.assertEqual(
            hashlib.sha256(b"").hexdigest(), DLFOpenApiV4Signer.EMPTY_BODY_SHA256)

    def test_openapi_v4_every_operation_resolves_to_itself(self):
        """Each template, filled in, must come back to its own action rather than an earlier one."""
        actions = set()
        for method, template, action in OPERATIONS:
            path = re.sub(r"\{[^}]+\}", "x1", template)
            self.assertEqual(action, resolve_action(method, path), path)
            actions.add(action)
        self.assertEqual(53, len(actions))

    def test_openapi_v4_client_paths_resolve_to_registered_actions(self):
        paths = ResourcePaths("clg-paimon-1")
        cases = [
            ("GetConfig", "GET", ResourcePaths.config()),
            ("ListDatabases", "GET", paths.databases()),
            ("CreateDatabase", "POST", paths.databases()),
            ("GetDatabase", "GET", paths.database("db")),
            ("DropDatabase", "DELETE", paths.database("db")),
            ("ListTables", "GET", paths.tables("db")),
            ("CreateTable", "POST", paths.tables("db")),
            ("ListTableDetails", "GET", paths.table_details("db")),
            ("GetTable", "GET", paths.table("db", "t")),
            ("AlterTable", "POST", paths.table("db", "t")),
            ("DropTable", "DELETE", paths.table("db", "t")),
            ("RenameTable", "POST", paths.rename_table()),
            ("GetTableToken", "GET", paths.table_token("db", "t")),
            ("CommitTable", "POST", paths.commit_table("db", "t")),
            ("RollbackToSnapshot", "POST", paths.rollback_table("db", "t")),
            ("GetTableSnapshot", "GET", paths.table_snapshot("db", "t")),
            ("ListPartitions", "GET", paths.partitions("db", "t")),
            ("ListFunctions", "GET", paths.functions("db")),
            ("GetFunction", "GET", paths.function("db", "f")),
            ("ListTags", "GET", paths.tags("db", "t")),
            ("GetTag", "GET", paths.tag("db", "t", "tag")),
            ("ListBranches", "GET", paths.branches("db", "t")),
            ("DropBranch", "DELETE", paths.branch("db", "t", "b")),
            ("FastForwardBranch", "POST", paths.forward_branch("db", "t", "b")),
            ("AuthTableQuery", "POST", paths.auth_table("db", "t")),
        ]
        for action, method, path in cases:
            self.assertEqual(action, resolve_action(method, path), method + " " + path)

    def test_openapi_v4_names_spelled_like_literals(self):
        """Names match whole segments, so one spelled like a literal still resolves."""
        paths = ResourcePaths("clg-paimon-1")
        self.assertEqual("GetDatabase", resolve_action("GET", paths.database("tables")))
        self.assertEqual("GetTable", resolve_action("GET", paths.table("db", "token")))
        self.assertEqual("DropBranch", resolve_action("DELETE", paths.branch("db", "t", "forward")))
        self.assertEqual("GetTableToken", resolve_action("GET", paths.table_token("config", "t$snapshots")))
        self.assertEqual("GetTable", resolve_action("GET", "/v1/clg-paimon-1/databases/a%2Fb/tables/c%20d"))
        self.assertEqual("ListDatabases", resolve_action("get", ResourcePaths("rename").databases()))

    def test_openapi_v4_unregistered_requests_have_no_action(self):
        paths = ResourcePaths("clg-paimon-1")
        self.assertIsNone(resolve_action("GET", paths.tables()))
        self.assertIsNone(resolve_action("POST", paths.rename_branch("db", "t", "b")))
        self.assertIsNone(resolve_action("PUT", paths.table("db", "t")))
        self.assertIsNone(resolve_action("GET", ResourcePaths.config() + "/"))
        self.assertIsNone(resolve_action("GET", "/v1//databases"))
        self.assertIsNone(resolve_action(None, ResourcePaths.config()))
        self.assertIsNone(resolve_action("GET", None))

    def test_openapi_v4_known_signature_with_action(self):
        """Known answer with a body and a resolved action. The Java signer pins the same string."""
        signer = DLFOpenApiV4Signer("cn-hangzhou")
        token = DLFToken("TestAKId", "TestAKSecret", None, None)
        host = "dlfnext.cn-hangzhou.aliyuncs.com"
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)
        body = '{"identifier":{"database":"db","object":"t"}}'
        rest_param = RESTAuthParameter("POST", "/v1/clg-paimon-1/databases/db/tables", body, {})

        headers = signer.sign_request_headers(rest_param, now, None, host)
        headers["x-acs-signature-nonce"] = "fixed-nonce-for-test"

        self.assertEqual("CreateTable", headers["x-acs-action"])
        self.assertEqual(
            "ACS4-HMAC-SHA256 Credential=TestAKId/20250416/cn-hangzhou/DlfNext/aliyun_v4_request,"
            "SignedHeaders=content-type;host;x-acs-action;x-acs-content-sha256;"
            "x-acs-date;x-acs-signature-nonce;x-acs-version,"
            "Signature=420206b5263536e6bc271a7a32582b4a820e23c8c58ae692b708c69f9d19e51d",
            signer.authorization(rest_param, token, host, headers))

    def test_openapi_v4_omits_action_for_unregistered_path(self):
        signer = DLFOpenApiV4Signer("cn-hangzhou")
        host = "dlfnext.cn-hangzhou.aliyuncs.com"
        now = datetime(2025, 4, 16, 3, 44, 46, tzinfo=timezone.utc)
        rest_param = RESTAuthParameter("GET", "/v1/clg-paimon-1/tables", "", {})

        headers = signer.sign_request_headers(rest_param, now, None, host)

        self.assertNotIn("x-acs-action", headers)
        self.assertEqual(set(signer.sign_headers(None, now, None, host)), set(headers))

    def test_auth_provider_sends_signed_action(self):
        provider = DLFAuthProvider(
            uri="https://dlfnext.cn-hangzhou.aliyuncs.com",
            region="cn-hangzhou",
            signing_algorithm=DLFOpenApiV4Signer.IDENTIFIER,
            token=DLFToken("akId", "akSecret", "securityToken", None)
        )

        header = provider.merge_auth_header(
            {}, RESTAuthParameter("GET", "/v1/clg-paimon-1/databases/db/tables/t/token", "", {}))

        self.assertEqual("GetTableToken", header["x-acs-action"])
        self.assertIn(
            "SignedHeaders=host;x-acs-action;x-acs-content-sha256;x-acs-date;"
            "x-acs-security-token;x-acs-signature-nonce;x-acs-version,",
            header["Authorization"])

    def test_only_openapi_v4_sends_action(self):
        """Only the ACS4 signer resolves an action; the other schemes send what they always did."""
        rest_param = RESTAuthParameter("GET", "/v1/clg-paimon-1/databases/db/tables/t/token", "", {})
        for algorithm in (DLFDefaultSigner.IDENTIFIER, DLFOpenApiSigner.IDENTIFIER):
            provider = DLFAuthProvider(
                uri="https://dlfnext.cn-hangzhou.aliyuncs.com",
                region="cn-hangzhou",
                signing_algorithm=algorithm,
                token=DLFToken("akId", "akSecret", None, None)
            )
            self.assertNotIn("x-acs-action", provider.merge_auth_header({}, rest_param), algorithm)


if __name__ == '__main__':
    unittest.main()
