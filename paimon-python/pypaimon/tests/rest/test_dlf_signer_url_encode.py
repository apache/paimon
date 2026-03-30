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

import unittest
import urllib.parse

from pypaimon.api.auth.dlf_signer import DLFDefaultSigner


class TestDLFSignerUrlEncode(unittest.TestCase):
    """Reproduce: DLF signer and HTTP client encode query params
    differently, causing signature mismatch when params contain
    special characters like '%'.

    The signer computes the canonical query string from raw values
    (e.g. "func%"), but the HTTP client sends the request with
    urllib.parse.urlencode which encodes '%' as '%25'
    (e.g. "func%25"). The server computes the signature from
    what it receives, so the signatures never match.
    """

    def test_percent_in_query_param_causes_signature_mismatch(self):
        """Query param with '%' produces different strings in
        signer vs HTTP request, which causes signature mismatch
        on the server side."""
        params = {"functionNamePattern": "func%"}

        # What the signer uses to compute signature
        signer = DLFDefaultSigner.__new__(DLFDefaultSigner)
        canonical_qs = signer._build_canonical_query_string(params)

        # What the HTTP client actually sends in the request
        http_qs = urllib.parse.urlencode(params)

        # These two SHOULD be the same for signature to match,
        # but they are NOT — this is the bug.
        self.assertNotEqual(
            canonical_qs, http_qs,
            "Bug is fixed if this fails — signer and HTTP client "
            "now produce the same query string"
        )

        # Show the actual difference:
        # canonical_qs = "functionNamePattern=func%"
        # http_qs      = "functionNamePattern=func%25"
        self.assertEqual(canonical_qs, "functionNamePattern=func%")
        self.assertEqual(http_qs, "functionNamePattern=func%25")

    def test_normal_query_param_no_mismatch(self):
        """Query params without special chars work fine."""
        params = {"maxResults": "10", "pageToken": "abc"}

        signer = DLFDefaultSigner.__new__(DLFDefaultSigner)
        canonical_qs = signer._build_canonical_query_string(params)
        http_qs = urllib.parse.urlencode(params)

        # For normal params, both produce the same string
        self.assertEqual(canonical_qs, http_qs)

    def test_space_in_query_param_causes_signature_mismatch(self):
        """Space in query param also causes mismatch."""
        params = {"pattern": "hello world"}

        signer = DLFDefaultSigner.__new__(DLFDefaultSigner)
        canonical_qs = signer._build_canonical_query_string(params)
        http_qs = urllib.parse.urlencode(params)

        # Signer: "pattern=hello world"
        # HTTP:   "pattern=hello+world"
        self.assertNotEqual(canonical_qs, http_qs)


if __name__ == "__main__":
    unittest.main()
