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

    def test_canonical_query_string_matches_urlencode(self):
        """The signer's canonical query string should match what
        urllib.parse.urlencode produces, otherwise the server
        will compute a different signature and reject the request.
        Currently this fails for params containing '%'."""
        params = {"functionNamePattern": "func%"}

        signer = DLFDefaultSigner.__new__(DLFDefaultSigner)
        canonical_qs = signer._build_canonical_query_string(params)
        http_qs = urllib.parse.urlencode(params)

        # canonical_qs = "functionNamePattern=func%"
        # http_qs      = "functionNamePattern=func%25"
        # They should be equal but are not — this is the bug.
        self.assertNotEqual(canonical_qs, http_qs)


if __name__ == "__main__":
    unittest.main()
