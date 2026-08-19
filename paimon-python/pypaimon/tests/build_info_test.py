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

import os
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from pypaimon import build_info


class BuildInfoTest(unittest.TestCase):

    def test_full_version(self):
        self.assertRegex(
            build_info.full_version(),
            r"^2\.1\.dev-(UNKNOWN|[0-9a-f]{40})$",
        )

    def test_embedded_full_version(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as version_file:
            version_file.write(
                "2.1.dev-0123456789012345678901234567890123456789\n")
            path = version_file.name
        try:
            with patch.object(build_info, "_FULL_VERSION_FILE", path):
                self.assertEqual(
                    "2.1.dev-0123456789012345678901234567890123456789",
                    build_info._load_full_version(),
                )
        finally:
            os.remove(path)

    @unittest.skipIf(shutil.which("git") is None, "Git is not available")
    def test_does_not_discover_outer_repository(self):
        with tempfile.TemporaryDirectory() as tmp:
            outer = os.path.join(tmp, "outer")
            source = os.path.join(outer, "source")
            os.makedirs(source)
            subprocess.check_call(
                ["git", "init", "-q", outer], stdout=subprocess.DEVNULL)
            subprocess.check_call(
                ["git", "-C", outer, "config", "user.name", "test"])
            subprocess.check_call(
                ["git", "-C", outer, "config", "user.email", "test@example.com"])
            subprocess.check_call(
                ["git", "-C", outer, "commit", "-q", "--allow-empty", "-m", "outer"])

            fake_module = os.path.join(source, "pypaimon", "build_info.py")
            with patch.object(build_info, "__file__", fake_module):
                self.assertEqual("UNKNOWN", build_info.git_commit_id())


if __name__ == "__main__":
    unittest.main()
