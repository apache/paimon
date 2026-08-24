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
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from pypaimon import build_info


class BuildInfoTest(unittest.TestCase):

    def test_full_version(self):
        self.assertRegex(
            build_info.full_version(),
            r"^python-2\.1\.dev-(UNKNOWN|[0-9a-f]{40})$",
        )

    def test_embedded_full_version(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as version_file:
            version_file.write(
                "python-2.1.dev-0123456789012345678901234567890123456789\n")
            path = version_file.name
        try:
            with patch.object(build_info, "_FULL_VERSION_FILE", path):
                self.assertEqual(
                    "python-2.1.dev-0123456789012345678901234567890123456789",
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

    @unittest.skipIf(shutil.which("git") is None, "Git is not available")
    def test_sdist_provenance_survives_downstream_git_repository(self):
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))))
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "paimon-python")
            shutil.copytree(
                project_root,
                source,
                ignore=shutil.ignore_patterns(
                    ".pytest_cache", "*.egg-info", "__pycache__", "build", "dist"),
            )
            upstream_commit = self._init_git_repository(source, "upstream")

            sdist_dir = os.path.join(tmp, "sdist")
            os.makedirs(sdist_dir)
            subprocess.check_call(
                [sys.executable, "setup.py", "-q", "sdist", "--dist-dir", sdist_dir],
                cwd=source,
            )
            archives = [
                os.path.join(sdist_dir, name)
                for name in os.listdir(sdist_dir)
                if name.endswith(".tar.gz")
            ]
            self.assertEqual(1, len(archives))

            extracted_root = os.path.join(tmp, "extracted")
            os.makedirs(extracted_root)
            with tarfile.open(archives[0], "r:gz") as archive:
                top_level = archive.getnames()[0].split("/")[0]
                archive.extractall(extracted_root)
            extracted = os.path.join(extracted_root, top_level)
            embedded_file = os.path.join(extracted, "pypaimon", "_full_version")
            with open(embedded_file, "r") as full_version_file:
                embedded = full_version_file.read().strip()
            self.assertEqual("python-2.1.dev-" + upstream_commit, embedded)

            downstream_commit = self._init_git_repository(extracted, "downstream")
            self.assertNotEqual(upstream_commit, downstream_commit)
            build_lib = os.path.join(tmp, "build")
            subprocess.check_call(
                [sys.executable, "setup.py", "-q", "build_py", "--build-lib", build_lib],
                cwd=extracted,
            )
            with open(
                    os.path.join(build_lib, "pypaimon", "_full_version"),
                    "r",
            ) as full_version_file:
                self.assertEqual(embedded, full_version_file.read().strip())

    @staticmethod
    def _init_git_repository(path, message):
        subprocess.check_call(["git", "init", "-q", path])
        subprocess.check_call(["git", "-C", path, "config", "user.name", "test"])
        subprocess.check_call(
            ["git", "-C", path, "config", "user.email", "test@example.com"])
        subprocess.check_call(["git", "-C", path, "add", "."])
        subprocess.check_call(
            ["git", "-C", path, "commit", "-q", "-m", message])
        return subprocess.check_output(
            ["git", "-C", path, "rev-parse", "HEAD"]
        ).decode("utf-8").strip()


if __name__ == "__main__":
    unittest.main()
