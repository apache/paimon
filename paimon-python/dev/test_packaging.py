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

"""Exercise actual distributions without importing runtime/native dependencies.

Run with ``python dev/test_packaging.py`` after installing the build tools.
The same suite runs on Python 3.6 and modern Python in CI.
"""
import email
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile

from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

try:
    import tomllib
except ImportError:
    import tomli as tomllib


def command(args, cwd, env=None):
    result = subprocess.run(args, cwd=str(cwd), env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise AssertionError(result.stdout.decode("utf-8", errors="replace")[-8000:])
    return result.stdout.decode("utf-8")


def wheel_contents(path):
    with zipfile.ZipFile(str(path)) as archive:
        names = archive.namelist()
        metadata = email.message_from_bytes(archive.read(next(
            name for name in names if name.endswith(".dist-info/METADATA"))))
        full_version = archive.read("pypaimon/_full_version").decode().strip()
    return names, metadata, full_version


class PackagingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="pypaimon-packaging-")
        cls.root = Path(cls.temp.name)
        cls.source = cls.root / "source"
        project = Path(__file__).resolve().parent.parent
        shutil.copytree(str(project), str(cls.source), ignore=shutil.ignore_patterns(
            "build", "dist", "*.egg-info", "__pycache__", ".pytest_cache", ".venv"))
        cls.env = os.environ.copy()
        for key in ("GIT_DIR", "GIT_WORK_TREE", "PYPAIMON_BUILD_VERSION"):
            cls.env.pop(key, None)
        cls.env.update(GIT_AUTHOR_DATE="2020-01-02T03:04:05+00:00",
                       GIT_COMMITTER_DATE="2020-01-02T03:04:05+00:00")
        cls.commit = cls.init_git(cls.source)
        base = runpy.run_path(str(cls.source / "pypaimon/_version.py"))["VERSION"]
        cls.version = str(Version(base + "20200102" if base.endswith(".dev") else base))
        with (cls.source / "pyproject.toml").open("rb") as config:
            cls.config = tomllib.load(config)
        cls.dist = cls.root / "custom-output"
        cls.build(cls.source, cls.dist)
        cls.wheel = next(cls.dist.glob("*.whl"))
        cls.sdist = next(cls.dist.glob("*.tar.gz"))

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    @classmethod
    def init_git(cls, root):
        for args in (["init", "-q"], ["config", "user.name", "Packaging Test"],
                     ["config", "user.email", "test@example.com"], ["add", "."],
                     ["commit", "-qm", "test source"]):
            command(["git"] + args, root, cls.env)
        return command(["git", "rev-parse", "HEAD"], root, cls.env).strip()

    @classmethod
    def build(cls, source, output, extra=(), env=None):
        command([sys.executable, "-m", "build", "--no-isolation", "--outdir",
                 str(output)] + list(extra), source, env or cls.env)

    def test_sdist_and_wheel_share_version_and_commit(self):
        self.assertEqual(2, len(list(self.dist.iterdir())))
        self.assertEqual("pypaimon-{}.tar.gz".format(self.version), self.sdist.name)
        self.assertEqual("pypaimon-{}-py3-none-any.whl".format(self.version), self.wheel.name)
        _, metadata, full_version = wheel_contents(self.wheel)
        self.assertEqual(self.version, metadata["Version"])
        self.assertEqual("python-{}-{}".format(self.version, self.commit), full_version)
        with tarfile.open(str(self.sdist)) as archive:
            prefix = "pypaimon-{}/".format(self.version)
            self.assertEqual(full_version, archive.extractfile(
                prefix + "pypaimon/_full_version").read().decode().strip())

    def test_distribution_contents(self):
        names, metadata, _ = wheel_contents(self.wheel)
        self.assertEqual(">=3.6", metadata["Requires-Python"])
        self.assertFalse(any(name.startswith(("pypaimon/tests/", "pypaimon/acceptance/"))
                             for name in names))
        self.assertFalse(any(name.endswith((".so", ".pyd", ".jar")) for name in names))
        for name in ("pypaimon/sample/data/data.jsonl", "pypaimon/_version.py",
                     "pypaimon/benchmark/act/default_experiment.json"):
            self.assertIn(name, names)
        for suffix in ("/LICENSE", "/NOTICE"):
            self.assertTrue(any(".dist-info/" in name and name.endswith(suffix)
                                for name in names))
        with tarfile.open(str(self.sdist)) as archive:
            names = archive.getnames()
            prefix = "pypaimon-{}/".format(self.version)
            for name in ("pyproject.toml", "setup.py", "LICENSE", "NOTICE", "README.md"):
                self.assertIn(prefix + name, names)
            self.assertFalse(any("requirements.txt" in name or "requirements-dev.txt" in name
                                 or name.startswith((prefix + "pypaimon/tests/",
                                                     prefix + "pypaimon/acceptance/"))
                                 for name in names))

    @staticmethod
    def active_requirements(values, environment):
        result = set()
        for value in values:
            requirement = Requirement(value)
            if requirement.marker is None or requirement.marker.evaluate(environment):
                result.add((canonicalize_name(requirement.name), str(requirement.specifier)))
        return result

    def test_metadata_preserves_core_and_extra_conditions(self):
        _, metadata, _ = wheel_contents(self.wheel)
        project = self.config["project"]
        self.assertEqual(set(project["optional-dependencies"]),
                         set(metadata.get_all("Provides-Extra")))
        for python_version in ("3.6", "3.7", "3.8", "3.11", "3.13"):
            for system in ("Linux", "Windows"):
                for extra in [""] + list(project["optional-dependencies"]):
                    with self.subTest(python=python_version, system=system, extra=extra):
                        environment = default_environment()
                        environment.update(python_version=python_version,
                                           python_full_version=python_version + ".0",
                                           platform_system=system, extra=extra)
                        expected = project["dependencies"] + project[
                            "optional-dependencies"].get(extra, [])
                        self.assertEqual(self.active_requirements(expected, environment),
                                         self.active_requirements(
                                             metadata.get_all("Requires-Dist"), environment))

    def test_dev_group_reuses_product_extras(self):
        resolve = runpy.run_path(str(self.source / "dev/install.py"))["resolve_group"]
        requirements = [Requirement(value) for value in resolve(
            self.config["dependency-groups"], "dev")]
        project = next(requirement for requirement in requirements if requirement.name == "pypaimon")
        self.assertTrue({"ray", "sql", "vindex", "full-text"}.issubset(project.extras))
        # Product constraints belong to extras, not a second developer copy.
        names = {requirement.name for requirement in requirements}
        self.assertFalse(names.intersection({"datafusion", "paimon-vindex", "requests", "h5py"}))

    def test_sdist_rebuild_preserves_provenance_in_another_git_repo(self):
        outer = self.root / "downstream"
        outer.mkdir()
        (outer / "README").write_text("Unrelated downstream repository")
        downstream_commit = self.init_git(outer)
        self.assertNotEqual(self.commit, downstream_commit)
        with tarfile.open(str(self.sdist)) as archive:
            # This archive was just built from our test source.
            archive.extractall(str(outer))
        output = self.root / "rebuilt"
        self.build(outer / "pypaimon-{}".format(self.version), output, extra=("--wheel",))
        _, metadata, full_version = wheel_contents(next(output.glob("*.whl")))
        self.assertEqual(self.version, metadata["Version"])
        self.assertEqual("python-{}-{}".format(self.version, self.commit), full_version)

    def test_release_override_applies_to_both_formats(self):
        env = dict(self.env, PYPAIMON_BUILD_VERSION="2.2rc1")
        output = self.root / "release-candidate"
        self.build(self.source, output, env=env)
        self.assertEqual({"pypaimon-2.2rc1.tar.gz", "pypaimon-2.2rc1-py3-none-any.whl"},
                         {path.name for path in output.iterdir()})
        _, metadata, full_version = wheel_contents(next(output.glob("*.whl")))
        self.assertEqual("2.2rc1", metadata["Version"])
        self.assertEqual("python-2.2rc1-{}".format(self.commit), full_version)
        self.assertEqual("", command(["git", "diff", "--", "pypaimon/_version.py"],
                                     self.source, self.env))

    def test_invalid_release_override_fails(self):
        env = dict(self.env, PYPAIMON_BUILD_VERSION="not a version")
        result = subprocess.run([sys.executable, "-m", "build", "--no-isolation", "--sdist"],
                                cwd=str(self.source), env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.assertNotEqual(0, result.returncode)


if __name__ == "__main__":
    unittest.main(verbosity=2)
