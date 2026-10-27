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
        self.assertFalse(any(name.startswith("pypaimon/tests/")
                             for name in names))
        self.assertFalse(any(name.endswith((".so", ".pyd", ".jar")) for name in names))
        self.assertIn("pypaimon/_version.py", names)
        execution_files = (
            "pypaimon/common/transform_schema.py",
            "pypaimon/filesystem/vortex_utils.py",
            "pypaimon/execution/python/write/format_file_writer.py",
            "pypaimon/table/source/search_execution.py",
            "pypaimon/ray/search_execution.py",
            "pypaimon/execution/native/read/search_execution.py",
            "pypaimon/execution/python/read/search/distributed_search.py",
            "pypaimon/execution/python/read/search/distributed_vector_search.py",
            "pypaimon/execution/python/read/search/distributed_full_text_search.py",
            "pypaimon/execution/python/map_blob_key_serializer.py",
            "pypaimon/execution/python/globalindex/ray_index_build.py",
            "pypaimon/common/external_path_provider.py",
            "pypaimon/common/arrow_predicate.py",
            "pypaimon/execution/python/read/blob_view_lookup.py",
            "pypaimon/execution/python/read/interval_partition.py",
            "pypaimon/execution/python/read/map_shared_shredding.py",
            "pypaimon/execution/python/read/variant_shredding.py",
            "pypaimon/execution/python/write/dynamic_bucket.py",
            "pypaimon/execution/python/write/merge_transform.py",
            "pypaimon/schema/data_type_casts.py",
            "pypaimon/utils/numeric_utils.py",
            "pypaimon/utils/temporal_utils.py",
            "pypaimon/utils/temporal_string.py",
            "pypaimon/utils/row_to_string.py",
            "pypaimon/execution/python/casting/__init__.py",
            "pypaimon/execution/python/casting/schema_evolution.py",
            "pypaimon/execution/python/casting/decimal_cast.py",
            "pypaimon/execution/python/casting/temporal_cast.py",
            "pypaimon/execution/python/casting/temporal_string.py",
            "pypaimon/execution/python/write/partition_spec.py",
            "pypaimon/execution/python/deletionvectors/__init__.py",
            "pypaimon/execution/python/deletionvectors/deletion_vector.py",
            "pypaimon/execution/python/deletionvectors/bitmap_deletion_vector.py",
            "pypaimon/execution/python/deletionvectors/apply_deletion_vector_reader.py",
            "pypaimon/write/backend_dispatch.py",
            "pypaimon/read/read_dispatch.py",
            "pypaimon/read/read_support.py",
            "pypaimon/read/read_explain.py",
            "pypaimon/globalindex/index_dispatch.py",
            "pypaimon/table/source/search_dispatch.py",
            "pypaimon/write/update_dispatch.py",
            "pypaimon/write/write_dispatch.py",
            "pypaimon/globalindex/vindex_input.py",
            "pypaimon/write/input_adapter.py",
            "pypaimon/write/merge_condition.py",
            "pypaimon/write/merge_transform.py",
            "pypaimon/read/datasource/torch_dataset.py",
            "pypaimon/read/datasource/ray_datasource.py",
            "pypaimon/read/datasource/split_provider.py",
            "pypaimon/write/ray_datasink.py",
            "pypaimon/execution/python/read/table_read.py",
            "pypaimon/execution/python/read/table_scan.py",
            "pypaimon/execution/python/read/auth_support.py",
            "pypaimon/execution/python/read/scan_distribution.py",
            "pypaimon/execution/python/read/format_table_read.py",
            "pypaimon/execution/python/read/format_table_scan.py",
            "pypaimon/execution/python/read/object_table_read.py",
            "pypaimon/execution/python/read/object_table_scan.py",
            "pypaimon/table/system/system_table_read.py",
            "pypaimon/table/system/system_table_scan.py",
            "pypaimon/execution/python/merge_engine_dispatch.py",
            "pypaimon/execution/python/read/search/vector_search_read.py",
            "pypaimon/execution/python/read/search/factories.py",
            "pypaimon/execution/python/read/search/hybrid_search.py",
            "pypaimon/ray/vector_index_build.py",
            "pypaimon/execution/python/globalindex/btree/btree_index_reader.py",
            "pypaimon/execution/python/globalindex/btree/block_handle.py",
            "pypaimon/execution/python/globalindex/block_compression.py",
            "pypaimon/execution/python/globalindex/key_serializer.py",
            "pypaimon/execution/python/index/pk/primary_key_index_source_policy.py",
            "pypaimon/execution/python/index/pksorted/pk_sorted_bucket_index_state.py",
            "pypaimon/execution/python/index/pksorted/pk_sorted_index_group.py",
            "pypaimon/execution/python/globalindex/vindex/vindex_vector_global_index_reader.py",
            "pypaimon/execution/python/write/table_write.py",
            "pypaimon/execution/python/write/table_commit.py",
            "pypaimon/execution/python/write/format_table_write.py",
            "pypaimon/execution/python/write/format_table_commit.py",
            "pypaimon/execution/python/write/bucket_metadata.py",
            "pypaimon/execution/native/read/table_read.py",
            "pypaimon/execution/native/read/plan.py",
            "pypaimon/execution/native/read/system_table.py",
            "pypaimon/execution/native/read/vindex_reader.py",
            "pypaimon/execution/native/write/native_write.py",
            "pypaimon/execution/native/write/native_commit.py",
            "pypaimon/execution/native/write/postpone_bucket.py",
            "pypaimon/read/blob_read_support.py",
            "pypaimon/read/scan_request.py",
            "pypaimon/table/source/search_types.py",
            "pypaimon/globalindex/vindex_reader.py",
            "pypaimon/write/bucket_planning.py",
            "pypaimon/write/commit_options.py",
            "pypaimon/execution/python/write/input_routing.py",
        )
        for name in execution_files:
            self.assertIn(name, names)
        for name in names:
            if name.startswith("pypaimon/execution/") and name.endswith(".py"):
                self.assertIn(name.split("/")[2], ("python", "native", "__init__.py"))
        internal_aliases = (
            "pypaimon/execution/python/write/external_path_provider.py",
            "pypaimon/execution/python/read/predicate_json_parser.py",
            "pypaimon/execution/python/write/merge_condition.py",
            "pypaimon/common/map_blob_key_serializer.py",
            "pypaimon/utils/blob_view_lookup.py",
            "pypaimon/read/interval_partition.py",
            "pypaimon/casting/__init__.py",
            "pypaimon/casting/data_type_casts.py",
            "pypaimon/casting/schema_evolution.py",
            "pypaimon/casting/decimal_cast.py",
            "pypaimon/casting/temporal_cast.py",
            "pypaimon/casting/temporal_string.py",
            "pypaimon/casting/numeric_utils.py",
            "pypaimon/casting/temporal_utils.py",
            "pypaimon/casting/row_to_string.py",
            "pypaimon/write/partition_spec.py",
            "pypaimon/deletionvectors/__init__.py",
            "pypaimon/deletionvectors/deletion_vector.py",
            "pypaimon/deletionvectors/bitmap_deletion_vector.py",
            "pypaimon/deletionvectors/apply_deletion_vector_reader.py",
            "pypaimon/execution/python/globalindex/ray_build.py",
            "pypaimon/execution/python/read/system_table_read.py",
            "pypaimon/execution/python/read/system_table_scan.py",
            "pypaimon/common/merge_engine_dispatch.py",
            "pypaimon/read/native_plan.py",
            "pypaimon/read/native_row_iterator.py",
            "pypaimon/table/source/full_text_read.py",
            "pypaimon/table/source/vector_search_scan.py",
            "pypaimon/table/source/native_search_plan.py",
            "pypaimon/table/system/native_system_table.py",
            "pypaimon/globalindex/bitmap/__init__.py",
            "pypaimon/globalindex/btree/__init__.py",
            "pypaimon/globalindex/full_text/__init__.py",
            "pypaimon/globalindex/lumina/__init__.py",
            "pypaimon/globalindex/vindex/__init__.py",
            "pypaimon/globalindex/btree/block_reader.py",
            "pypaimon/globalindex/btree_format/block_handle.py",
            "pypaimon/globalindex/bitmap/bitmap_index_writer.py",
            "pypaimon/globalindex/block_compression.py",
            "pypaimon/index/pk/primary_key_index_source_policy.py",
            "pypaimon/index/pksorted/pk_sorted_bucket_index_state.py",
            "pypaimon/index/pksorted/pk_sorted_index_group.py",
        )
        for name in internal_aliases:
            self.assertNotIn(name, names)
        for suffix in ("/LICENSE", "/NOTICE"):
            self.assertTrue(any(".dist-info/" in name and name.endswith(suffix)
                                for name in names))
        with tarfile.open(str(self.sdist)) as archive:
            names = archive.getnames()
            prefix = "pypaimon-{}/".format(self.version)
            for name in ("pyproject.toml", "setup.py", "LICENSE", "NOTICE", "README.md"):
                self.assertIn(prefix + name, names)
            for name in execution_files:
                self.assertIn(prefix + name, names)
            for name in internal_aliases:
                self.assertNotIn(prefix + name, names)
            self.assertFalse(any("requirements.txt" in name or "requirements-dev.txt" in name
                                 or name.startswith(prefix + "pypaimon/tests/")
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

    def test_readerwriterlock_python_compatibility(self):
        _, metadata, _ = wheel_contents(self.wheel)
        requirements = [Requirement(value) for value in metadata.get_all("Requires-Dist")]
        locks = [requirement for requirement in requirements
                 if canonicalize_name(requirement.name) == "readerwriterlock"]
        for python_version, supports_1_0_10 in (("3.6", False), ("3.7", False), ("3.8", True), ("3.13", True)):
            with self.subTest(python=python_version):
                environment = default_environment()
                environment.update(python_version=python_version,
                                   python_full_version=python_version + ".0", extra="")
                active = [requirement for requirement in locks
                          if requirement.marker is None or requirement.marker.evaluate(environment)]
                self.assertEqual(1, len(active))
                self.assertIn("1.0.9", active[0].specifier)
                self.assertEqual(supports_1_0_10, "1.0.10" in active[0].specifier)
                self.assertNotIn("2.0", active[0].specifier)

    def test_python_ci_dependencies_do_not_pull_native_sql(self):
        resolve = runpy.run_path(str(self.source / "dev/install.py"))["resolve_group"]
        extras = self.config["project"]["optional-dependencies"]

        def dependencies(group):
            pending = list(resolve(self.config["dependency-groups"], group))
            names, expanded = set(), set()
            while pending:
                requirement = Requirement(pending.pop())
                names.add(canonicalize_name(requirement.name))
                if canonicalize_name(requirement.name) == "pypaimon":
                    for extra in requirement.extras - expanded:
                        pending.extend(extras[extra])
                        expanded.add(extra)
            return names

        # Follow local extras too: the Rust extension previously entered the
        # ordinary Python and Torch environments indirectly through [sql].
        for group in ("torch", "python-tests", "legacy"):
            with self.subTest(group=group):
                self.assertNotIn("pypaimon-rust", dependencies(group))
        self.assertIn("pypaimon-rust", dependencies("dev"))

    def test_dev_group_reuses_product_extras(self):
        resolve = runpy.run_path(str(self.source / "dev/install.py"))["resolve_group"]
        requirements = [Requirement(value) for value in resolve(
            self.config["dependency-groups"], "dev")]
        extras = set().union(*(requirement.extras for requirement in requirements
                               if requirement.name == "pypaimon"))
        self.assertTrue({"ray", "sql", "vindex", "full-text"}.issubset(extras))
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
