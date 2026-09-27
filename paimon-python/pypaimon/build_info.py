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
import runpy
import subprocess

_UNKNOWN = "UNKNOWN"
_FULL_VERSION_FILE = os.path.join(os.path.dirname(__file__), "_full_version")
_VERSION_FILE = os.path.join(os.path.dirname(__file__), "_version.py")


def _repository_root():
    python_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parent = os.path.dirname(python_root)
    if os.path.basename(python_root) == "paimon-python" and os.path.exists(
            os.path.join(parent, "pom.xml")):
        return parent
    return python_root


def _git_output(args):
    repository_root = _repository_root()
    env = os.environ.copy()
    env["GIT_CEILING_DIRECTORIES"] = os.path.dirname(repository_root)
    try:
        return subprocess.check_output(
            ["git", "-C", repository_root] + args,
            stderr=subprocess.DEVNULL,
            env=env,
        ).decode("utf-8").strip()
    except Exception:
        return None


def git_commit_id():
    """Return the current Paimon Git revision without discovering an outer repo."""
    return _git_output(["rev-parse", "HEAD"]) or _UNKNOWN


def _source_version():
    try:
        return runpy.run_path(_VERSION_FILE)["VERSION"]
    except OSError:
        return None


def _embedded_full_version():
    try:
        with open(_FULL_VERSION_FILE, "r") as full_version_file:
            value = full_version_file.read().strip()
            if value:
                return value
    except OSError:
        pass
    return None


def package_version():
    """Resolve one version for metadata, sdist and wheel, even outside Git."""
    embedded = _embedded_full_version()
    if embedded:
        return embedded[len("python-"):].rsplit("-", 1)[0]
    version = _source_version()
    if version and version.endswith(".dev"):
        # Commit date, rather than build date, keeps source rebuilds stable.
        date = _git_output(["log", "-1", "--format=%cd", "--date=format:%Y%m%d"])
        return version + (date or "0")
    return version


def _load_full_version():
    """Return the embedded full version, or derive it from the checkout."""
    embedded = _embedded_full_version()
    if embedded:
        return embedded
    version = package_version()
    return (
        _UNKNOWN
        if version is None
        else "python-{}-{}".format(version, git_commit_id())
    )


_FULL_VERSION = _load_full_version()


def full_version():
    """Return ``<pypaimon-version>-<commit-id>`` for snapshot provenance."""
    return _FULL_VERSION
