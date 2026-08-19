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
import re
import subprocess

_UNKNOWN = "UNKNOWN"
_FULL_VERSION_FILE = os.path.join(os.path.dirname(__file__), "_full_version")
_SETUP_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "setup.py")


def _repository_root():
    python_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parent = os.path.dirname(python_root)
    if os.path.basename(python_root) == "paimon-python" and os.path.exists(
            os.path.join(parent, "pom.xml")):
        return parent
    return python_root


def git_commit_id():
    """Return the current Paimon Git revision without discovering an outer repo."""
    repository_root = _repository_root()
    env = os.environ.copy()
    env["GIT_CEILING_DIRECTORIES"] = os.path.dirname(repository_root)
    try:
        return subprocess.check_output(
            ["git", "-C", repository_root, "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            env=env,
        ).decode("utf-8").strip()
    except Exception:
        return _UNKNOWN


def _source_version():
    try:
        with open(_SETUP_FILE, "r") as setup_file:
            match = re.search(
                r'^VERSION = ["\']([^"\']+)["\']$',
                setup_file.read(),
                re.MULTILINE,
            )
            return None if match is None else match.group(1)
    except OSError:
        return None


def _load_full_version():
    """Return the embedded full version, or derive it from the checkout."""
    try:
        with open(_FULL_VERSION_FILE, "r") as full_version_file:
            value = full_version_file.read().strip()
            if value:
                return value
    except OSError:
        pass
    version = _source_version()
    return _UNKNOWN if version is None else "{}-{}".format(version, git_commit_id())


_FULL_VERSION = _load_full_version()


def full_version():
    """Return ``<pypaimon-version>-<commit-id>`` for snapshot provenance."""
    return _FULL_VERSION
