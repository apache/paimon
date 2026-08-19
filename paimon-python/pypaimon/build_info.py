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
import subprocess

from pypaimon._version import VERSION

_UNKNOWN = "UNKNOWN"
_COMMIT_ID_FILE = os.path.join(os.path.dirname(__file__), "_commit_id")


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


def commit_id():
    """Return the revision embedded in the package, or the checkout revision."""
    try:
        with open(_COMMIT_ID_FILE, "r") as commit_file:
            value = commit_file.read().strip()
            if value:
                return value
    except OSError:
        pass
    return git_commit_id()


_FULL_VERSION = "{}-{}".format(VERSION, commit_id())


def full_version():
    """Return ``<pypaimon-version>-<commit-id>`` for snapshot provenance."""
    return _FULL_VERSION
