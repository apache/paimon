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

"""Install TOML dependency groups on old Python/pip without --group support.

Modern development environments use ``pip install -e . --group dev`` directly.
This bridge reads the same configuration for the Python 3.6/3.7 CI lanes.
"""
import argparse
import os
import re
import subprocess
import sys

try:
    import tomllib
except ImportError:
    import tomli as tomllib


def _normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def resolve_group(groups, name, parents=()):
    name = _normalize(name)
    if name in parents:
        raise ValueError("Cyclic dependency group: {}".format(name))
    requirements = []
    for item in groups[name]:
        if isinstance(item, str):
            requirements.append(item)
        elif isinstance(item, dict) and set(item) == {"include-group"}:
            requirements.extend(resolve_group(
                groups, item["include-group"], parents + (name,)))
        else:
            raise ValueError("Invalid dependency group item: {!r}".format(item))
    return requirements


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("group")
    args = parser.parse_args()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "pyproject.toml"), "rb") as project_file:
        raw_groups = tomllib.load(project_file)["dependency-groups"]
    groups = {_normalize(name): value for name, value in raw_groups.items()}
    if len(groups) != len(raw_groups):
        raise ValueError("Duplicate normalized dependency group names")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", root] +
        resolve_group(groups, args.group))


if __name__ == "__main__":
    main()
