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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import platform
from typing import Optional

PYPAIMON_DISTRIBUTION = "pypaimon"
UNKNOWN_VERSION = "unknown"


def _distribution_version(distribution: str) -> str:
    try:
        from importlib.metadata import version
    except ImportError:
        import pkg_resources
        return pkg_resources.get_distribution(distribution).version
    # PackageNotFoundError inherits ImportError, so keep lookup outside the import guard.
    return version(distribution)


def get_pypaimon_version() -> str:
    try:
        return _distribution_version(PYPAIMON_DISTRIBUTION)
    except Exception:
        return UNKNOWN_VERSION


def get_user_agent(client_name: Optional[str] = None) -> str:
    products = []
    if client_name:
        products.append(client_name)
    products.extend([
        "PyPaimon/{}".format(get_pypaimon_version()),
        "Python/{}".format(platform.python_version()),
    ])
    return " ".join(products)
