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

"""Parts of Paimon's unified OSS User-Agent ``module(transport;features) extended``."""

import functools
from typing import List, Optional

from pypaimon import build_info
from pypaimon.common.options import Options

USER_AGENT_MODULE = "fs.oss.user.agent.module"
USER_AGENT_FEATURES = "fs.oss.user.agent.features"
USER_AGENT_EXTENDED = "fs.oss.user.agent.extended"
# Catalog-wide keys shared with the REST client; the fs.oss keys above take precedence.
COMMON_USER_AGENT_MODULE = "user-agent.module"
COMMON_USER_AGENT_FEATURES = "user-agent.features"
COMMON_USER_AGENT_EXTENDED = "user-agent.extended"
DLF_ACCESS_TRACKING_EXTENDED_INFO = "dlf.access-tracking.extended-info"

_NAME = "pypaimon"


@functools.lru_cache(maxsize=None)
def identity() -> str:
    """``pypaimon/<version>`` with the version embedded at build time, or bare ``pypaimon``."""
    version = build_info.version()
    return "{}/{}".format(_NAME, version) if version else _NAME


def module(options: Options) -> Optional[str]:
    """The configured module, or None to keep the backend's own."""
    return _effective(options, USER_AGENT_MODULE, COMMON_USER_AGENT_MODULE)


def features(options: Options) -> str:
    """The identity followed by the configured features, space separated."""
    user_features = _split(_effective(options, USER_AGENT_FEATURES, COMMON_USER_AGENT_FEATURES))
    if any(f == _NAME or f.startswith(_NAME + "/") for f in user_features):
        return " ".join(user_features)
    return " ".join([identity()] + user_features)


def extended(options: Options) -> Optional[str]:
    """The configured extended info with the DLF access-tracking info appended, or None."""
    parts = [_effective(options, USER_AGENT_EXTENDED, COMMON_USER_AGENT_EXTENDED),
             _effective(options, DLF_ACCESS_TRACKING_EXTENDED_INFO)]
    parts = [p for p in parts if p]
    return " ".join(parts) if parts else None


def _effective(options: Options, *keys: str) -> Optional[str]:
    """The first non-blank value among ``keys``, stripped."""
    data = options.to_map()
    for key in keys:
        value = data.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _split(value) -> List[str]:
    return str(value).split() if value is not None else []
