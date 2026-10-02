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

"""Paimon's unified User-Agent ``module(transport;features) extended``."""

import functools
from typing import Iterable, Optional

import requests

from pypaimon import build_info
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions

NAME = "pypaimon"


@functools.lru_cache(maxsize=None)
def identity() -> str:
    """``pypaimon/<version>`` with the version embedded at build time, or bare ``pypaimon``."""
    version = build_info.version()
    return "{}/{}".format(NAME, version) if version else NAME


def format_user_agent(module: str, transport: str, features: Iterable[str] = (),
                      extended: Optional[str] = None) -> str:
    """Render ``module(transport;feature...) extended``, skipping empty parts."""
    user_agent = "{}({})".format(module, ";".join([transport] + [f for f in features if f]))
    return "{} {}".format(user_agent, extended) if extended else user_agent


def rest_user_agent(options: Optional[Options] = None) -> str:
    """The User-Agent of REST requests, built from the catalog's ``user-agent.*`` options."""
    data = options.to_map() if options is not None else {}
    module = _value(data, CatalogOptions.USER_AGENT_MODULE.key()) or identity()
    features = (_value(data, CatalogOptions.USER_AGENT_FEATURES.key()) or "").split()
    return format_user_agent(module, "python-requests/" + requests.__version__, features,
                             _value(data, CatalogOptions.USER_AGENT_EXTENDED.key()))


def with_feature(options: Options, feature: str) -> None:
    """Put ``feature`` first in the ``user-agent.features`` option."""
    key = CatalogOptions.USER_AGENT_FEATURES.key()
    features = [f for f in (_value(options.to_map(), key) or "").split() if f != feature]
    options.set(CatalogOptions.USER_AGENT_FEATURES, " ".join([feature] + features))


def _value(data, key: str) -> Optional[str]:
    value = data.get(key)
    return str(value).strip() if value is not None and str(value).strip() else None
