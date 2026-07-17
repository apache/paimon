#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""Skip test modules that import an optional dependency unavailable on this
interpreter (e.g. ray/daft/datafusion/pypaimon_rust/mosaic/lance/vortex have no
wheels on Python 3.7). Those modules import the dep at load time, so they cannot
be collected and would raise collection errors; ignore them instead. Modules that
guard their optional import (pytest.importorskip / try-except) are left alone and
skip themselves. When the dep is installed this is a no-op."""

import importlib.util
import os
import re

_OPTIONAL_DEPS = ("ray", "daft", "datafusion", "pypaimon_rust", "mosaic", "lance", "vortex")


def _is_missing(name):
    try:
        return importlib.util.find_spec(name) is None
    except (ImportError, ValueError):
        return True


collect_ignore = []
_absent = [d for d in _OPTIONAL_DEPS if _is_missing(d)]
if _absent:
    _here = os.path.dirname(os.path.abspath(__file__))
    _top_import = re.compile(
        r"^(?:import|from)\s+(?:%s)\b" % "|".join(re.escape(d) for d in _absent),
        re.MULTILINE,
    )
    for _root, _dirs, _files in os.walk(_here):
        for _name in _files:
            # Match pytest's default collection: both test_*.py and *_test.py.
            if not (_name.startswith("test_") or _name.endswith("_test.py")):
                continue
            if not _name.endswith(".py"):
                continue
            _path = os.path.join(_root, _name)
            try:
                with open(_path, encoding="utf-8") as _fh:
                    _src = _fh.read()
            except OSError:
                continue
            if _top_import.search(_src):
                collect_ignore.append(os.path.relpath(_path, _here))
