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

"""Skip test modules that cannot run on this interpreter.

Two cases are handled:

* A module imports an optional dependency with no wheels here (e.g.
  ray/daft/datafusion/pypaimon_rust/mosaic/lance/vortex on Python 3.7). Such
  modules import the dep at load time and would raise collection errors.
* A module builds RoaringBitmap64 at runtime, but pyroaring < 1.0 (Python 3.7)
  has no BitMap64, so the 64-bit roaring index path is unavailable.

Modules that guard their optional import (pytest.importorskip / try-except) skip
themselves and are left alone. When everything is installed this is a no-op."""

import importlib.util
import os
import re

_OPTIONAL_DEPS = ("ray", "daft", "datafusion", "pypaimon_rust", "mosaic", "lance", "vortex")
_here = os.path.dirname(os.path.abspath(__file__))
collect_ignore = []


def _is_missing(name):
    try:
        return importlib.util.find_spec(name) is None
    except (ImportError, ValueError):
        return True


def _iter_test_sources():
    """Yield (path, source) for every test module pytest would collect."""
    for _root, _dirs, _files in os.walk(_here):
        for _name in _files:
            # Match pytest's default collection: both test_*.py and *_test.py.
            if not _name.endswith(".py"):
                continue
            if not (_name.startswith("test_") or _name.endswith("_test.py")):
                continue
            _path = os.path.join(_root, _name)
            try:
                with open(_path, encoding="utf-8") as _fh:
                    yield _path, _fh.read()
            except OSError:
                continue


def _ignore(path):
    rel = os.path.relpath(path, _here)
    if rel not in collect_ignore:
        collect_ignore.append(rel)


def _has_bitmap64():
    if importlib.util.find_spec("pyroaring") is None:
        return False
    try:
        return hasattr(importlib.import_module("pyroaring"), "BitMap64")
    except ImportError:
        return False


# Modules importing an optional dependency absent on this interpreter.
_absent = [d for d in _OPTIONAL_DEPS if _is_missing(d)]
if _absent:
    _top_import = re.compile(
        r"^(?:import|from)\s+(?:%s)\b" % "|".join(re.escape(d) for d in _absent),
        re.MULTILINE,
    )
    for _path, _src in _iter_test_sources():
        if _top_import.search(_src):
            _ignore(_path)

# Modules that build RoaringBitmap64 need pyroaring >= 1.0 (Python >= 3.8).
if not _has_bitmap64():
    _rb = re.compile(r"\bRoaringBitmap64\b")
    for _path, _src in _iter_test_sources():
        if _rb.search(_src):
            _ignore(_path)
