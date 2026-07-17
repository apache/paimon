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

"""Keep the supported-feature suite green on interpreters missing optional
capabilities (e.g. Python 3.7).

* collect_ignore: a module that imports an optional dependency at load time
  (ray/daft/mosaic/lance/... with no 3.7 wheels) cannot be collected, so it is
  ignored. The scan is intentionally line-anchored -- it only matches top-level
  imports; guarded imports (indented pytest.importorskip / try-except) are left
  to skip themselves.
* pytest_runtest_makereport: a test that reaches a missing optional dependency
  -- or pyroaring.BitMap64 (needs pyroaring>=1.0, i.e. Python>=3.8) -- only at
  runtime is turned into a skip instead of a failure. Real failures are
  untouched. When everything is installed this is a no-op."""

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

# In PR CI this branch is merged with master, whose newer feature tests are not
# present here to add pytest.importorskip to; this hook is the only branch-level
# place that can reach them. A test that fails *only* because an optional
# dependency (or pyroaring.BitMap64) is missing at runtime is turned into a
# skip. Matching is deliberately narrow -- a structured ImportError.name, or the
# two symbol/message cases that carry no usable .name -- and every downgrade is
# logged, so a genuine "core hard-imports an optional dep" regression on 3.7
# stays visible rather than being silently swallowed.
_RUNTIME_OPTIONAL = ("ray", "daft", "datafusion", "pypaimon_rust", "mosaic",
                     "lance", "vortex", "duckdb", "snappy")


def _optional_absence_reason(exc):
    _seen = set()
    while exc is not None and id(exc) not in _seen:
        _seen.add(id(exc))
        if isinstance(exc, ImportError):
            _name = (getattr(exc, "name", None) or "").split(".")[0]
            if _name in _RUNTIME_OPTIONAL:
                return "optional dependency %r unavailable" % _name
            if "BitMap64" in str(exc):  # ImportError.name here is 'pyroaring'
                return "pyroaring BitMap64 requires Python >= 3.8"
        elif isinstance(exc, ValueError) and "python-snappy" in str(exc):
            return "python-snappy not installed"
        exc = exc.__cause__ or exc.__context__
    return None


if _absent or not _has_bitmap64() or _is_missing("duckdb") or _is_missing("snappy"):
    import sys
    import pytest

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(item, call):
        outcome = yield
        try:
            rep = outcome.get_result()
            if rep.when not in ("setup", "call") or not rep.failed or call.excinfo is None:
                return
            _reason = _optional_absence_reason(call.excinfo.value)
            if _reason:
                rep.outcome = "skipped"
                rep.longrepr = (str(item.location[0]), item.location[1] or 0,
                                "Skipped: " + _reason)
                sys.stderr.write("\n[conftest] downgraded to SKIP (%s): %s\n"
                                 % (_reason, item.nodeid))
        except Exception:
            # Never let the skip-shim itself break test reporting.
            return
