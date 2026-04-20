################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Lumina vector index options for Paimon tables (read/search only).

Strips the ``lumina.`` prefix from Paimon table properties, populates
search-related defaults, and passes them to the lumina-data SDK searcher.
"""

LUMINA_PREFIX = "lumina."

# Search-related options with defaults: (paimon_key, default_value)
_SEARCH_OPTIONS = [
    ("lumina.diskann.search.beam_width", "4"),
    ("lumina.search.parallel_number", "5"),
]


def strip_lumina_options(paimon_options):
    """Extract lumina.* keys from Paimon table options, strip the prefix,
    and populate search-related defaults.

    Args:
        paimon_options: dict of table properties, e.g.
            {"lumina.index.dimension": "128", "lumina.distance.metric": "l2", ...}
            Non-lumina keys are ignored.

    Returns:
        dict with prefix stripped and search defaults populated, e.g.
            {"index.dimension": "128", "distance.metric": "l2",
             "diskann.search.beam_width": "4", "search.parallel_number": "5", ...}
    """
    result = {}

    # 1. Populate search-related defaults
    for paimon_key, default_value in _SEARCH_OPTIONS:
        native_key = paimon_key[len(LUMINA_PREFIX):]
        result[native_key] = default_value

    # 2. Overlay user-specified lumina.* options (overrides defaults)
    for key, value in paimon_options.items():
        if key.startswith(LUMINA_PREFIX):
            native_key = key[len(LUMINA_PREFIX):]
            result[native_key] = str(value)

    return result
