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

import pytest

from pypaimon.common.options.core_options import CoreOptions


@pytest.mark.parametrize("options, expected", [
    ({}, 25_000_000),
    ({"sorted-index.records-per-file": "300"}, 300),
    ({"sorted-index.records-per-range": "200"}, 200),
    ({"btree-index.records-per-range": "100"}, 100),
    ({"sorted-index.records-per-range": "200",
      "btree-index.records-per-range": "100"}, 200),
    ({"sorted-index.records-per-file": "300",
      "sorted-index.records-per-range": "200",
      "btree-index.records-per-range": "100"}, 300),
])
def test_sorted_index_records_per_file_default_and_fallback(options, expected):
    assert CoreOptions.from_dict(options).sorted_index_records_per_range() == expected


@pytest.mark.parametrize("index_type", ["btree", "bitmap"])
def test_primary_key_options_exclude_global_index_file_size(index_type):
    core_options = CoreOptions.from_dict({
        "sorted-index.records-per-file": "300",
        "sorted-index.records-per-range": "200",
        "write-buffer-size": "8 mb",
    })

    resolve_options = getattr(core_options, "primary_key_%s_index_options" % index_type)
    options = resolve_options("name").to_map()

    assert "sorted-index.records-per-file" not in options
    assert "sorted-index.records-per-range" not in options
    assert options["write-buffer-size"] == "8 mb"
