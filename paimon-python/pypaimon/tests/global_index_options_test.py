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
    ({"global-index.row-count-per-file": "2500"}, 2500),
    ({"global-index.row-count-per-shard": "100000"}, 100_000),
    ({"global-index.row-count-per-file": "2500",
      "global-index.row-count-per-shard": "100000"}, 2500),
])
def test_row_count_per_file_default_and_fallback(options, expected):
    assert CoreOptions.from_dict(options).global_index_row_count_per_shard() == expected
