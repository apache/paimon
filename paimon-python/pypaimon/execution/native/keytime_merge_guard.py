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

"""Partial Update keytime MERGE refusal shared by public dispatch and Native MERGE.

This module only decides whether a source is unsupported. It does not open a
Native writer, read a source, or change effective-aggregator parsing.
"""

from pypaimon.common.options.core_options import MergeEngine
from pypaimon.execution.native.keytime_capability import (
    partial_update_keytime_unsupported_message,
    table_uses_merge_map_keytime,
)


def refuse_partial_update_keytime_merge_source(source):
    """Raise when a MERGE source needs unsupported Partial Update keytime."""
    from pypaimon.write.merge_inputs import _is_table_like

    if not _is_table_like(source):
        return
    if source.options.merge_engine() != MergeEngine.PARTIAL_UPDATE:
        return
    if not table_uses_merge_map_keytime(source):
        return
    raise NotImplementedError(partial_update_keytime_unsupported_message())
