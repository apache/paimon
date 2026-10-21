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

"""Public MERGE INTO API for Paimon data-evolution tables."""

from typing import Any, List, Sequence

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.merge_types import OnSpec, WhenMatched, WhenNotMatched, lit, source_col, target_col

__all__ = [
    "merge_into",
    "WhenMatched",
    "WhenNotMatched",
    "source_col",
    "target_col",
    "lit",
]


def merge_into(
    target_table,
    source: Any,
    *,
    on: OnSpec,
    when_matched: Sequence[WhenMatched] = (),
    when_not_matched: Sequence[WhenNotMatched] = (),
    commit_user: str,
    commit_identifier: int = BATCH_COMMIT_IDENTIFIER,
) -> List[CommitMessage]:
    """Prepare MERGE INTO commit messages in the current Python process.

    The returned messages must be committed by the caller's matching
    ``TableCommit``.

    SET and INSERT mappings may contain callables accepting an Arrow table
    with qualified ``t.`` target and ``s.`` source columns (INSERT has only
    source columns). Each callable evaluates the rows selected by its clause
    and returns an Arrow Array or ChunkedArray of the same length. Empty
    clauses do not invoke callbacks; callback errors propagate to the caller.
    """
    from pypaimon.write.update_dispatch import merge_into as execute
    return execute(
        target_table, source, on=on, when_matched=when_matched,
        when_not_matched=when_not_matched, commit_user=commit_user,
        commit_identifier=commit_identifier)
