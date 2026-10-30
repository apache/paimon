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

"""Runtime commit configuration independent of either execution backend."""

from typing import Optional


class CommitDedup:
    """Duplicate-commit scan parameters for one committer.

    Passed to :class:`FileStoreCommit` (via ``WriteBuilder.new_commit``) rather
    than read from table options: the bound describes *this* writer at *this*
    moment -- the branch's latest snapshot when the write started -- and would be
    wrong for any other writer inheriting it from the table schema.

    ``base_snapshot_id`` and ``base_file_millis`` are only meaningful together:
    the id bounds the scan and the mtime proves the id still names the snapshot
    it named when the bound was taken. A caller with no bound leaves both None.

    Plain class rather than a dataclass, which pypaimon still supports Python
    3.6 without.
    """

    __slots__ = ("on_first_attempt", "base_snapshot_id", "base_file_millis")

    def __init__(self, on_first_attempt: bool = False,
                 base_snapshot_id: Optional[int] = None,
                 base_file_millis: Optional[int] = None):
        self.on_first_attempt = on_first_attempt
        self.base_snapshot_id = base_snapshot_id
        self.base_file_millis = base_file_millis

    def __repr__(self) -> str:
        return ("CommitDedup(on_first_attempt=%r, base_snapshot_id=%r, "
                "base_file_millis=%r)" % (
                    self.on_first_attempt, self.base_snapshot_id,
                    self.base_file_millis))


def validate_commit_options(options):
    # These values were parsed when constructing the Python committer. Keep
    # invalid configuration failures before writers can prepare data, including
    # when commit execution will use Rust or remain lazy until publication.
    from pypaimon.manifest import avro_codec

    avro_codec(options.manifest_compression())
    options.manifest_target_size()
    options.commit_max_retries()
    options.commit_timeout()
    options.commit_min_retry_wait()
    options.commit_max_retry_wait()
    options.data_evolution_row_id_conflict_rewrite_max_size()
    options.data_evolution_enabled()
