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

class TableRollback:
    """Rollback table to instant by delegating to catalog.rollback_to."""

    def __init__(self, catalog, identifier, table_uuid=None, commit_user=None):
        self._catalog = catalog
        self._identifier = identifier
        self._table_uuid = table_uuid
        self._commit_user = commit_user

    def rollback_to(self, instant, from_snapshot=None, from_snapshot_uuid=None):
        """Rollback table to the given instant via catalog.

        Args:
            instant: The Instant (SnapshotInstant or TagInstant) to rollback to.
            from_snapshot: Optional snapshot ID. Success only occurs when the
                latest snapshot is this snapshot.
            from_snapshot_uuid: Expected latest snapshot UUID.

        Raises:
            RuntimeError: If the rollback fails.
        """
        context = {}
        if self._table_uuid is not None:
            context['table_uuid'] = self._table_uuid
        if from_snapshot_uuid is not None:
            context['from_snapshot_uuid'] = from_snapshot_uuid
        if self._commit_user is not None:
            context['commit_user'] = self._commit_user
        try:
            self._catalog.rollback_to(self._identifier, instant, from_snapshot, **context)
        except Exception as e:
            raise RuntimeError(
                "Failed to rollback table {}: {}".format(
                    self._identifier, e)) from e
