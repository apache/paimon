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

"""Conflict checks for batches that assign real buckets to postpone tables."""


class FixedBucketCommitCheck:
    """Retain the native writer's single-owner requirement across serialization.

    A writer seeds automatic sequence numbers from its baseline snapshot. Two
    owners of the same bucket can overlap those sequences and silently lose an
    update, even when their total bucket counts agree. Pending files do not own
    a real bucket and can coexist, as in Java's postpone writes.
    """

    def __init__(self, messages):
        self.owners = set()
        baselines = []
        for message in messages:
            count = message.total_buckets
            if count is None:
                continue
            if (isinstance(count, bool) or not isinstance(count, int)
                    or not 0 < count <= 2147483647
                    or not 0 <= message.bucket < count):
                raise ValueError('Invalid fixed bucket {} for total_buckets={}'.format(message.bucket, count))
            if not message.new_files:
                continue
            owner = (tuple(message.partition), message.bucket)
            if owner in self.owners:
                raise ValueError('Postpone fixed-bucket writer ownership conflict: '
                                 'one partition and bucket must have a single writer')
            self.owners.add(owner)
            if message.check_from_snapshot is not None:
                if message.check_from_snapshot < 0:
                    raise ValueError('Invalid fixed-bucket check snapshot')
                baselines.append(message.check_from_snapshot)
        self.baseline = min(baselines) if baselines else None

    def check(self, latest, entries, commit_kind, snapshot_manager, scanner):
        if (not self.owners or self.baseline is None or latest is None
                or latest.id <= self.baseline):
            return None
        partitions = {partition for partition, _ in self.owners}
        for snapshot_id in range(self.baseline + 1, latest.id + 1):
            snapshot = snapshot_manager.get_snapshot_by_id(snapshot_id)
            if snapshot is None:
                return RuntimeError('Fixed-bucket conflict check cannot find snapshot {}'.format(snapshot_id))
            changes = scanner.read_incremental_raw_entries_from_changed_partitions(snapshot, entries)
            for change in changes:
                partition = tuple(change.partition.values)
                if commit_kind == 'OVERWRITE' and partition in partitions:
                    return RuntimeError('Postpone fixed-bucket overwrite conflict: '
                                        'target partition changed after snapshot {}'.format(self.baseline))
                if change.kind == 0 and (partition, change.bucket) in self.owners:
                    return RuntimeError('Postpone fixed-bucket writer ownership conflict: '
                                        'bucket changed after snapshot {}'.format(self.baseline))
        return None
