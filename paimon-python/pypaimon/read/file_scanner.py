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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""File-level configuration and dispatch for the public table scanner."""

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.read.scan_request import scan_partition_predicate
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.utils.range import Range


class FileScanner:
    """Preserve the file scanner API without selecting an executor on access.

    Planning uses the table's Native options. Manifest sources and follow-up
    strategies are internal to the selected executor.
    """

    _is_dispatch_file_scanner = True
    _CONFIGURATION_FIELDS = frozenset((
        'predicate', '_user_predicate', '_partition_predicate',
        'partition_key_predicate', '_explicit_partition_filter', 'limit',
        'skip_level0', 'only_read_real_buckets', 'is_streaming',
        'idx_of_this_subtask', 'number_of_para_subtasks',
        'start_pos_of_this_subtask', 'end_pos_of_this_subtask',
        'chunk_shuffle', '_global_index_result', '_row_ranges',
    ))

    def __init__(self, scan):
        self._scan = scan
        self.table = scan.table
        request = scan._scan_request()
        self.predicate = request.predicate
        self._user_predicate = request.user_predicate
        self._partition_predicate = scan.partition_predicate
        self.partition_key_predicate = request.partition_key_predicate
        self._explicit_partition_filter = request.explicit_partition_filter
        self.limit = request.limit
        self.data_evolution = request.data_evolution
        self.deletion_vectors_enabled = request.deletion_vectors_enabled
        self.idx_of_this_subtask = request.idx_of_this_subtask
        self.number_of_para_subtasks = request.number_of_para_subtasks
        self.start_pos_of_this_subtask = request.start_pos_of_this_subtask
        self.end_pos_of_this_subtask = request.end_pos_of_this_subtask
        self.chunk_shuffle = request.chunk_shuffle
        self._global_index_result = request.global_index_result
        self._row_ranges = (None if request.row_ranges is None else
                            [Range(start, end) for start, end in request.row_ranges])
        self.is_streaming = self.table.options.options.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)
        self.skip_level0 = False if self.is_streaming else self.table.options.batch_scan_skip_level0()
        self.only_read_real_buckets = self.table.options.bucket() == BucketMode.POSTPONE_BUCKET.value
        self.auth_partition_predicate = None
        self.auth_has_non_partition_filter = False
        self.scan_stats = None
        self._scanned_snapshot = None
        self._scanned_snapshot_id = None
        self._policy_overrides = set()
        self._initialized = True

    def __setattr__(self, name, value):
        initialized = self.__dict__.get('_initialized', False)
        object.__setattr__(self, name, value)
        if initialized and name in self._CONFIGURATION_FIELDS:
            if name == 'predicate':
                object.__setattr__(self, '_user_predicate', value)
                object.__setattr__(self, 'partition_key_predicate', scan_partition_predicate(
                    self.table, value, self._partition_predicate))
            if name in ('skip_level0', 'only_read_real_buckets'):
                self._policy_overrides.add(name)
            self._invalidate_python_scanner()

    def _invalidate_python_scanner(self):
        if getattr(self._scan, '_file_scanner', None) is self:
            self._scan._python_file_scanner = None

    def _configure_python_scanner(self, scanner):
        scanner._user_predicate = self._user_predicate
        scanner._partition_predicate = self._partition_predicate
        scanner._explicit_partition_filter = self._explicit_partition_filter
        scanner._set_predicate(self.predicate)
        scanner.partition_key_predicate = self.partition_key_predicate
        for name in ('limit', 'skip_level0', 'only_read_real_buckets', 'is_streaming'):
            setattr(scanner, name, getattr(self, name))
        # Preserve explicit caller overrides without routing Python execution
        # through the public facade again.
        for name in ('scan', 'scan_with_stats', 'plan_files'):
            if name in self.__dict__:
                setattr(scanner, name, self.__dict__[name])

    def _request(self):
        return self._scan._scan_request()

    def scan(self):
        from pypaimon.read.read_dispatch import plan_scan
        return plan_scan(self._scan)

    def scan_with_stats(self):
        from pypaimon.read.read_dispatch import scan_with_stats
        plan, stats = scan_with_stats(self._scan)
        self.scan_stats = stats
        return plan, stats

    def plan_files(self):
        from pypaimon.read.read_dispatch import plan_files
        return plan_files(self)

    def with_all_buckets(self):
        self.only_read_real_buckets = False
        return self

    def _apply_request(self, request):
        self.idx_of_this_subtask = request.idx_of_this_subtask
        self.number_of_para_subtasks = request.number_of_para_subtasks
        self.start_pos_of_this_subtask = request.start_pos_of_this_subtask
        self.end_pos_of_this_subtask = request.end_pos_of_this_subtask
        self.chunk_shuffle = request.chunk_shuffle
        self._global_index_result = request.global_index_result
        self._row_ranges = (None if request.row_ranges is None else
                            [Range(start, end) for start, end in request.row_ranges])

    def with_shard(self, idx_of_this_subtask, number_of_para_subtasks):
        self._apply_request(self._request().with_shard(idx_of_this_subtask, number_of_para_subtasks))
        return self

    def with_slice(self, start_pos, end_pos):
        self._apply_request(self._request().with_slice(start_pos, end_pos))
        return self

    def with_global_index_result(self, result):
        self._apply_request(self._request().with_global_index_result(result))
        return self

    def with_row_ranges(self, row_ranges):
        self._apply_request(self._request().with_row_ranges(row_ranges))
        return self

    def with_chunk_shuffle(self, seed, chunk_size):
        self._apply_request(self._request().with_chunk_shuffle(seed, chunk_size))
        return self

    def _set_predicate(self, predicate):
        self.predicate = predicate
