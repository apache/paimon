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

"""Write pending primary-key events in arrival order, as Java does."""

import pyarrow as pa

from pypaimon.common.options.core_options import ChangelogProducer
from pypaimon.table.row.key_value import KeyValue
from pypaimon.write.writer.key_value_data_writer import KeyValueDataWriter


class PostponeDataWriter(KeyValueDataWriter):
    def __init__(self, **kwargs):
        # Pending files never restore bucket sequences or produce a separate
        # input changelog. Their own events become input to bucket assignment.
        kwargs['max_seq_number'] = -1
        kwargs['changelog_producer'] = ChangelogProducer.NONE
        super().__init__(**kwargs)
        self._retract_validated = False
        self._failed = False
        self._writing_stats = None

    def write(self, data):
        self._ensure_active()
        try:
            super().write(data)
        except Exception:
            self._failed = True
            raise

    def _ensure_active(self):
        if self._failed:
            raise RuntimeError('Postpone writer failed; create a new writer')

    def _process_data(self, data):
        enhanced = self._add_system_fields(data)
        self._validate_retract(enhanced, enhanced.column('_VALUE_KIND').to_pylist())
        return pa.Table.from_batches([enhanced])

    def _validate_retract(self, data, kinds):
        if self._retract_validated:
            return
        for index, kind in enumerate(kinds):
            if kind not in (1, 3):
                continue
            row = tuple(column[index].as_py() for column in data.columns)
            kv = KeyValue(len(self.trimmed_primary_keys), len(self.table.fields)).replace(row)
            self._merge_function.reset()
            self._merge_function.add(kv)
            self._merge_function.get_result()
            self._retract_validated = True
            break

    def _check_and_roll_if_needed(self):
        if self._buffer.num_rows and (
                self._buffer.num_rows >= self.target_file_row_num
                or self._buffer.nbytes >= self.target_file_size):
            self._flush_all()

    def _flush_all(self):
        data = self._buffer.materialize()
        if data is not None and data.num_rows:
            self._write_data_to_file(data)
            self._buffer.reset()

    def _write_data_to_file(self, data):
        sequence = data.column('_SEQUENCE_NUMBER')
        self._writing_stats = (
            sequence[0].as_py(), sequence[-1].as_py(),
            sum(kind in (1, 3) for kind in data.column('_VALUE_KIND').to_pylist()))
        try:
            super()._write_data_to_file(data)
        finally:
            self._writing_stats = None

    def _create_data_file_meta(self, **kwargs):
        minimum, maximum, deletes = self._writing_stats
        kwargs['min_sequence_number'] = minimum
        kwargs['max_sequence_number'] = maximum
        meta = super()._create_data_file_meta(**kwargs)
        meta.delete_row_count = deletes
        return meta

    def prepare_commit(self):
        self._ensure_active()
        try:
            return super().prepare_commit()
        except Exception:
            self._failed = True
            self.abort()
            raise

    def close(self):
        # Prepared messages own their files; discard only unprepared events.
        self.abort()
