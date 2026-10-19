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

"""Avro creation timestamps retain Java's exact local millisecond fields."""

from datetime import datetime, timedelta, timezone

import fastavro
import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import GenericRow


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]


@pytest.mark.parametrize('prefix_filter', [False, True], ids=['generic', 'filtered-block'])
def test_creation_time_milliseconds_survive_real_avro_decoding(tmp_path, prefix_filter):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(pa.schema([('id', pa.int32())])), False)
    table = catalog.get_table('default.t')
    manager = ManifestFileManager(table)
    empty = GenericRow([], [])
    stats = SimpleStats(empty, empty, [])
    millis = [None, -1001, -1, 0, 1, 1001, 1005, 2017, 1_700_000_000_123]
    entries = [ManifestEntry(
        kind=0, partition=empty, bucket=0, total_buckets=1,
        file=DataFileMeta(
            file_name='data-%d.parquet' % index, file_size=1, row_count=1,
            min_key=empty, max_key=empty, key_stats=stats, value_stats=stats,
            min_sequence_number=0, max_sequence_number=0, schema_id=table.table_schema.id,
            level=0, extra_files=[], value_stats_cols=[],
            creation_time=None if value is None else Timestamp.from_epoch_millis(value),
        )) for index, value in enumerate(millis)]
    manager.write('manifest-creation-time', entries)

    # Fastavro returns aware UTC datetimes even though Paimon's TIMESTAMP
    # contains local wall fields. Verify the actual wire values independently.
    with table.file_io.new_input_stream(manager.manifest_path + '/manifest-creation-time') as stream:
        wire_times = [record['_FILE']['_CREATION_TIME'] for record in fastavro.reader(stream)]
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert wire_times == [None if value is None else epoch + timedelta(milliseconds=value) for value in millis]

    options = {'early_entry_filter': lambda bucket, total: bucket == 0 and total == 1} if prefix_filter else {}
    decoded = manager.read('manifest-creation-time', drop_stats=False, **options)
    assert [None if entry.file.creation_time is None else entry.file.creation_time.get_millisecond()
            for entry in decoded] == millis

    # Rewriting a manifest must not alter metadata handed over by a writer.
    manager.write('manifest-creation-time-rewritten', decoded)
    rewritten = manager.read('manifest-creation-time-rewritten', **options)
    assert [entry.file.creation_time for entry in rewritten] == [entry.file.creation_time for entry in entries]
