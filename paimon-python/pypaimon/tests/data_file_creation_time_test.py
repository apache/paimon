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

"""Data-file creation times follow Java local Timestamp/epoch semantics."""

import json
import os
import subprocess
import sys
import time
from datetime import datetime

import pytest


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_CASES = {
    'UTC': [('2024-01-02T12:34:56.789', '2024-01-02T12:34:56.789'),
            ('1969-12-31T23:59:59.999', '1969-12-31T23:59:59.999')],
    'Asia/Shanghai': [('2024-01-02T12:34:56.789', '2024-01-02T04:34:56.789')],
    'America/New_York': [('2024-01-02T12:34:56.789', '2024-01-02T17:34:56.789'),
                         ('2024-03-10T02:30:00.123', '2024-03-10T07:30:00.123'),
                         ('2024-11-03T01:30:00.456', '2024-11-03T05:30:00.456')],
}
_SCRIPT = r'''import json, sys, time
from datetime import datetime
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
if hasattr(time, 'tzset'):
    time.tzset()
result = []
for wall in json.loads(sys.argv[1]):
    metadata = DataFileMeta('data.parquet', 1, 1, None, None, None, None, 0, 0, 0, 0, [])
    metadata.creation_time = Timestamp.from_local_date_time(datetime.strptime(wall, '%Y-%m-%dT%H:%M:%S.%f'))
    result.append(metadata.creation_time_epoch_millis())
metadata.creation_time = None
assert metadata.creation_time_epoch_millis() is None
print(json.dumps(result))
'''


@pytest.mark.skipif(not hasattr(time, 'tzset'), reason='process TZ cannot be changed on this platform')
@pytest.mark.parametrize('zone', sorted(_CASES))
def test_creation_time_epoch_millis_matches_java_zone_rules_and_precision(zone):
    # Run separate processes so TZ changes cannot race other tests or cached
    # native timezone detection. Goldens use LocalDateTime.atZone semantics:
    # earlier instant in an overlap, forward through a spring gap.
    environment = dict(os.environ, TZ=zone)
    output = subprocess.check_output([sys.executable, '-c', _SCRIPT,
                                      json.dumps([wall for wall, _ in _CASES[zone]])], env=environment)
    assert json.loads(output) == _expected_epoch_millis(zone)


def test_create_preserves_an_explicit_creation_timestamp():
    from pypaimon.data.timestamp import Timestamp
    from pypaimon.manifest.schema.data_file_meta import DataFileMeta

    # Java's generic factory accepts supplied timestamps unchanged, while its
    # normal writer factories create millisecond timestamps.
    explicit = Timestamp(1001, 987654)
    metadata = DataFileMeta.create('data.parquet', 1, 1, None, None, None, None, 0, 0, 0, 0, [],
                                   creation_time=explicit)
    assert metadata.creation_time == explicit


_REST_SCRIPT = r'''import json, sys, time
from contextlib import ExitStack
from datetime import datetime
from unittest.mock import Mock, patch
import pyarrow as pa
from pypaimon import CatalogFactory, Schema
from pypaimon.api.api_response import ConfigResponse
from pypaimon.api.auth import BearTokenAuthProvider
from pypaimon.data.timestamp import Timestamp
from pypaimon.tests.rest.rest_server import RESTCatalogServer
if hasattr(time, 'tzset'):
    time.tzset()
server = RESTCatalogServer(sys.argv[1], BearTokenAuthProvider('test-token'),
                           ConfigResponse(defaults={'prefix': 'native-test'}), 'warehouse')
server.start()
try:
    catalog = CatalogFactory.create({'metastore': 'rest', 'uri': server.get_url(), 'warehouse': 'warehouse',
                                      'token.provider': 'bear', 'token': 'test-token', 'data-token.enabled': 'false'})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('pt', pa.string())])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, partition_keys=['pt'], options={
        'write.native.enabled': 'false', 'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        'commit.native.enabled': sys.argv[2]}), False)
    table = catalog.get_table('default.t')
    captured = []
    original = server._table_commit_handle
    def publish(method, data, *args, **kwargs):
        captured.append(json.loads(data)['statistics'])
        return original(method, data, *args, **kwargs)
    for wall in json.loads(sys.argv[3]):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        callback = Mock()
        commit.add_commit_callback(callback)
        timestamp = Timestamp.from_local_date_time(datetime.strptime(wall, '%Y-%m-%dT%H:%M:%S.%f'))
        try:
            writer.write_arrow(pa.Table.from_pylist([{'id': 1, 'pt': 'p'}], schema=schema))
            messages = writer.prepare_commit()
            for message in messages:
                for file in message.new_files:
                    file.creation_time = timestamp
            with ExitStack() as stack:
                stack.enter_context(patch.object(server, '_table_commit_handle', side_effect=publish))
                if sys.argv[2] == 'true':
                    stack.enter_context(patch.object(commit.file_store_commit, '_try_commit',
                                                     side_effect=AssertionError('native commit fell back')))
                commit.commit(messages)
            callback.call.assert_called_once()
            assert callback.call.call_args[0][0].commit_entries[0].file.creation_time == timestamp
        finally:
            writer.close()
            commit.close()
    print('CREATION_TIME_RESULT=' + json.dumps([stats[0]['lastFileCreationTime'] for stats in captured]))
finally:
    server.shutdown()
'''


def _expected_epoch_millis(zone):
    values = []
    for _, instant in _CASES[zone]:
        delta = datetime.strptime(instant, '%Y-%m-%dT%H:%M:%S.%f') - datetime(1970, 1, 1)
        values.append((delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000)
    return values


@pytest.mark.skipif(not hasattr(time, 'tzset'), reason='process TZ cannot be changed on this platform')
@pytest.mark.parametrize('zone', sorted(_CASES))
@pytest.mark.parametrize('native', [False, True], ids=['python', 'native'])
def test_rest_commit_publishes_creation_time_instants(tmp_path, zone, native):
    from pypaimon.read.native_plan import native_runtime_available

    if native and not native_runtime_available():
        pytest.skip('pypaimon-rust not installed')
    # Existing pre-epoch metadata is covered by the DFM helper test; Java's
    # per-partition accumulator clamps lastFileCreationTime to zero.
    cases = [case for case in _CASES[zone] if not case[0].startswith('1969')]
    output = subprocess.check_output(
        [sys.executable, '-c', _REST_SCRIPT, str(tmp_path), str(native).lower(),
         json.dumps([wall for wall, _ in cases])], env=dict(os.environ, TZ=zone))
    result = next(line for line in output.decode().splitlines() if line.startswith('CREATION_TIME_RESULT='))
    assert json.loads(result.split('=', 1)[1]) == [value for value in _expected_epoch_millis(zone) if value >= 0]
