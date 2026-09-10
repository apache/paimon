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

"""Local lifecycle tests for REST FileIO caching; only the remote token API is stubbed."""

import pickle
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest import mock

import pytest
from cachetools import TTLCache

from pypaimon.api.rest_api import RESTApi
from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options


@pytest.fixture
def options():
    with mock.patch.object(RESTTokenFileIO, '_TOKEN_CACHE', {}), \
            mock.patch.object(RESTTokenFileIO, '_TOKEN_LOCKS', {}):
        yield Options({'uri': 'http://127.0.0.1:1', 'token.provider': 'bear', 'token': 'test-user'})


def new_io(path, options):
    return RESTTokenFileIO(Identifier.from_string('db.table'), str(path), options)


def token_response(now, value):
    return SimpleNamespace(token={'test.credential': value}, expires_at_millis=int(now * 1000) + 7_200_000)


def test_concurrent_expiry_refresh_keeps_instance_backends_isolated(tmp_path, options):
    clock = [1_000_000.0]
    instances = [new_io(tmp_path / str(index), options) for index in range(2)]
    previous = [None, None]
    with mock.patch('pypaimon.catalog.rest.rest_token_file_io.time.time', side_effect=lambda: clock[0]), \
            mock.patch.object(RESTApi, 'load_table_token') as load:
        for generation in range(3):
            load.return_value = token_response(clock[0], str(generation))
            barrier = threading.Barrier(16)

            def write(index):
                barrier.wait(timeout=10)
                io = instances[index % 2]
                backend = io.file_io()
                path = io.path + '/generation-{}-writer-{}'.format(generation, index)
                backend.write_file(path, str(generation))
                return backend, path

            with ThreadPoolExecutor(max_workers=16) as pool:
                results = list(pool.map(write, range(16)))
            assert load.call_count == generation + 1
            for index, io in enumerate(instances):
                backend = results[index][0]
                assert backend is not previous[index]
                assert all(result[0] is backend for result in results[index::2])
                assert backend.path == io.path
                assert backend.properties.to_map()['test.credential'] == str(generation)
                assert all(io.read_file_utf8(path) == str(generation) for _, path in results[index::2])
                previous[index] = backend
            assert previous[0] is not previous[1]
            clock[0] += 7_200


def test_refresh_and_initialization_failures_do_not_poison_cache(tmp_path, options):
    io = new_io(tmp_path, options)
    clock = [1_000_000.0]
    responses = [token_response(clock[0], 'old'), OSError('token unavailable'),
                 token_response(clock[0] + 7_200, 'new')]
    with mock.patch('pypaimon.catalog.rest.rest_token_file_io.time.time', side_effect=lambda: clock[0]), \
            mock.patch.object(RESTApi, 'load_table_token', side_effect=responses) as load:
        old = io.file_io()
        clock[0] += 7_200
        with pytest.raises(OSError, match='token unavailable'):
            io.file_io()
        with mock.patch.object(FileIO, 'get', side_effect=OSError('backend unavailable')):
            with pytest.raises(OSError, match='backend unavailable'):
                io.file_io()
        backend = io.file_io()
        assert backend is not old
        assert io.file_io() is backend
        assert load.call_count == 3
        path = str(tmp_path / 'recovered')
        io.write_file(path, 'ok')
        assert io.read_file_utf8(path) == 'ok'
        assert backend.properties.to_map()['test.credential'] == 'new'


def test_cache_ttl_rebuilds_backend_without_refetching_valid_token(tmp_path, options):
    clock = [0.0]
    with mock.patch('pypaimon.catalog.rest.rest_token_file_io.TTLCache',
                    side_effect=lambda **kwargs: TTLCache(timer=lambda: clock[0], **kwargs)), \
            mock.patch.object(RESTTokenFileIO, '_FILE_IO_CACHE_TTL', 10), \
            mock.patch('pypaimon.catalog.rest.rest_token_file_io.time.time', return_value=1_000_000), \
            mock.patch.object(RESTApi, 'load_table_token', return_value=token_response(1_000_000, 'valid')) as load:
        io = new_io(tmp_path, options)
        first = io.file_io()
        path = str(tmp_path / 'before-expiry')
        io.write_file(path, 'preserved')
        clock[0] = 9
        assert io.file_io() is first
        clock[0] = 11
        assert io.file_io() is not first
        assert io.read_file_utf8(path) == 'preserved'
        assert load.call_count == 1


def test_initialized_file_io_can_be_used_in_fresh_processes(tmp_path, options):
    import time

    io = new_io(tmp_path, options)
    with mock.patch.object(RESTApi, 'load_table_token', return_value=token_response(time.time(), 'valid')):
        io.write_file(str(tmp_path / 'parent'), 'parent data')
        payload = pickle.dumps(io)
    code = '''
import pickle, sys
io = pickle.loads(sys.stdin.buffer.read())
assert io.read_file_utf8(io.path + '/parent') == 'parent data'
backend = io.file_io()
assert io.file_io() is backend
assert backend.properties.to_map()['test.credential'] == 'valid'
io.write_file(io.path + '/child-' + sys.argv[1], 'child data')
'''

    def run_child(index):
        result = subprocess.run([sys.executable, '-c', code, str(index)], input=payload,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        assert result.returncode == 0, result.stderr.decode()

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(run_child, range(4)))
    for index in range(4):
        assert io.read_file_utf8(str(tmp_path / ('child-' + str(index)))) == 'child data'
