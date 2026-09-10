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

"""Exercise conditional OSS writes through the real SDK against a local HTTP server."""

import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from unittest import mock
from urllib.parse import unquote, urlsplit

import pyarrow.fs as pafs
import pytest

from pypaimon.catalog.rest.rest_token import RESTToken
from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.filesystem.oss_file_io import OssFileIO

oss2 = pytest.importorskip("oss2")


@pytest.fixture
def oss_server():
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, status, body):
            self.send_response(status)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def error(self, status, code):
            self.respond(status, ('<Error><Code>' + code + '</Code></Error>').encode())

        def do_GET(self):
            server.gets += 1
            assert urlsplit(self.path).query in ('versioning', 'versioning=')
            if server.fail_method == 'GET':
                self.error(*server.failure)
                return
            status = '' if server.versioning is None else '<Status>' + server.versioning + '</Status>'
            self.respond(200, ('<VersioningConfiguration>' + status +
                               '</VersioningConfiguration>').encode())

        def do_PUT(self):
            key = unquote(urlsplit(self.path).path)
            data = self.rfile.read(int(self.headers['Content-Length']))
            with server.lock:
                server.puts += 1
                server.token = self.headers.get('x-oss-security-token')
                server.sse_headers = {key.lower()[len('x-oss-'):]: value
                                      for key, value in self.headers.items()
                                      if key.lower().startswith('x-oss-server-side-')}
                if server.fail_method == 'PUT':
                    self.error(*server.failure)
                    return
                if key in server.objects and self.headers.get('x-oss-forbid-overwrite') == 'true':
                    self.error(409, 'FileAlreadyExists')
                    return
                server.objects[key] = data
            if server.lose_response:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.respond(200, b'')

    class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.objects = {}
    server.lock = threading.Lock()
    server.versioning = None
    server.fail_method = None
    server.failure = (403, 'AccessDenied')
    server.lose_response = False
    server.puts = 0
    server.gets = 0
    server.token = None
    server.sse_headers = {}
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()
    thread.join()


def options_for(server, token=None):
    return Options({
        'fs.oss.impl': 'legacy',
        'fs.oss.endpoint': 'http://127.0.0.1:{}'.format(server.server_port),
        'fs.oss.accessKeyId': 'test-ak',
        'fs.oss.accessKeySecret': 'test-sk',
        'fs.oss.securityToken': token,
    })


def file_io(server, resolving=False):
    options = options_for(server)
    if resolving:
        options = Options(dict(options.to_map(), **{
            CatalogOptions.RESOLVING_FILE_IO_ENABLED.key(): 'true'}))
    with mock.patch.object(OssFileIO, '_initialize_oss_fs'):
        return FileIO.get('oss://test-bucket/', options)


@pytest.mark.parametrize('resolving', [False, True])
def test_atomic_competition_and_existing_content(oss_server, resolving):
    io = file_io(oss_server, resolving)
    path = 'oss://test-bucket/table/snapshot-1'
    barrier = threading.Barrier(2)

    def write(content):
        barrier.wait(timeout=10)
        return io.try_to_write_atomic(path, content)

    contents = ['提交者一', '提交者二']
    with mock.patch.object(OssFileIO, '_initialize_oss_fs'), ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(write, contents))
    assert sorted(results) == [False, True]
    assert list(oss_server.objects.values()) == [contents[results.index(True)].encode()]
    assert io.try_to_write_atomic(path, 'overwrite') is False
    assert list(oss_server.objects.values()) == [contents[results.index(True)].encode()]


def test_rest_token_refresh_keeps_oss_atomic_creation(oss_server):
    options = dict(options_for(oss_server).to_map())
    options[CatalogOptions.RESOLVING_FILE_IO_ENABLED.key()] = 'true'
    path = 'oss://test-bucket/table/snapshot-1'
    with mock.patch.object(RESTTokenFileIO, 'try_to_refresh_token'), \
            mock.patch.object(RESTTokenFileIO, '_FILE_IO_CACHE', {}), \
            mock.patch.object(OssFileIO, '_initialize_oss_fs'):
        io = RESTTokenFileIO(Identifier.from_string('default.table'), path, Options(options))
        io.token = RESTToken({'fs.oss.securityToken': 'first-token'}, 1)
        assert io.try_to_write_atomic(path, 'first')
        assert oss_server.token == 'first-token'
        io.token = RESTToken({'fs.oss.securityToken': 'refreshed-token'}, 2)
        assert io.try_to_write_atomic(path, 'overwrite') is False
        assert io.try_to_write_atomic(path.replace('snapshot-1', 'snapshot-2'), 'second')
        assert oss_server.token == 'refreshed-token'
    assert sorted(oss_server.objects.values()) == [b'first', b'second']


@pytest.mark.parametrize('versioning', ['Enabled', 'Suspended', 'Unexpected', None])
def test_versioning_fallback_preserves_legacy_writes(oss_server, versioning, tmp_path, caplog):
    oss_server.versioning = versioning
    if versioning is None:
        oss_server.fail_method = 'GET'
        oss_server.failure = (403, 'AccessDenied')
    io = file_io(oss_server)
    # Run the inherited stream/rename operations against an actual Arrow filesystem.
    io.filesystem = pafs.LocalFileSystem()
    path = 'oss://test-bucket{}/snapshot-1'.format(tmp_path)
    assert io.try_to_write_atomic(path, '兼容写入') is True
    assert io.try_to_write_atomic(path, 'overwrite') is False
    assert (tmp_path / 'snapshot-1').read_text() == '兼容写入'
    assert sorted(p.name for p in tmp_path.iterdir()) == ['snapshot-1']
    assert oss_server.puts == 0
    assert 'Concurrent commits are not protected' in caplog.text


@pytest.mark.parametrize('method,status,code', [
    ('GET', 403, 'InvalidAccessKeyId'),
    ('GET', 403, 'SecurityTokenExpired'),
    ('GET', 404, 'NoSuchBucket'),
    ('GET', 500, 'InternalError'),
    ('PUT', 403, 'AccessDenied'),
    ('PUT', 500, 'InternalError'),
    ('PUT', 409, 'OtherConflict'),
])
def test_errors_are_not_competition(oss_server, method, status, code):
    oss_server.fail_method, oss_server.failure = method, (status, code)
    with pytest.raises(OSError) as caught:
        file_io(oss_server).try_to_write_atomic('oss://test-bucket/snapshot-1', 'data')
    assert isinstance(caught.value.__cause__, oss2.exceptions.ServerError)
    assert caught.value.__cause__.code == code
    assert oss_server.objects == {}


def test_lost_response_is_not_replayed_or_reported_as_conflict(oss_server):
    oss_server.lose_response = True
    with pytest.raises(OSError) as caught:
        file_io(oss_server).try_to_write_atomic('oss://test-bucket/snapshot-1', 'data')
    assert isinstance(caught.value.__cause__, oss2.exceptions.RequestError)
    assert oss_server.puts == 1
    assert list(oss_server.objects.values()) == [b'data']


@pytest.mark.parametrize('jindo,legacy,path', [
    (False, False, 'oss://test-bucket/table/p=a%2Fb/snapshot-1'),
    (False, False, 'test-bucket/table/p=a%2Fb/snapshot-1'),
    (False, True, 'table/p=a%2Fb/snapshot-1'),
    (True, False, 'table/p=a%2Fb/snapshot-1'),
])
def test_path_modes_and_sts(oss_server, jindo, legacy, path):
    io = file_io(oss_server)
    io.properties = options_for(oss_server, token='test-sts')
    io._use_jindo, io._oss_bucket_in_endpoint = jindo, legacy
    assert io.try_to_write_atomic(path, 'data') is True
    assert oss_server.token == 'test-sts'
    assert list(oss_server.objects) == ['/test-bucket/table/p=a%2Fb/snapshot-1']


def test_wrong_bucket_rejected(oss_server):
    with pytest.raises(ValueError, match='configured OSS bucket'):
        file_io(oss_server).try_to_write_atomic('oss://other/snapshot-1', 'data')
    assert oss_server.puts == 0


def test_missing_credentials_do_not_fall_back(oss_server):
    io = file_io(oss_server)
    io.properties = Options({'fs.oss.endpoint': 'oss.example.com'})
    with pytest.raises(ValueError, match='fs.oss.accessKeyId'):
        io.try_to_write_atomic('oss://test-bucket/snapshot-1', 'data')
    assert oss_server.puts == 0


def test_versioning_is_rechecked_for_a_long_lived_file_io(oss_server, tmp_path):
    io = file_io(oss_server)
    io.filesystem = pafs.LocalFileSystem()
    assert io.try_to_write_atomic('oss://test-bucket/snapshot-1', 'first')
    oss_server.versioning = 'Enabled'
    assert io.try_to_write_atomic('oss://test-bucket{}/snapshot-2'.format(tmp_path), 'second')
    assert (tmp_path / 'snapshot-2').read_text() == 'second'
    assert list(oss_server.objects.values()) == [b'first']


@pytest.mark.parametrize('settings,expected', [
    ({'server-side-encryption': ' aes256 '}, {'server-side-encryption': 'AES256'}),
    ({'server-side-encryption': 'sm4'}, {'server-side-encryption': 'SM4'}),
    ({'server-side-encryption-key-id': ' my-cmk '},
     {'server-side-encryption': 'KMS', 'server-side-encryption-key-id': 'my-cmk'}),
    ({'server-side-data-encryption': ' sm4 '},
     {'server-side-encryption': 'KMS', 'server-side-data-encryption': 'SM4'}),
    ({'server-side-encryption': ' kms ', 'server-side-encryption-key-id': 'my-cmk',
      'server-side-data-encryption': 'sm4', 'server-side-encryption-algorithm': 'AES256'},
     {'server-side-encryption': 'KMS', 'server-side-encryption-key-id': 'my-cmk',
      'server-side-data-encryption': 'SM4'}),
    ({'server-side-encryption-algorithm': 'AES256'}, {'server-side-encryption': 'AES256'}),
])
def test_sse_headers_and_conditional_creation(oss_server, settings, expected):
    io = file_io(oss_server)
    io.properties = Options(dict(io.properties.to_map(), **{
        'fs.oss.' + key: value for key, value in settings.items()}))
    path = 'oss://test-bucket/snapshot-1'
    assert io.try_to_write_atomic(path, 'encrypted metadata')
    assert oss_server.sse_headers == expected
    assert io.try_to_write_atomic(path, 'overwrite') is False
    assert list(oss_server.objects.values()) == [b'encrypted metadata']


@pytest.mark.parametrize('settings', [
    {'server-side-encryption': 'AES-256'},
    {'server-side-encryption': 'AES256', 'server-side-encryption-key-id': 'my-cmk'},
    {'server-side-encryption': 'SM4', 'server-side-data-encryption': 'SM4'},
    {'server-side-encryption': 'KMS', 'server-side-data-encryption': 'AES256'},
    {'server-side-encryption-key-id': 'my cmk'},
    {'server-side-encryption': '', 'server-side-encryption-algorithm': 'AES256'},
])
def test_invalid_sse_is_rejected_before_io(oss_server, settings):
    io = file_io(oss_server)
    io.properties = Options(dict(io.properties.to_map(), **{
        'fs.oss.' + key: value for key, value in settings.items()}))
    with pytest.raises(ValueError, match='fs.oss.server-side-'):
        io.try_to_write_atomic('oss://test-bucket/snapshot-1', 'data')
    assert oss_server.gets == 0
    assert oss_server.puts == 0
