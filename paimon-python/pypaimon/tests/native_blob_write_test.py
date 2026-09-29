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

"""Blob Arrow writes must retain row identity across physical file rolls."""

import io
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.native_plan import native_plan
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.blob import Blob, BlobDescriptor
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('id', pa.int32()), ('large', pa.large_binary()), ('small', pa.large_binary())])


def _table(tmp_path, options=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    settings = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                'write.native.enabled': 'true', 'target-file-row-num': '2',
                'blob.target-file-size': '32 B', 'file-index.bloom-filter.columns': 'id',
                'file-index.bloom-filter.id.items': '10', 'file-index.in-manifest-threshold': '0 B'}
    settings.update(options or {})
    fields = [DataField(0, 'id', AtomicType('INT')),
              DataField(1, 'large', AtomicType('BLOB')), DataField(2, 'small', AtomicType('BLOB'))]
    catalog.create_table('db.t', Schema(fields=fields, options=settings), False)
    return catalog.get_table('db.t')


def _data(start=0):
    return pa.table({'id': [start, start + 1],
                     'large': [bytes([start + 1]) * 40, None],
                     'small': [b'', b'abc']}, schema=_SCHEMA)


def _read(table, planner, reader):
    table = table.copy({'scan.native-plan.enabled': str(planner).lower(),
                        'read.native.enabled': str(reader).lower()})
    builder = table.new_read_builder()
    plan = native_plan(table) if planner else builder.new_scan().plan()
    read = builder.new_read()
    with ExitStack() as stack:
        if reader:
            stack.enter_context(patch.object(read, '_create_split_read',
                                             side_effect=AssertionError('Python read fallback')))
        return sorted(read.to_arrow(plan.splits()).to_pylist(), key=lambda row: row['id'])


def _physical_files(tmp_path):
    return {path for path in tmp_path.rglob('*')
            if path.is_file() and path.suffix in ('.parquet', '.blob', '.index')}


@pytest.mark.parametrize('external', [False, True])
@pytest.mark.parametrize('optimize', [False, True])
@pytest.mark.parametrize('native_commit', [False, True])
def test_native_blob_stream_rolls_and_reuses_writer(tmp_path, external, optimize, native_commit):
    options = {'data-evolution.write-cols-optimization.enabled': str(optimize).lower(),
               'commit.native.enabled': str(native_commit).lower()}
    if external:
        options.update({'data-file.external-paths': (tmp_path / 'external').as_uri(),
                        'data-file.external-paths.strategy': 'entropy-inject'})
    table = _table(tmp_path, options)
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    expected = []
    try:
        assert isinstance(writer, NativeTableWrite)
        for identifier in (1, 2):
            for start in range((identifier - 1) * 6, identifier * 6, 2):
                data = _data(start)
                writer.write_arrow(data)
                expected.extend(data.to_pylist())
            messages = writer.prepare_commit(identifier)
            files = [file for message in messages for file in message.new_files]
            normal = [file for file in files if file.file_name.endswith('.parquet')]
            assert len(normal) == 3
            assert all(file.row_count == 2 and file.extra_files for file in normal)
            assert all(file.write_cols == (None if optimize else ['id']) for file in normal)
            assert all((file.min_sequence_number, file.max_sequence_number) == (0, file.row_count - 1)
                       for file in files)
            assert all(bool(file.external_path) == external for file in files)
            # Each normal file owns the following Blob files until the next normal file.
            groups = []
            for file in files:
                if file.file_name.endswith('.parquet'):
                    groups.append({'large': [], 'small': []})
                else:
                    groups[-1][file.write_cols[0]].append(file.row_count)
            assert groups == [{'large': [1, 1], 'small': [2]}] * 3
            commit.commit(messages, identifier)
            assert writer._python_writer is None
            for planner in (False, True):
                for reader in (False, True):
                    assert _read(table, planner, reader) == expected
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('external', [False, True])
@pytest.mark.parametrize('action', ['abort', 'close', 'failed-write'])
def test_native_blob_cleanup_includes_completed_groups(tmp_path, external, action):
    options = {}
    if external:
        options.update({'data-file.external-paths': (tmp_path / 'external').as_uri(),
                        'data-file.external-paths.strategy': 'entropy-inject'})
    table = _table(tmp_path, options)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(_data())
        completed = _physical_files(tmp_path)
        assert {path.suffix for path in completed} == {'.parquet', '.blob', '.index'}
        if action == 'failed-write':
            missing = BlobDescriptor(str(tmp_path / 'missing'), 0, 40).serialize()
            with pytest.raises(Exception, match='missing'):
                writer.write_arrow(pa.table({'id': [2], 'large': [missing], 'small': [b'ok']},
                                            schema=_SCHEMA))
        else:
            # Leave another normal group open when aborting/closing.
            writer.write_arrow(_data(2).slice(0, 1))
            getattr(writer, action)()
        assert not _physical_files(tmp_path)
        assert table.snapshot_manager().get_latest_snapshot() is None
    finally:
        writer.close()


@pytest.mark.parametrize('external', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_native_blob_abort_removes_prepared_and_outstanding_files(tmp_path, external, stream):
    options = {'data-file.path-directory': 'data/nested'}
    if external:
        options['data-file.external-paths'] = (tmp_path / 'external').as_uri()
    table = _table(tmp_path, options)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        assert isinstance(writer, NativeTableWrite)
        for identifier in range(1, 3 if stream else 2):
            writer.write_arrow(_data(identifier * 2))
            messages = writer.prepare_commit(identifier) if stream else writer.prepare_commit()
            assert messages
        assert {path.suffix for path in _physical_files(tmp_path)} == {'.parquet', '.blob', '.index'}
        writer.write_arrow(_data(6))
        writer.abort()
        writer.abort()
        assert not _physical_files(tmp_path)
        assert table.snapshot_manager().get_latest_snapshot() is None
    finally:
        writer.close()


@pytest.mark.parametrize('external', [False, True])
def test_native_blob_close_releases_prepared_files_to_committer(tmp_path, external):
    options = {'data-file.external-paths': (tmp_path / 'external').as_uri()} if external else {}
    table = _table(tmp_path, options)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(_data())
        messages = writer.prepare_commit()
        prepared = _physical_files(tmp_path)
        assert prepared
        writer.close()
        writer.abort()
        assert _physical_files(tmp_path) == prepared
        commit.commit(messages)
        for planner in (False, True):
            for reader in (False, True):
                assert _read(table, planner, reader) == _data().to_pylist()
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('external', [False, True])
@pytest.mark.parametrize('commit_native_option', [False, True])
def test_native_blob_stream_abort_preserves_submitted_files(tmp_path, external, commit_native_option):
    options = {'commit.native.enabled': str(commit_native_option).lower()}
    if external:
        options['data-file.external-paths'] = (tmp_path / 'external').as_uri()
    table = _table(tmp_path, options)
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(_data())
        commit.commit(writer.prepare_commit(1), 1)
        submitted = _physical_files(tmp_path)
        writer.write_arrow(_data(2))
        assert writer.prepare_commit(2)
        writer.write_arrow(_data(4))
        assert _physical_files(tmp_path) > submitted
        writer.abort()
        assert _physical_files(tmp_path) == submitted
        for planner in (False, True):
            for reader in (False, True):
                assert _read(table, planner, reader) == _data().to_pylist()
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('published', [False, True])
def test_native_blob_abort_preserves_files_after_commit_exception(tmp_path, published):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(_data())
        messages = writer.prepare_commit()
        prepared = _physical_files(tmp_path)
        original_commit = commit.file_store_commit.commit

        def fail_commit(**kwargs):
            if published:
                original_commit(**kwargs)
            raise RuntimeError('Commit outcome is unknown')

        with patch.object(commit.file_store_commit, 'commit', side_effect=fail_commit):
            with pytest.raises(RuntimeError, match='Commit outcome is unknown'):
                commit.commit(messages)
        writer.abort()
        assert _physical_files(tmp_path) == prepared
        assert (table.snapshot_manager().get_latest_snapshot() is not None) == published
        if published:
            assert _read(table, True, True) == _data().to_pylist()
    finally:
        writer.close()
        commit.close()


class _StreamingBlob(Blob):
    def __init__(self, data):
        self.data = data
        self.opened = False

    def to_data(self):
        raise AssertionError('Blob must stay streaming')

    def to_descriptor(self):
        raise RuntimeError('No descriptor')

    def new_input_stream(self):
        self.opened = True
        return io.BytesIO(self.data)


def test_blob_object_selects_python_before_writing(tmp_path):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    blob = _StreamingBlob(b'stream')
    try:
        assert isinstance(writer, NativeTableWrite)
        writer.write_row(GenericRow([0, b'bytes', None], table.fields))
        writer.write_row(GenericRow([1, blob, None], table.fields))
        writer.write_arrow(_data(2))
        assert writer._python_writer is not None
        commit.commit(writer.prepare_commit())
        assert blob.opened
        expected = [{'id': 0, 'large': b'bytes', 'small': None},
                    {'id': 1, 'large': b'stream', 'small': None}] + _data(2).to_pylist()
        assert _read(table, True, True) == expected
    finally:
        writer.close()
        commit.close()


def test_blob_object_cannot_switch_after_native_write(tmp_path):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    blob = _StreamingBlob(b'stream')
    try:
        writer.write_arrow(_data())
        with pytest.raises(RuntimeError, match='after native data was written'):
            writer.write_row(GenericRow([2, blob, None], table.fields))
        assert not blob.opened
        commit.commit(writer.prepare_commit())
        assert _read(table, True, True) == _data().to_pylist()
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('trailing', [b'', b'padding'])
def test_inline_descriptors_keep_v1_v2_and_null_semantics(tmp_path, native, trailing):
    source = tmp_path / 'source'
    source.write_bytes(b'prefix-payload-suffix')
    v2 = BlobDescriptor(str(source), 7, 7).serialize()
    v1 = bytes([1]) + v2[9:] + trailing
    v2 += trailing
    table = _table(tmp_path, {'blob-descriptor-field': 'large',
                              'write.native.enabled': str(native).lower()})
    data = pa.table({'id': [0, 1, 2], 'large': [v1, v2, None], 'small': [b'a', None, b'']}, schema=_SCHEMA)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, NativeTableWrite) == native
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    expected = data.to_pylist()
    expected[0]['large'] = expected[1]['large'] = b'payload'
    for planner in (False, True):
        for reader in (False, True):
            assert _read(table, planner, reader) == expected
            assert _read(table.copy({'blob-as-descriptor': 'true'}), planner, reader)[0]['large'] == v1


@pytest.fixture
def http_blob_uri():
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            if self.path == '/large':
                self.send_response(200)
                self.send_header('Content-Length', str(7 + 18 * 1024 * 1024))
                self.end_headers()
                self.wfile.write(b'prefix-')
                for _ in range(18):
                    self.wfile.write(b'x' * 1024 * 1024)
                return
            if self.path in ('/gzip', '/deflate'):
                import gzip
                import zlib
                payload = b'prefix-payload'
                body = gzip.compress(payload) if self.path == '/gzip' else zlib.compress(payload)
                self.send_response(200)
                self.send_header('Content-Encoding', self.path[1:])
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path == '/missing':
                self.send_error(404)
                return
            # Deliberately ignore Range and omit Content-Length. Native must
            # stream past the prefix and determine EOF for length=-1.
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'prefix-payload')

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield 'http://127.0.0.1:{}'.format(server.server_port), requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.mark.parametrize('encoding', ['payload', 'gzip', 'deflate'])
@pytest.mark.parametrize('inline', [False, True])
@pytest.mark.parametrize('length', [3, -1])
@pytest.mark.parametrize('native', [False, True])
def test_http_descriptors_follow_uri_scheme(tmp_path, http_blob_uri, inline, length, native, encoding):
    uri, _ = http_blob_uri
    options = {'write.native.enabled': str(native).lower()}
    if inline:
        options['blob-descriptor-field'] = 'large'
    table = _table(tmp_path, options)
    descriptor = BlobDescriptor(uri + '/' + encoding, 7, length).serialize()
    data = pa.table({'id': [0], 'large': [descriptor], 'small': [None]}, schema=_SCHEMA)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, NativeTableWrite) == native
        # Write a preceding local payload to ensure HTTP input works after
        # native ownership is established, without any late fallback.
        prior = data if inline else _data(1)
        writer.write_arrow(prior)
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
        if native:
            assert writer._python_writer is None
        expected = [{'id': 0, 'large': b'pay' if length == 3 else b'payload', 'small': None}]
        expected += expected if inline else prior.to_pylist()
        for planner in (False, True):
            for reader in (False, True):
                assert _read(table, planner, reader) == expected
    finally:
        writer.close()
        commit.close()


def test_http_descriptor_failure_cleans_native_files(tmp_path, http_blob_uri):
    uri, _ = http_blob_uri
    table = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.write_arrow(_data())
        descriptor = BlobDescriptor(uri + '/missing', 0, 5).serialize()
        with pytest.raises(Exception, match='404'):
            writer.write_arrow(pa.table({'id': [2], 'large': [descriptor], 'small': [None]}, schema=_SCHEMA))
        assert not _physical_files(tmp_path)
    finally:
        writer.close()


def test_large_http_descriptor_uses_one_stream_for_all_copy_chunks(tmp_path, http_blob_uri):
    uri, requests = http_blob_uri
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    length = 18 * 1024 * 1024  # More than two native 8 MiB copy buffers.
    descriptor = BlobDescriptor(uri + '/large', 7, length).serialize()
    try:
        writer.write_arrow(pa.table({'id': [0], 'large': [descriptor], 'small': [None]}, schema=_SCHEMA))
        commit.commit(writer.prepare_commit())
        assert requests == ['/large']
        assert _read(table, True, True) == [{'id': 0, 'large': b'x' * length, 'small': None}]
    finally:
        writer.close()
        commit.close()


def test_native_blob_row_target_rolls_normal_files_at_batch_boundaries(tmp_path):
    table = _table(tmp_path, {'target-file-row-num': '3', 'blob.target-file-size': '10 MB'})
    data = pa.table({'id': list(range(7)), 'large': [b'a'] * 7, 'small': [None] * 7}, schema=_SCHEMA)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        messages = writer.prepare_commit()
        files = [file for message in messages for file in message.new_files]
        assert [file.row_count for file in files if file.file_name.endswith('.parquet')] == [7]
        for name in ('large', 'small'):
            assert [file.row_count for file in files if file.write_cols == [name]] == [3, 3, 1]
        commit.commit(messages)
        for planner in (False, True):
            for reader in (False, True):
                assert _read(table, planner, reader) == data.to_pylist()
    finally:
        writer.close()
        commit.close()
