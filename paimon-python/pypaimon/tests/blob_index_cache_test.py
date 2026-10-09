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

import pickle
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.blob_index_cache import BlobIndexCache
from pypaimon.common.memory_size import MemorySize
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader import format_blob_reader


@pytest.mark.parametrize('budget', [1, 1024 * 1024])
@pytest.mark.parametrize('error_type', [None, IOError, KeyboardInterrupt])
def test_concurrent_load_result_and_failure_are_shared(budget, error_type):
    cache = BlobIndexCache(MemorySize.of_bytes(budget))
    entered, release, waiting = (threading.Event() for _ in range(3))
    index = ((3,), (0,))

    def load():
        entered.set()
        assert release.wait(10)
        if error_type is not None:
            raise error_type('index load failed')
        return index

    loader = Mock(side_effect=load)
    with ThreadPoolExecutor(max_workers=2) as executor:
        owner = executor.submit(cache.get_or_load, 'file.blob', loader)
        try:
            assert entered.wait(10)
            pending = cache._loading['file.blob']
            original_result = pending.result

            def wait_for_result():
                waiting.set()
                return original_result()

            with patch.object(pending, 'result', wait_for_result):
                waiter = executor.submit(cache.get_or_load, 'file.blob', loader)
                assert waiting.wait(10)
                release.set()
                for future in (owner, waiter):
                    if error_type is None:
                        assert future.result(timeout=10) is index
                    else:
                        with pytest.raises(error_type, match='index load failed'):
                            future.result(timeout=10)
        finally:
            release.set()
    assert loader.call_count == 1
    assert not cache._loading
    retained = error_type is None and budget > 1
    assert len(cache) == int(retained)
    retry = Mock(return_value=index)
    assert cache.get_or_load('file.blob', retry) == index
    assert retry.call_count == int(not retained)


@pytest.mark.parametrize('separate_caches', [False, True])
def test_other_files_and_catalogs_do_not_wait(separate_caches):
    first = BlobIndexCache(MemorySize.of_mebi_bytes(1))
    second = BlobIndexCache(MemorySize.of_mebi_bytes(1)) if separate_caches else first
    second_path = 'a.blob' if separate_caches else 'b.blob'
    both_loading = threading.Barrier(2, timeout=10)

    def load():
        both_loading.wait()
        return ((1,), (0,))

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(first.get_or_load, 'a.blob', load),
                   executor.submit(second.get_or_load, second_path, load)]
        for future in futures:
            assert future.result(timeout=10) == ((1,), (0,))


def test_disabled_cache_does_not_share_or_retain_loads():
    cache = BlobIndexCache(MemorySize.of_bytes(0))
    both_loading = threading.Barrier(2, timeout=10)

    def load():
        both_loading.wait()
        return ((1,), (0,))

    loader = Mock(side_effect=load)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(cache.get_or_load, 'a.blob', loader) for _ in range(2)]
        for future in futures:
            assert future.result(timeout=10) == ((1,), (0,))
    assert loader.call_count == 2
    assert len(cache) == 0
    assert not cache._loading


def test_hot_cache_does_not_create_a_future():
    cache = BlobIndexCache(MemorySize.of_mebi_bytes(1))
    cache.put('a.blob', (1,), (0,))
    loader = Mock(side_effect=AssertionError('hot cache invoked loader'))
    with patch('pypaimon.common.blob_index_cache.Future',
               side_effect=AssertionError('hot cache created future')):
        assert cache.get_or_load('a.blob', loader) == ((1,), (0,))


def test_serialization_does_not_copy_inflight_loads():
    cache = BlobIndexCache(MemorySize.of_mebi_bytes(1))
    entered, release = threading.Event(), threading.Event()

    def load():
        entered.set()
        assert release.wait(10)
        return ((1,), (0,))

    with ThreadPoolExecutor(max_workers=1) as executor:
        owner = executor.submit(cache.get_or_load, 'a.blob', load)
        try:
            assert entered.wait(10)
            restored = pickle.loads(pickle.dumps(cache))
            assert restored.max_size_bytes == cache.max_size_bytes
            assert len(restored) == 0
            assert not restored._loading
            assert restored.get_or_load('a.blob', lambda: ((2,), (0,))) == ((2,), (0,))
        finally:
            release.set()
        assert owner.result(timeout=10) == ((1,), (0,))
    assert cache.get('a.blob') == ((1,), (0,))


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.parametrize('blob_parallelism', [1, 4])
def test_public_shard_reads_share_index_and_keep_payloads(tmp_path, blob_parallelism):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', False)
    schema = pa.schema([('id', pa.int64()), ('payload', pa.large_binary())])
    catalog.create_table('default.blobs', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
    }), False)
    table = catalog.get_table('default.blobs')
    expected = pa.table({'id': list(range(1000)),
                         'payload': [('value-%d' % i).encode() for i in range(1000)]},
                        schema=schema)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(expected)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    cache = table.catalog_environment.blob_index_cache()
    jobs = []
    for shard in range(4):
        shard_table = catalog.get_table('default.blobs')
        assert shard_table.catalog_environment.blob_index_cache() is cache
        builder = shard_table.new_read_builder()
        splits = builder.new_scan().with_shard(shard, 4).plan().splits()
        assert len(splits) == 1
        jobs.append((builder, splits))
    paths = [file.physical_path() for _, splits in jobs
             for file in splits[0].files if file.file_name.endswith('.blob')]
    assert len(paths) == 4 and len(set(paths)) == 1

    start = threading.Barrier(4, timeout=10)
    streams = []
    stream_lock = threading.Lock()
    original_open = LocalFileIO.new_input_stream

    def open_stream(file_io, path):
        stream = original_open(file_io, path)
        if str(path).endswith('.blob'):
            with stream_lock:
                streams.append(stream)
        return stream

    def read(job):
        builder, splits = job
        start.wait()
        return builder.new_read().to_arrow(
            splits, parallelism=1, blob_parallelism=blob_parallelism)

    with patch.object(LocalFileIO, 'new_input_stream', open_stream), patch.object(
            format_blob_reader, '_decode_blob_index',
            wraps=format_blob_reader._decode_blob_index) as decode:
        for _ in range(2):
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(read, jobs))
            assert pa.concat_tables(results).equals(expected)
            assert decode.call_count == 1
            assert all(stream.closed for stream in streams)
    assert len(cache) == 1
    assert not cache._loading
