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

"""Conditional publication must never overwrite a completed checkpoint."""

from pypaimon.filesystem.local_file_io import LocalFileIO


def test_local_create_preserves_first_complete_result(tmp_path):
    io = LocalFileIO()
    path = str(tmp_path / "nested" / "checkpoint.json")
    payload = "完整 checkpoint " * 10000
    assert io.create_file_if_absent(path, payload)
    assert not io.create_file_if_absent(path, "replacement")
    assert io.read_file_utf8(path) == payload
    assert list((tmp_path / "nested").iterdir()) == [tmp_path / "nested" / "checkpoint.json"]


def test_file_io_wrappers_preserve_conditional_creation(tmp_path):
    from unittest.mock import patch

    from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
    from pypaimon.common.identifier import Identifier
    from pypaimon.common.options import Options
    from pypaimon.filesystem.caching_file_io import CachingFileIO
    from pypaimon.filesystem.resolving_file_io import ResolvingFileIO

    delegate = LocalFileIO()
    rest = RESTTokenFileIO(Identifier.from_string('db.table'), str(tmp_path))
    wrappers = [ResolvingFileIO(Options({})), CachingFileIO(delegate, None), rest]
    with patch.object(rest, 'file_io', return_value=delegate):
        for index, io in enumerate(wrappers):
            path = str(tmp_path / str(index))
            assert io.create_file_if_absent(path, 'first')
            assert not io.create_file_if_absent(path, 'second')
            assert delegate.read_file_utf8(path) == 'first'


def _compete_for_checkpoint(path, payload, barrier, results):
    io = LocalFileIO()
    barrier.wait(timeout=30)
    results.put((payload, io.create_file_if_absent(path, payload)))


def test_local_conditional_create_has_one_process_winner(tmp_path):
    import multiprocessing

    context = multiprocessing.get_context('spawn')
    barrier = context.Barrier(4)
    results = context.Queue()
    path = str(tmp_path / 'checkpoint.json')
    payloads = [str(index) * 65536 for index in range(4)]
    processes = [context.Process(
        target=_compete_for_checkpoint,
        args=(path, payload, barrier, results)) for payload in payloads]
    try:
        for process in processes:
            process.start()
        outcomes = [results.get(timeout=45) for _ in processes]
        for process in processes:
            process.join(timeout=10)
            assert process.exitcode == 0
        winners = [payload for payload, created in outcomes if created]
        assert len(winners) == 1
        assert LocalFileIO().read_file_utf8(path) == winners[0]
        assert len(list(tmp_path.iterdir())) == 1
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
        results.close()


def test_local_publication_error_propagates_and_removes_temp(tmp_path):
    from unittest.mock import patch

    import pytest

    with patch('pypaimon.filesystem.local_file_io.os.link',
               side_effect=PermissionError('denied')):
        with pytest.raises(PermissionError, match='denied'):
            LocalFileIO().create_file_if_absent(str(tmp_path / 'result'), 'data')
    assert list(tmp_path.iterdir()) == []


def test_unsupported_conditional_create_fails_closed():
    import pytest

    from pypaimon.common.file_io import FileIO

    with pytest.raises(NotImplementedError, match='conditional file creation'):
        FileIO.create_file_if_absent(LocalFileIO(), 'unused', 'data')
