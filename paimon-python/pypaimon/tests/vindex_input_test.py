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

import io
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from unittest import mock

from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import (
    PaimonVindexInput,
    VindexVectorGlobalIndexReader,
    _read_parallelism,
)


class VindexInputTest(unittest.TestCase):

    def test_positional_reads_preserve_order_and_cursor(self):
        data = bytes(range(256)) * 100
        ranges = [(100, 20), (0, 12), (100, 20), (250, 30), (len(data) - 2, 10), (0, 0)]
        with tempfile.TemporaryFile() as stream:
            stream.write(data)
            stream.flush()
            stream.seek(7)
            input_ = PaimonVindexInput(stream, parallelism=4)
            try:
                self.assertEqual([data[o:o + n] for o, n in ranges], input_.pread_many(ranges))
                self.assertEqual(7, stream.tell())
                self.assertEqual([], input_.pread_many([]))
            finally:
                input_.close()
            self.assertFalse(stream.closed)
            input_.close()
            with self.assertRaisesRegex(ValueError, "closed"):
                input_.pread_many([(0, 1)])

    def test_concurrent_callbacks_share_worker_limit(self):
        entered = threading.Event()
        release = threading.Event()
        lock = threading.Lock()
        active = 0
        peak = 0

        class Stream:
            def read_at(self, length, offset):
                nonlocal active, peak
                with lock:
                    active += 1
                    peak = max(peak, active)
                    if active == 2:
                        entered.set()
                try:
                    if not release.wait(5):
                        raise TimeoutError("Readers were not released")
                    return bytes([offset]) * length
                finally:
                    with lock:
                        active -= 1

        input_ = PaimonVindexInput(Stream(), parallelism=2)
        with ThreadPoolExecutor(2) as callers:
            try:
                first = callers.submit(input_.pread_many, [(3, 2), (1, 4), (2, 1)])
                second = callers.submit(input_.pread_many, [(4, 1)])
                self.assertTrue(entered.wait(5), "Position reads did not overlap")
                release.set()
                self.assertEqual([b"\x03" * 2, b"\x01" * 4, b"\x02"], first.result(5))
                self.assertEqual([b"\x04"], second.result(5))
                self.assertEqual(2, peak)
            finally:
                release.set()
                input_.close()

    def test_failure_waits_for_other_reads_before_returning(self):
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        error = OSError("range read failed")

        class Stream:
            def read_at(self, length, offset):
                if offset == 0:
                    if not started.wait(5):
                        raise TimeoutError("Second range did not start")
                    raise error
                started.set()
                if not release.wait(5):
                    raise TimeoutError("Second range was not released")
                finished.set()
                return b"x"

        input_ = PaimonVindexInput(Stream(), parallelism=2)
        with ThreadPoolExecutor(1) as caller:
            try:
                result = caller.submit(input_.pread_many, [(0, 1), (1, 1)])
                self.assertTrue(started.wait(5))
                with self.assertRaises(FutureTimeoutError):
                    result.result(timeout=0.05)
                release.set()
                with self.assertRaises(OSError) as raised:
                    result.result(5)
                self.assertIs(error, raised.exception)
                self.assertTrue(finished.is_set())
            finally:
                release.set()
                input_.close()

    def test_seek_read_fallback_is_serial_across_callbacks(self):
        class Stream(io.BytesIO):
            def __init__(self):
                super().__init__(b"abcdefgh")
                self.guard = threading.Lock()

            def seek(self, offset):
                if not self.guard.acquire(blocking=False):
                    raise AssertionError("Concurrent seek/read")
                return super().seek(offset)

            def read(self, length):
                try:
                    return super().read(length)
                finally:
                    self.guard.release()

        input_ = PaimonVindexInput(Stream(), parallelism=4)
        try:
            with ThreadPoolExecutor(4) as pool:
                results = list(pool.map(input_.pread_many, [[(3, 2), (0, 3)]] * 20))
            self.assertEqual([[b"de", b"abc"]] * 20, results)
            self.assertIsNone(input_._executor)
        finally:
            input_.close()

    def test_serial_and_single_range_reads_do_not_start_workers(self):
        stream = mock.Mock(spec=["read_at"])
        stream.read_at.return_value = b"x"
        for parallelism, ranges in ((1, [(0, 1), (1, 1)]), (4, [(0, 1)]), (4, [])):
            input_ = PaimonVindexInput(stream, parallelism)
            try:
                self.assertEqual([b"x"] * len(ranges), input_.pread_many(ranges))
                self.assertIsNone(input_._executor)
            finally:
                input_.close()

    def test_parallelism_defaults_and_validation(self):
        for path in ("/tmp/index", "file:///tmp/index", "C:/index"):
            self.assertEqual(1, _read_parallelism({}, path))
        for path in ("s3://bucket/index", "hdfs://host/index", "oss://bucket/index"):
            self.assertEqual(4, _read_parallelism({}, path))
        for value in (1, "2", 8):
            self.assertEqual(int(value), _read_parallelism({"vindex.read.parallelism": value}, "x"))
        for value in (0, -1, "invalid", "1.5", 1.5, True):
            with self.assertRaisesRegex(ValueError, "positive integer"):
                _read_parallelism({"vindex.read.parallelism": value}, "x")

    def test_reader_releases_workers_and_stream_on_open_failure(self):
        self._check_reader_cleanup("initialize")

    def test_reader_releases_workers_and_stream_on_close(self):
        self._check_reader_cleanup("success")

    def test_reader_releases_workers_when_native_constructor_fails(self):
        self._check_reader_cleanup("constructor")

    def test_reader_releases_workers_when_native_close_fails(self):
        self._check_reader_cleanup("close")

    def _check_reader_cleanup(self, phase):
        stream = mock.Mock(spec=["read_at", "close"])
        stream.read_at.return_value = b"x"
        io_ = mock.Mock()
        io_.new_input_stream.return_value = stream
        native = mock.Mock()
        inputs = []
        workers = []

        def open_reader(input_):
            inputs.append(input_)
            self.assertEqual([b"x", b"x"], input_.pread_many([(0, 1), (1, 1)]))
            workers.extend(input_._executor._threads)
            if phase == "constructor":
                raise error
            return native

        error = OSError("native reader failed")
        if phase == "initialize":
            native.optimize_for_search.side_effect = error
        if phase == "close":
            native.close.side_effect = error
        module = mock.Mock()
        module.VectorIndexReader.side_effect = open_reader
        with mock.patch.dict("sys.modules", {"paimon_vindex": module}):
            reader = VindexVectorGlobalIndexReader(
                io_, "s3://bucket", [GlobalIndexIOMeta(file_name="index", file_size=2)])
            if phase in ("constructor", "initialize"):
                with self.assertRaises(OSError) as raised:
                    reader._ensure_loaded()
                self.assertIs(error, raised.exception)
            else:
                reader._ensure_loaded()
            if phase == "close":
                with self.assertRaises(OSError) as raised:
                    reader.close()
                self.assertIs(error, raised.exception)
            else:
                reader.close()
            reader.close()
        if phase == "constructor":
            native.close.assert_not_called()
        else:
            native.close.assert_called_once_with()
        stream.close.assert_called_once_with()
        self.assertTrue(workers)
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        with self.assertRaisesRegex(ValueError, "closed"):
            inputs[0].pread_many([(0, 1)])
