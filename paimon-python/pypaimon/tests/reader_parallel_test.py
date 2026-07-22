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

import os
import shutil
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.read.table_read import TableRead, _RemainingRows


class ResolveParallelismTest(unittest.TestCase):
    """Pure unit tests for parallelism resolution — no Paimon table needed.

    ``_resolve_parallelism`` only reads ``self._read_parallelism``, so a
    lightweight stand-in with that attribute is enough to exercise it.
    """

    @staticmethod
    def _resolve(runtime, num_splits, option=None):
        stub = types.SimpleNamespace(_read_parallelism=option)
        return TableRead._resolve_parallelism(stub, runtime, num_splits)

    def test_runtime_int_passthrough(self):
        self.assertEqual(self._resolve(4, 10), 4)
        self.assertEqual(self._resolve(1, 10), 1)

    def test_runtime_overrides_option(self):
        self.assertEqual(self._resolve(2, 10, option=8), 2)

    def test_option_used_when_no_runtime(self):
        self.assertEqual(self._resolve(None, 10, option=8), 8)

    def test_none_resolves_to_auto_min_splits_cpu(self):
        cpu = os.cpu_count() or 1
        # Neither runtime nor option set => auto.
        # More splits than CPUs => capped at CPU count.
        self.assertEqual(self._resolve(None, cpu + 100), cpu)
        # Fewer splits than CPUs => capped at split count.
        self.assertEqual(self._resolve(None, 1), 1)
        # Zero splits never yields a sub-1 worker count.
        self.assertEqual(self._resolve(None, 0), 1)

    def test_invalid_runtime_raises_with_source(self):
        with self.assertRaises(ValueError) as ctx:
            self._resolve(0, 10)
        self.assertIn("parallelism", str(ctx.exception))
        self.assertNotIn("read.parallelism", str(ctx.exception))

    def test_invalid_option_raises_with_source(self):
        with self.assertRaises(ValueError) as ctx:
            self._resolve(None, 10, option=0)
        self.assertIn("read.parallelism", str(ctx.exception))


class RemainingRowsTest(unittest.TestCase):
    """Pure unit tests for the row-quota counter — no Paimon table needed."""

    def test_unlimited(self):
        rr = _RemainingRows(None)
        self.assertEqual(rr.try_consume(1_000_000), 1_000_000)
        self.assertEqual(rr.try_consume(1), 1)
        self.assertFalse(rr.exhausted())

    def test_basic_pre_debit(self):
        rr = _RemainingRows(100)
        self.assertEqual(rr.try_consume(40), 40)
        self.assertEqual(rr.try_consume(40), 40)
        # Only 20 left, asking for 30 returns 20.
        self.assertEqual(rr.try_consume(30), 20)
        self.assertTrue(rr.exhausted())
        self.assertEqual(rr.try_consume(1), 0)

    def test_zero_request(self):
        rr = _RemainingRows(100)
        self.assertEqual(rr.try_consume(0), 0)
        # Quota unchanged.
        self.assertEqual(rr.try_consume(100), 100)

    def test_concurrent_consume_never_overcommits(self):
        rr = _RemainingRows(10_000)
        granted = []
        granted_lock = threading.Lock()
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            for _ in range(2000):
                got = rr.try_consume(7)
                if got:
                    with granted_lock:
                        granted.append(got)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sum(granted), 10_000)
        self.assertTrue(rr.exhausted())


class PipelinedBatchGeneratorTest(unittest.TestCase):

    def setUp(self):
        self.read = TableRead.__new__(TableRead)
        self.read.limit = None
        self.read._read_parallelism = None
        self.read.read_type = []
        self.read.include_row_kind = False
        self.schema = pa.schema([('value', pa.int64())])

    def _batch(self, value):
        return pa.RecordBatch.from_arrays(
            [pa.array([value], type=pa.int64())], schema=self.schema)

    def test_reads_concurrently_and_emits_in_split_order(self):
        second_started = threading.Event()

        def generate(splits, schema, blob_parallelism):
            value = splits[0]
            if value == 0 and not second_started.wait(5):
                raise TimeoutError('second split did not start concurrently')
            if value == 1:
                second_started.set()
            yield self._batch(value)

        with mock.patch.object(
            self.read, '_arrow_batch_generator', side_effect=generate
        ):
            batches = list(self.read._pipelined_arrow_batch_generator(
                [0, 1, 2], self.schema, 1, 2))

        self.assertEqual(
            [0, 1, 2],
            [batch.column(0)[0].as_py() for batch in batches],
        )

    def test_propagates_worker_error(self):
        def generate(splits, schema, blob_parallelism):
            if splits[0] == 0:
                raise RuntimeError('simulated streaming failure')
            yield self._batch(splits[0])

        with mock.patch.object(
            self.read, '_arrow_batch_generator', side_effect=generate
        ):
            with self.assertRaisesRegex(
                RuntimeError, 'simulated streaming failure'
            ):
                list(self.read._pipelined_arrow_batch_generator(
                    [0, 1], self.schema, 1, 2))

    def test_close_cancels_workers(self):
        started = threading.Barrier(2)
        closed = []
        closed_lock = threading.Lock()

        def generate(splits, schema, blob_parallelism):
            value = splits[0]
            try:
                started.wait(5)
                while True:
                    yield self._batch(value)
            finally:
                with closed_lock:
                    closed.append(value)

        with mock.patch.object(
            self.read, '_arrow_batch_generator', side_effect=generate
        ):
            batches = self.read._pipelined_arrow_batch_generator(
                [0, 1], self.schema, 1, 2)
            next(batches)
            batches.close()

        self.assertCountEqual([0, 1], closed)

    def test_public_reader_close_cancels_workers(self):
        started = threading.Barrier(2)
        closed = []
        closed_lock = threading.Lock()

        def generate(splits, schema, blob_parallelism):
            value = splits[0]
            try:
                started.wait(5)
                while True:
                    yield self._batch(value)
            finally:
                with closed_lock:
                    closed.append(value)

        with mock.patch(
            'pypaimon.read.table_read.PyarrowFieldParser.from_paimon_schema',
            return_value=self.schema,
        ), mock.patch.object(
            self.read, '_arrow_batch_generator', side_effect=generate
        ):
            reader = self.read.to_arrow_batch_reader(
                [0, 1], parallelism=2)
            reader.read_next_batch()
            reader.close()

        deadline = time.monotonic() + 1
        while len(closed) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertCountEqual([0, 1], closed)

    def test_close_does_not_wait_for_blocked_split(self):
        blocked = threading.Event()
        release = threading.Event()

        def generate(splits, schema, blob_parallelism):
            value = splits[0]
            yield self._batch(value)
            if value == 0:
                blocked.set()
                release.wait(5)

        try:
            with mock.patch(
                'pypaimon.read.table_read.PyarrowFieldParser.from_paimon_schema',
                return_value=self.schema,
            ), mock.patch.object(
                self.read, '_arrow_batch_generator', side_effect=generate
            ):
                reader = self.read.to_arrow_batch_reader(
                    [0, 1], parallelism=2)
                reader.read_next_batch()
                self.assertTrue(blocked.wait(5))
                started = time.monotonic()
                reader.close()
                self.assertLess(time.monotonic() - started, 1)
        finally:
            release.set()

    def test_later_error_bypasses_blocked_earlier_split(self):
        first_started = threading.Event()
        release = threading.Event()

        def generate(splits, schema, blob_parallelism):
            value = splits[0]
            if value == 0:
                first_started.set()
                release.wait(5)
                yield self._batch(value)
            else:
                if not first_started.wait(5):
                    raise TimeoutError('first split did not start')
                raise RuntimeError('later split failed')

        started = time.monotonic()
        try:
            with mock.patch.object(
                self.read, '_arrow_batch_generator', side_effect=generate
            ):
                with self.assertRaisesRegex(
                    RuntimeError, 'later split failed'
                ):
                    list(self.read._pipelined_arrow_batch_generator(
                        [0, 1], self.schema, 1, 2))
        finally:
            release.set()
        self.assertLess(time.monotonic() - started, 1)

    def test_zero_limit_does_not_start_reading(self):
        self.read.limit = 0
        with mock.patch(
            'pypaimon.read.table_read.PyarrowFieldParser.from_paimon_schema',
            return_value=self.schema,
        ), mock.patch.object(
            self.read, '_pipelined_arrow_batch_generator'
        ) as pipeline, mock.patch.object(
            self.read, '_arrow_batch_generator'
        ) as serial:
            reader = self.read.to_arrow_batch_reader(
                [0, 1], parallelism=2)
            with self.assertRaises(StopIteration):
                reader.read_next_batch()
            reader.close()

        pipeline.assert_not_called()
        serial.assert_not_called()


class ParallelReaderAppendOnlyTest(unittest.TestCase):
    """Append-only multi-partition table — parallel must match serial exactly."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            ('user_id', pa.int64()),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            ('dt', pa.string()),
        ])

        # 8 partitions => 8 splits.
        rows_per_partition = 250
        user_ids, item_ids, behaviors, dts = [], [], [], []
        for p in range(8):
            for i in range(rows_per_partition):
                user_ids.append(p * rows_per_partition + i)
                item_ids.append(1000 + i)
                behaviors.append(f"act-{i % 5}")
                dts.append(f"p{p}")
        cls.expected_rows = len(user_ids)
        data = pa.Table.from_pydict({
            'user_id': user_ids,
            'item_id': item_ids,
            'behavior': behaviors,
            'dt': dts,
        }, schema=cls.pa_schema)

        # Default table — read.parallelism unset => auto (parallel when
        # there are >= 2 splits and >= 2 CPUs).
        cls.table = cls._build_table('append_parallel_default', None, data)
        # Option-set table — read.parallelism=4 baked into the table schema.
        cls.table_opt_4 = cls._build_table(
            'append_parallel_opt4', {'read.parallelism': '4'}, data)
        # Option forcing serial reads.
        cls.table_opt_1 = cls._build_table(
            'append_parallel_opt1', {'read.parallelism': '1'}, data)

    @classmethod
    def _build_table(cls, name, options, data):
        schema = Schema.from_pyarrow_schema(
            cls.pa_schema,
            partition_keys=['dt'],
            options=options,
        )
        cls.catalog.create_table(f'default.{name}', schema, False)
        table = cls.catalog.get_table(f'default.{name}')
        wb = table.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        w.write_arrow(data)
        c.commit(w.prepare_commit())
        w.close()
        c.close()
        return table

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _scan_splits(self, read_builder):
        return read_builder.new_scan().plan().splits()

    def test_multi_partition_yields_multiple_splits(self):
        splits = self._scan_splits(self.table.new_read_builder())
        self.assertGreaterEqual(len(splits), 4,
                                f"expected multi-split fixture, got {len(splits)}")

    # ------------------------------------------------------------------
    # Result-parity tests for the two opt-in paths.
    # ------------------------------------------------------------------

    def test_parallel_via_method_arg_matches_serial(self):
        rb = self.table.new_read_builder()
        splits = self._scan_splits(rb)
        read = rb.new_read()
        serial = read.to_arrow(splits, parallelism=1)
        parallel = read.to_arrow(splits, parallelism=4)
        # Same split order preserved => byte-identical tables.
        self.assertEqual(serial, parallel)

        df_serial = serial.to_pandas().sort_values('user_id').reset_index(drop=True)
        df_parallel = read.to_pandas(splits, parallelism=4) \
            .sort_values('user_id').reset_index(drop=True)
        self.assertTrue(df_serial.equals(df_parallel))
        self.assertEqual(len(df_parallel), self.expected_rows)

    def test_parallel_via_table_option_matches_serial(self):
        rb_serial = self.table.new_read_builder()
        rb_parallel = self.table_opt_4.new_read_builder()
        splits_serial = self._scan_splits(rb_serial)
        splits_parallel = self._scan_splits(rb_parallel)

        serial_df = rb_serial.new_read().to_pandas(splits_serial, parallelism=1) \
            .sort_values('user_id').reset_index(drop=True)
        # No explicit parallelism — must pick up read.parallelism=4 from the table option.
        parallel_df = rb_parallel.new_read().to_pandas(splits_parallel) \
            .sort_values('user_id').reset_index(drop=True)
        self.assertTrue(serial_df.equals(parallel_df))

    def test_parallel_batch_reader_matches_serial(self):
        rb = self.table.new_read_builder()
        splits = self._scan_splits(rb)
        read = rb.new_read()
        serial = pa.Table.from_batches(
            read.to_arrow_batch_reader(splits, parallelism=1))
        parallel = pa.Table.from_batches(
            read.to_arrow_batch_reader(splits, parallelism=4))
        self.assertEqual(serial, parallel)

    def test_parallel_batch_reader_uses_table_option(self):
        rb = self.table_opt_4.new_read_builder()
        splits = self._scan_splits(rb)
        read = rb.new_read()
        with mock.patch.object(
            read,
            '_pipelined_arrow_batch_generator',
            wraps=read._pipelined_arrow_batch_generator,
        ) as pipeline:
            result = pa.Table.from_batches(
                read.to_arrow_batch_reader(splits))
        pipeline.assert_called_once()
        self.assertEqual(self.expected_rows, result.num_rows)

    def test_default_batch_reader_auto_uses_pipeline_when_multicore(self):
        if (os.cpu_count() or 1) < 2:
            self.skipTest('single-core runner: auto resolves to serial')
        rb = self.table.new_read_builder()
        splits = self._scan_splits(rb)
        read = rb.new_read()
        with mock.patch.object(
            read,
            '_pipelined_arrow_batch_generator',
            wraps=read._pipelined_arrow_batch_generator,
        ) as pipeline:
            result = pa.Table.from_batches(
                read.to_arrow_batch_reader(splits))
        pipeline.assert_called_once()
        self.assertEqual(self.expected_rows, result.num_rows)

    def test_parallel_batch_reader_limit_matches_serial(self):
        rb = self.table.new_read_builder().with_limit(600)
        splits = self._scan_splits(rb)
        read = rb.new_read()
        serial = pa.Table.from_batches(
            read.to_arrow_batch_reader(splits, parallelism=1))
        parallel = pa.Table.from_batches(
            read.to_arrow_batch_reader(splits, parallelism=4))
        self.assertEqual(serial, parallel)
        self.assertEqual(600, parallel.num_rows)

    def test_default_none_runs_auto_parallel(self):
        # No runtime arg and no table option => auto. With >= 2 splits on a
        # multi-core box this takes the parallel fan-out path; the result
        # must still match a forced-serial read row for row.
        read = self.table.new_read_builder().new_read()
        splits = self._scan_splits(self.table.new_read_builder())
        self.assertGreaterEqual(len(splits), 2)
        serial = read.to_arrow(splits, parallelism=1)
        auto = read.to_arrow(splits)
        self.assertEqual(serial, auto)
        self.assertEqual(auto.num_rows, self.expected_rows)

    def test_default_none_auto_takes_parallel_path_when_multicore(self):
        read = self.table.new_read_builder().new_read()
        splits = self._scan_splits(self.table.new_read_builder())
        # Only assert fan-out where the box actually has >= 2 CPUs, else
        # auto legitimately resolves to 1 and stays serial.
        if (os.cpu_count() or 1) < 2:
            self.skipTest("single-core runner: auto resolves to serial")
        with mock.patch.object(
            read, '_to_arrow_parallel', wraps=read._to_arrow_parallel
        ) as spy:
            read.to_arrow(splits)
        spy.assert_called_once()

    # ------------------------------------------------------------------
    # Priority: method arg > table option > built-in default.
    # ------------------------------------------------------------------

    def test_method_arg_overrides_option_to_serial(self):
        # option=4 but caller passes 1: should disable parallelism.
        read = self.table_opt_4.new_read_builder().new_read()
        with mock.patch.object(read, '_to_arrow_parallel') as patched, \
                mock.patch.object(read, '_pipelined_arrow_batch_generator') as pipeline:
            patched.side_effect = AssertionError(
                "_to_arrow_parallel should not be called when arg=1 overrides option")
            splits = self._scan_splits(self.table_opt_4.new_read_builder())
            read.to_arrow(splits, parallelism=1)
        pipeline.assert_not_called()

    def test_method_arg_overrides_option_to_parallel(self):
        # option=1 (forces serial) but caller passes 4: should enable parallelism.
        read = self.table_opt_1.new_read_builder().new_read()
        splits = self._scan_splits(self.table_opt_1.new_read_builder())
        with mock.patch.object(
            read, '_to_arrow_parallel', wraps=read._to_arrow_parallel
        ) as spy:
            result = read.to_arrow(splits, parallelism=4)
        spy.assert_called_once()
        self.assertEqual(result.num_rows, self.expected_rows)

    # ------------------------------------------------------------------
    # Boundary / invalid value handling.
    # ------------------------------------------------------------------

    def test_option_one_equals_serial(self):
        # Table option read.parallelism=1 must behave exactly like an
        # explicit parallelism=1 (serial) read.
        rb = self.table_opt_1.new_read_builder()
        splits = self._scan_splits(rb)
        read = rb.new_read()
        self.assertEqual(read.to_arrow(splits),
                         read.to_arrow(splits, parallelism=1))

    def test_parallelism_exceeds_split_count(self):
        rb = self.table.new_read_builder()
        splits = self._scan_splits(rb)
        read = rb.new_read()
        # 64 workers but only ~8 splits — should clamp internally, no error.
        result = read.to_arrow(splits, parallelism=64)
        self.assertEqual(result.num_rows, self.expected_rows)

    def test_invalid_method_arg_raises(self):
        rb = self.table.new_read_builder()
        splits = self._scan_splits(rb)
        read = rb.new_read()
        with self.assertRaises(ValueError) as ctx:
            read.to_arrow(splits, parallelism=0)
        self.assertIn("parallelism", str(ctx.exception))
        self.assertNotIn("read.parallelism", str(ctx.exception))
        with self.assertRaises(ValueError):
            read.to_pandas(splits, parallelism=-1)
        with self.assertRaises(ValueError):
            read.to_arrow_batch_reader(splits, parallelism=0)

    def test_invalid_option_value_raises(self):
        # Build a fresh table with an invalid option value.
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=['dt'],
            options={'read.parallelism': '0'},
        )
        self.catalog.create_table('default.bad_option', schema, False)
        bad = self.catalog.get_table('default.bad_option')
        # Reproduce the data so split planning yields a non-trivial plan.
        wb = bad.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        w.write_arrow(pa.Table.from_pydict({
            'user_id': [1, 2], 'item_id': [10, 20],
            'behavior': ['a', 'b'], 'dt': ['p1', 'p2'],
        }, schema=self.pa_schema))
        c.commit(w.prepare_commit())
        w.close()
        c.close()

        read = bad.new_read_builder().new_read()
        splits = bad.new_read_builder().new_scan().plan().splits()
        with self.assertRaises(ValueError) as ctx:
            read.to_arrow(splits)
        self.assertIn("read.parallelism", str(ctx.exception))

    def test_empty_splits_with_parallel_arg(self):
        rb = self.table.new_read_builder()
        read = rb.new_read()
        result = read.to_arrow([], parallelism=4)
        self.assertEqual(result.num_rows, 0)
        self.assertEqual([f.name for f in result.schema],
                         [f.name for f in self.pa_schema])

    def test_parallel_with_limit_soft_stop(self):
        # 10 calls with limit=600 should all return exactly 600 rows.
        limit = 600
        for _ in range(10):
            rb = self.table.new_read_builder().with_limit(limit)
            splits = self._scan_splits(rb)
            df = rb.new_read().to_pandas(splits, parallelism=4)
            self.assertEqual(len(df), limit)

    def test_parallel_reader_error_propagates(self):
        rb = self.table.new_read_builder()
        splits = self._scan_splits(rb)
        self.assertGreaterEqual(len(splits), 2)

        original_create = TableRead._create_split_read
        call_counter = {'n': 0}
        lock = threading.Lock()

        def flaky(self_, split, blob_parallelism=1):
            with lock:
                call_counter['n'] += 1
                idx = call_counter['n']
            if idx == 2:
                raise RuntimeError("simulated reader failure")
            return original_create(self_, split, blob_parallelism)

        with mock.patch.object(TableRead, '_create_split_read', flaky):
            read = rb.new_read()
            with self.assertRaises(RuntimeError) as ctx:
                read.to_pandas(splits, parallelism=4)
            self.assertIn("simulated reader failure", str(ctx.exception))


class ParallelReaderPrimaryKeyTest(unittest.TestCase):
    """PK + multi-bucket merge-on-read parity between serial and parallel."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            pa.field('user_id', pa.int32(), nullable=False),
            ('item_id', pa.int64()),
            ('behavior', pa.string()),
            pa.field('dt', pa.string(), nullable=False),
        ])

        schema = Schema.from_pyarrow_schema(
            cls.pa_schema,
            partition_keys=['dt'],
            primary_keys=['user_id', 'dt'],
            options={'bucket': '4'},
        )
        cls.catalog.create_table('default.pk_parallel', schema, False)
        cls.table = cls.catalog.get_table('default.pk_parallel')

        # First snapshot.
        v1 = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] * 4,
            'item_id': list(range(1001, 1041)),
            'behavior': [f"v1-{i}" for i in range(40)],
            'dt': (['p1'] * 10 + ['p2'] * 10 + ['p3'] * 10 + ['p4'] * 10),
        }, schema=cls.pa_schema)
        wb = cls.table.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        w.write_arrow(v1)
        c.commit(w.prepare_commit())
        w.close()
        c.close()

        # Second snapshot — updates some rows for the same PK to exercise merge.
        v2 = pa.Table.from_pydict({
            'user_id': [1, 2, 3, 4, 5],
            'item_id': [9001, 9002, 9003, 9004, 9005],
            'behavior': ['v2-updated'] * 5,
            'dt': ['p1', 'p1', 'p2', 'p2', 'p3'],
        }, schema=cls.pa_schema)
        wb = cls.table.new_batch_write_builder()
        w, c = wb.new_write(), wb.new_commit()
        w.write_arrow(v2)
        c.commit(w.prepare_commit())
        w.close()
        c.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_multi_bucket_yields_multiple_splits(self):
        splits = self.table.new_read_builder().new_scan().plan().splits()
        self.assertGreaterEqual(
            len(splits), 2,
            f"expected multi-bucket fixture to yield >= 2 splits, got {len(splits)}")

    def test_parallel_merge_matches_serial(self):
        rb = self.table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        read = rb.new_read()
        serial = read.to_pandas(splits).sort_values(
            ['dt', 'user_id']).reset_index(drop=True)
        parallel = read.to_pandas(splits, parallelism=4).sort_values(
            ['dt', 'user_id']).reset_index(drop=True)
        self.assertTrue(serial.equals(parallel))
        # Ensure the merge actually picked the latest version.
        # user_id=1 / dt=p1 must have behavior='v2-updated', item_id=9001.
        updated = parallel[(parallel.user_id == 1) & (parallel.dt == 'p1')]
        self.assertEqual(len(updated), 1)
        self.assertEqual(updated.iloc[0].behavior, 'v2-updated')
        self.assertEqual(updated.iloc[0].item_id, 9001)

    def test_parallel_batch_reader_merge_matches_serial(self):
        rb = self.table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        read = rb.new_read()
        serial = pa.Table.from_batches(
            read.to_arrow_batch_reader(splits, parallelism=1))
        parallel = pa.Table.from_batches(
            read.to_arrow_batch_reader(splits, parallelism=4))
        serial = serial.to_pandas().sort_values(
            ['dt', 'user_id']).reset_index(drop=True)
        parallel = parallel.to_pandas().sort_values(
            ['dt', 'user_id']).reset_index(drop=True)
        self.assertTrue(serial.equals(parallel))

    def test_parallel_with_limit_pk(self):
        limit = 12
        rb = self.table.new_read_builder().with_limit(limit)
        splits = rb.new_scan().plan().splits()
        df = rb.new_read().to_pandas(splits, parallelism=4)
        self.assertLessEqual(len(df), limit)
        # PK table merge applies limit per-split internally; in addition the
        # parallel soft-stop caps the global total. The combined output must
        # never exceed the user-visible limit.

    def test_include_row_kind_parallel(self):
        rb = self.table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        read = rb.new_read()
        read.include_row_kind = True
        serial = read.to_arrow(splits)
        parallel = read.to_arrow(splits, parallelism=4)
        self.assertEqual(serial.schema, parallel.schema)
        self.assertIn('_row_kind', serial.schema.names)
        df_s = serial.to_pandas().sort_values(['dt', 'user_id']).reset_index(drop=True)
        df_p = parallel.to_pandas().sort_values(['dt', 'user_id']).reset_index(drop=True)
        self.assertTrue(df_s.equals(df_p))

    def test_parallel_row_kind_survives_output_projection(self):
        rb_full = self.table.new_read_builder()
        predicate = rb_full.new_predicate_builder().equal('behavior', 'v2-updated')
        rb = self.table.new_read_builder().with_projection(
            ['user_id', 'dt']).with_filter(predicate)
        splits = rb.new_scan().plan().splits()
        read = rb.new_read()
        read.include_row_kind = True

        result = read.to_arrow(splits, parallelism=4)

        self.assertEqual(result.schema.names, ['_row_kind', 'user_id', 'dt'])
        self.assertEqual(result.num_rows, 5)
        self.assertEqual(set(result.column('_row_kind').to_pylist()), {'+I'})


if __name__ == '__main__':
    unittest.main()
