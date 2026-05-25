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

"""
Predicate pushdown correctness regression suite.

Locks in the invariants:

  - PK tables: manifest stats pruning consults *key_stats only*. A
    value-only predicate must short-circuit to "keep the file" even if
    value_stats would say otherwise.
  - Reader-level value predicate is applied *after* merge — covered rows
    cannot resurface even when an older file passes manifest pruning.
  - Append-only tables: value_stats pruning is false-positive-safe (may
    over-include) but never false-negative (never drops a live row).

Three layers:

  1. Unit  — synthetic ManifestEntry to assert the manifest gate's exact
             behaviour without relying on full I/O.
  2. Round-trip — write real snapshots, read back through the public API,
             compare to the source-of-truth (post-merge oracle).
  3. Property — random datasets + random predicates, asserting that the
             pushed-down result equals the full-scan-then-filter oracle.
             (Deterministic random; no hypothesis dependency, keeps the
             Python 3.6 compatibility contract intact.)
"""

import os
import random
import shutil
import tempfile
import unittest
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate import Predicate
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow


# ---------------------------------------------------------------------------
# Synthetic-fixture helpers used by the unit-level tests.
# ---------------------------------------------------------------------------
def _stats(min_vals: List[Any], max_vals: List[Any],
           fields: List[DataField], null_counts: Optional[List[int]] = None) -> SimpleStats:
    if null_counts is None:
        null_counts = [0] * len(min_vals)
    return SimpleStats(
        min_values=GenericRow(min_vals, fields),
        max_values=GenericRow(max_vals, fields),
        null_counts=null_counts,
    )


def _make_pk_entry(level: int, key_stats: SimpleStats, value_stats: SimpleStats,
                   row_count: int = 1, schema_id: int = 0,
                   value_stats_cols: Optional[List[str]] = None) -> ManifestEntry:
    file = DataFileMeta.create(
        file_name=f'data-L{level}-{random.randint(0, 1 << 30)}.parquet',
        file_size=1024,
        row_count=row_count,
        min_key=key_stats.min_values,
        max_key=key_stats.max_values,
        key_stats=key_stats,
        value_stats=value_stats,
        min_sequence_number=0,
        max_sequence_number=0,
        schema_id=schema_id,
        level=level,
        extra_files=[],
        value_stats_cols=value_stats_cols,
    )
    return ManifestEntry(
        kind=0, partition=GenericRow([], []), bucket=0,
        total_buckets=1, file=file,
    )


def _build_pk_scanner(table, predicate: Optional[Predicate]) -> FileScanner:
    """Construct a FileScanner without running any actual scan."""
    return FileScanner(
        table=table,
        manifest_scanner=lambda: [],
        predicate=predicate,
    )


# ---------------------------------------------------------------------------
# Layer 1 — Unit: assert _filter_manifest_entry behaviour on synthetic input.
# ---------------------------------------------------------------------------
class FilterManifestEntryUnitTest(unittest.TestCase):
    """Pin down the contract of FileScanner._filter_manifest_entry."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

        cls._pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('val', pa.int64()),
        ])
        # PK table — exercises the stats-only path.
        cls.catalog.create_table(
            'default.pk_plain',
            Schema.from_pyarrow_schema(
                cls._pa_schema,
                primary_keys=['id'],
                options={'bucket': '1', 'file.format': 'parquet'},
            ),
            False,
        )
        # Append-only table — exercises the value_stats path for comparison.
        cls.catalog.create_table(
            'default.append_only',
            Schema.from_pyarrow_schema(
                cls._pa_schema,
                options={'file.format': 'parquet', 'metadata.stats-mode': 'full'},
            ),
            False,
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    @staticmethod
    def _id_field():
        return DataField(0, 'id', AtomicType('BIGINT', nullable=False))

    @staticmethod
    def _val_field():
        return DataField(1, 'val', AtomicType('BIGINT', nullable=True))

    # ---- The headline invariant ----------------------------------------
    def test_pk_table_uses_key_stats_only_not_value_stats(self):
        """The whole point: PK manifest pruning must NEVER consult value_stats.

        We construct an entry where:
          * key_stats says id ∈ [10, 20]   ← truthful PK range, includes 15
          * value_stats says val ∈ [99, 99] ← intentionally a lie that, if
            consulted, would let a `val == 0` predicate falsely drop the file.

        The query is on the value column (`val == 0`). On a PK table the
        Python design only projects the predicate onto PK columns first
        (which yields an empty/no-op predicate), so the file MUST be kept.
        If the implementation regressed and consulted value_stats, the file
        would be dropped and the (post-merge) row that actually has val=0
        in some other file would still come back — silently incorrect.
        """
        table = self.catalog.get_table('default.pk_plain')
        pred = table.new_read_builder().new_predicate_builder().equal('val', 0)

        entry = _make_pk_entry(
            level=1,
            key_stats=_stats([10], [20], [self._id_field()]),
            # Lie about val: stats claim 99..99, predicate is val==0.
            value_stats=_stats([99], [99], [self._val_field()]),
        )
        scanner = _build_pk_scanner(table, pred)
        # primary_key_predicate is None for a value-only predicate on PK
        # table → the gate must short-circuit to True regardless of stats.
        self.assertIsNone(scanner.primary_key_predicate,
                          "value predicate must not project onto PK")
        self.assertTrue(scanner._filter_manifest_entry(entry),
                        "PK table must NOT consult value_stats — entry kept")

    def test_pk_table_uses_key_stats_to_drop_outside_range(self):
        """Symmetric: when the predicate IS on PK and key_stats excludes it, drop."""
        table = self.catalog.get_table('default.pk_plain')
        pred = table.new_read_builder().new_predicate_builder().equal('id', 5)

        entry = _make_pk_entry(
            level=1,
            key_stats=_stats([10], [20], [self._id_field()]),
            value_stats=_stats([0], [0], [self._val_field()]),
        )
        scanner = _build_pk_scanner(table, pred)
        self.assertFalse(scanner._filter_manifest_entry(entry),
                         "PK predicate outside key_stats range must drop")

    def test_pk_table_uses_key_stats_to_keep_inside_range(self):
        table = self.catalog.get_table('default.pk_plain')
        pred = table.new_read_builder().new_predicate_builder().equal('id', 15)

        entry = _make_pk_entry(
            level=1,
            key_stats=_stats([10], [20], [self._id_field()]),
            value_stats=_stats([0], [0], [self._val_field()]),
        )
        scanner = _build_pk_scanner(table, pred)
        self.assertTrue(scanner._filter_manifest_entry(entry))

    def test_pk_compound_predicate_only_consults_key_stats(self):
        """Dual to test_pk_table_uses_key_stats_only_*: covers the
        non-early-return branch.

        Predicate is `id == 15 AND val == 0`. The PK projection yields
        `id == 15` which key_stats [10,20] keeps. value_stats deliberately
        lies that val ∈ [99,99], so a (buggy) implementation that consulted
        value_stats anywhere — even AFTER the early return — would drop the
        file (Equal.test_by_stats(99,99,[0]) is False). The correct
        implementation reaches `primary_key_predicate.test_by_simple_stats`
        which only ever touches key_stats, so the assertion is True.
        """
        table = self.catalog.get_table('default.pk_plain')
        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.and_predicates([
            pb.equal('id', 15),
            pb.equal('val', 0),
        ])

        entry = _make_pk_entry(
            level=1,
            key_stats=_stats([10], [20], [self._id_field()]),
            # value_stats lies: claims val ∈ [99,99]. If consulted, val==0
            # is outside the range and the file would be dropped.
            value_stats=_stats([99], [99], [self._val_field()]),
        )
        scanner = _build_pk_scanner(table, pred)
        # PK predicate is now non-empty (id == 15) — exercises the path
        # that calls primary_key_predicate.test_by_simple_stats(key_stats).
        self.assertIsNotNone(scanner.primary_key_predicate)
        self.assertTrue(scanner._filter_manifest_entry(entry),
                        "value_stats must NEVER be consulted on PK tables — "
                        "even when the PK predicate path runs")

    # ---- Append-only path uses value_stats (and that IS correct) -------
    def _append_entry(self, val_min: int, val_max: int) -> ManifestEntry:
        """Append-only entries: value_stats covers BOTH id and val (the
        projection in SimpleStatsEvolution maps schema indices through
        value_stats_cols)."""
        fields = [self._id_field(), self._val_field()]
        return _make_pk_entry(
            level=0,
            key_stats=_stats([0], [0], [self._id_field()]),
            value_stats=_stats([0, val_min], [0, val_max], fields),
            value_stats_cols=['id', 'val'],
        )

    def test_append_only_uses_value_stats_to_drop(self):
        """Append-only has no L0/merge complications — value_stats pruning
        is safe and required for performance."""
        table = self.catalog.get_table('default.append_only')
        pred = table.new_read_builder().new_predicate_builder().equal('val', 5)

        # value_stats excludes 5 → must drop.
        scanner = _build_pk_scanner(table, pred)
        self.assertFalse(scanner._filter_manifest_entry(self._append_entry(10, 20)))

    def test_append_only_uses_value_stats_to_keep(self):
        table = self.catalog.get_table('default.append_only')
        pred = table.new_read_builder().new_predicate_builder().equal('val', 15)

        scanner = _build_pk_scanner(table, pred)
        self.assertTrue(scanner._filter_manifest_entry(self._append_entry(10, 20)))


# ---------------------------------------------------------------------------
# Layer 2 — Round-trip integration: write real snapshots, read via public API.
# ---------------------------------------------------------------------------
class PushdownRoundTripIntegrationTest(unittest.TestCase):
    """Write multi-snapshot tables and verify pushdown vs full-scan oracle."""

    # Suppress compaction so L0 deterministically persists across snapshots.
    _SUPPRESS = {
        'bucket': '1',
        'num-levels': '3',
        'num-sorted-run.compaction-trigger': '999',
        'num-sorted-run.stop-trigger': '999',
        'compaction.max-size-amplification-percent': '999',
    }

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_pk_table(self, name: str) -> Any:
        opts = dict(self._SUPPRESS)
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('val', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=['id'], options=opts,
        )
        full = f'default.{name}'
        self.catalog.create_table(full, schema, False)
        return self.catalog.get_table(full)

    def _create_append_table(self, name: str) -> Any:
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('val', pa.int64()),
            ('grp', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, options={'file.format': 'parquet', 'metadata.stats-mode': 'full'},
        )
        full = f'default.{name}'
        self.catalog.create_table(full, schema, False)
        return self.catalog.get_table(full)

    def _write(self, table, batches: List[List[Dict]]):
        pa_schema = table.table_schema.to_arrow_schema() if hasattr(
            table.table_schema, 'to_arrow_schema') else None
        for rows in batches:
            wb = table.new_batch_write_builder()
            w = wb.new_write()
            c = wb.new_commit()
            try:
                if pa_schema is not None:
                    batch = pa.Table.from_pylist(rows, schema=pa_schema)
                else:
                    batch = pa.Table.from_pylist(rows)
                w.write_arrow(batch)
                c.commit(w.prepare_commit())
            finally:
                w.close()
                c.close()

    def _read_all(self, table, predicate=None) -> List[Dict]:
        rb = table.new_read_builder()
        if predicate is not None:
            rb = rb.with_filter(predicate)
        scan = rb.new_scan()
        splits = scan.plan().splits()
        if not splits:
            return []
        return rb.new_read().to_arrow(splits).to_pylist()

    # -------------------------------------------------------------------
    # Partition stats false-positive safety: predicate on a value column
    # never accidentally drops a manifest. (A filter ON the partition column
    # is exact; this asserts the orthogonal predicate path.)
    # -------------------------------------------------------------------
    def test_append_value_predicate_matches_oracle(self):
        table = self._create_append_table('rt_append_val')
        rows = [{'id': i, 'val': i * 10, 'grp': 'a' if i % 2 else 'b'}
                for i in range(100)]
        # Two snapshots → two manifests → exercises manifest-level partition
        # stats pruning even on append tables.
        self._write(table, [rows[:50], rows[50:]])

        pred_builder = table.new_read_builder().new_predicate_builder()
        for predicate, oracle_pred in [
            (pred_builder.equal('val', 250), lambda r: r['val'] == 250),
            (pred_builder.greater_than('val', 800), lambda r: r['val'] > 800),
            (pred_builder.between('val', 100, 300),
             lambda r: 100 <= r['val'] <= 300),
            (pred_builder.is_in('grp', ['a']), lambda r: r['grp'] == 'a'),
        ]:
            with self.subTest(predicate=predicate.method):
                got = sorted(self._read_all(table, predicate=predicate),
                             key=lambda r: r['id'])
                want = sorted([r for r in rows if oracle_pred(r)],
                              key=lambda r: r['id'])
                self.assertEqual(got, want,
                                 "pushed-down predicate result must equal full-scan oracle")

    def test_pk_pk_predicate_matches_oracle(self):
        """PK predicate on PK table: exact, exercises key_stats path."""
        table = self._create_pk_table('rt_pk_pk_pred')
        rows = [{'id': i, 'val': i * 7} for i in range(50)]
        self._write(table, [rows[:25], rows[25:]])

        pb = table.new_read_builder().new_predicate_builder()
        for predicate, oracle in [
            (pb.equal('id', 13), lambda r: r['id'] == 13),
            (pb.is_in('id', [1, 5, 10, 99]),
             lambda r: r['id'] in {1, 5, 10, 99}),
            (pb.greater_or_equal('id', 40), lambda r: r['id'] >= 40),
        ]:
            with self.subTest(predicate=predicate.method):
                got = sorted(self._read_all(table, predicate=predicate),
                             key=lambda r: r['id'])
                want = sorted([r for r in rows if oracle(r)],
                              key=lambda r: r['id'])
                self.assertEqual(got, want)

    def test_pk_value_predicate_matches_post_merge_oracle(self):
        """A value predicate on a PK table: post-merge oracle == reader output.

        This is the 'value filter applied after merge' contract translated
        into a directly-checkable property: the answer is whatever you'd
        get by (a) merging snapshots into latest-per-PK, (b) applying the
        predicate to that merged set in Python.

        Note: this asserts end-to-end semantics only (predicate is applied
        post-merge, not per-file). The "PK manifest gate must not consult
        value_stats" invariant is locked down in the unit tests above
        (``test_pk_table_uses_key_stats_only_*``); this round-trip case
        does not by itself catch a regression where the gate misuses
        value_stats, since the file-level effect is masked by the
        post-merge filter.
        """
        table = self._create_pk_table('rt_pk_val_pred')
        self._write(table, [
            [{'id': i, 'val': i} for i in range(20)],
            [{'id': i, 'val': i + 1000} for i in range(0, 20, 2)],  # update evens
        ])
        # Oracle: latest write per PK wins.
        merged: Dict[int, int] = {}
        for batch in [
            [(i, i) for i in range(20)],
            [(i, i + 1000) for i in range(0, 20, 2)],
        ]:
            for k, v in batch:
                merged[k] = v

        pb = table.new_read_builder().new_predicate_builder()
        for predicate, oracle in [
            (pb.greater_than('val', 500), lambda v: v > 500),
            (pb.less_or_equal('val', 5), lambda v: v <= 5),
            (pb.between('val', 1000, 1010), lambda v: 1000 <= v <= 1010),
        ]:
            with self.subTest(predicate=predicate.method):
                got = sorted(self._read_all(table, predicate=predicate),
                             key=lambda r: r['id'])
                want = sorted(
                    [{'id': k, 'val': v} for k, v in merged.items() if oracle(v)],
                    key=lambda r: r['id'])
                self.assertEqual(got, want,
                                 "pushed-down value filter must agree with post-merge oracle")


# ---------------------------------------------------------------------------
# Layer 3 — Property: random datasets + random predicates.
# ---------------------------------------------------------------------------
class PushdownPropertyTest(unittest.TestCase):
    """For N random (table, predicate) pairs, push-down result must equal
    the full-scan-then-Python-filter result.

    No hypothesis dependency — keeps Python 3.6 compat. Seeded for repro.
    """

    SEED = 0x70D0  # stable across runs; bump to flush flaky regressions.
    APPEND_TRIALS = 40
    PK_TRIALS = 30

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def setUp(self):
        # Fresh per-test RNG keeps each test method's dataset/predicate
        # sequence stable regardless of method execution order or future
        # additions in this class.
        self.rnd = random.Random(self.SEED)

    def _make_append_table(self, idx: int):
        pa_schema = pa.schema([
            pa.field('k', pa.int64(), nullable=False),
            ('v', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={'file.format': 'parquet', 'metadata.stats-mode': 'full'},
        )
        name = f'default.prop_append_{idx}'
        self.catalog.create_table(name, schema, False)
        return self.catalog.get_table(name)

    def _make_pk_table(self, idx: int):
        pa_schema = pa.schema([
            pa.field('k', pa.int64(), nullable=False),
            ('v', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['k'],
            options={'bucket': '1', 'file.format': 'parquet'},
        )
        name = f'default.prop_pk_{idx}'
        self.catalog.create_table(name, schema, False)
        return self.catalog.get_table(name)

    def _write_one_snapshot(self, table, rows: List[Dict]):
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            pa_schema = table.table_schema.to_arrow_schema() if hasattr(
                table.table_schema, 'to_arrow_schema') else None
            batch = pa.Table.from_pylist(rows, schema=pa_schema) \
                if pa_schema is not None else pa.Table.from_pylist(rows)
            w.write_arrow(batch)
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()

    def _read_with(self, table, predicate=None):
        rb = table.new_read_builder()
        if predicate is not None:
            rb = rb.with_filter(predicate)
        splits = rb.new_scan().plan().splits()
        if not splits:
            return []
        return rb.new_read().to_arrow(splits).to_pylist()

    def _gen_predicate(self, pb, field: str,
                       sample_values: List[int]) -> Tuple[Predicate, Any]:
        """Pick a random predicate on `field`. Returns (predicate, oracle).

        Operator set covers every method the property layer can sensibly
        exercise on numeric columns: equal/not_equal, full ordering family,
        between/not_between, in/not_in, is_null/is_not_null. The
        is_null path in particular re-covers a historical bug where
        missing null_counts in stats caused isNull to drop every file.
        """
        op = self.rnd.choice([
            'equal', 'not_equal',
            'less_than', 'less_or_equal', 'greater_than', 'greater_or_equal',
            'between', 'not_between',
            'is_in', 'is_not_in',
            'is_null', 'is_not_null',
        ])
        if op == 'equal':
            v = self.rnd.choice(sample_values)
            return pb.equal(field, v), lambda r: r[field] == v
        if op == 'not_equal':
            v = self.rnd.choice(sample_values)
            return pb.not_equal(field, v), lambda r: r[field] != v
        if op == 'less_than':
            v = self.rnd.choice(sample_values)
            return pb.less_than(field, v), lambda r: r[field] < v
        if op == 'less_or_equal':
            v = self.rnd.choice(sample_values)
            return pb.less_or_equal(field, v), lambda r: r[field] <= v
        if op == 'greater_than':
            v = self.rnd.choice(sample_values)
            return pb.greater_than(field, v), lambda r: r[field] > v
        if op == 'greater_or_equal':
            v = self.rnd.choice(sample_values)
            return pb.greater_or_equal(field, v), lambda r: r[field] >= v
        if op == 'between':
            a, b = sorted(self.rnd.sample(sample_values, 2))
            return (pb.between(field, a, b),
                    lambda r, a=a, b=b: a <= r[field] <= b)
        if op == 'not_between':
            a, b = sorted(self.rnd.sample(sample_values, 2))
            return (pb.not_between(field, a, b),
                    lambda r, a=a, b=b: not (a <= r[field] <= b))
        if op == 'is_in':
            n = self.rnd.randint(1, min(5, len(sample_values)))
            xs = self.rnd.sample(sample_values, n)
            return pb.is_in(field, xs), (lambda r, xs=set(xs): r[field] in xs)
        if op == 'is_not_in':
            n = self.rnd.randint(1, min(5, len(sample_values)))
            xs = self.rnd.sample(sample_values, n)
            return (pb.is_not_in(field, xs),
                    lambda r, xs=set(xs): r[field] not in xs)
        if op == 'is_null':
            # Our test data has no nulls, so the oracle is "no row matches"
            # — but the pushdown must arrive at the same answer without
            # incorrectly dropping non-null files (the historical bug).
            return pb.is_null(field), lambda r: False
        # is_not_null
        return pb.is_not_null(field), lambda r: True

    # -------------------------------------------------------------------
    # Append-only: oracle = identity over rows; push-down filter result
    # must equal Python-side filter over all rows.
    # -------------------------------------------------------------------
    def test_property_append_random(self):
        for trial in range(self.APPEND_TRIALS):
            table = self._make_append_table(trial)
            n = self.rnd.randint(20, 200)
            rows = [{'k': i, 'v': self.rnd.randint(-50, 50)} for i in range(n)]
            # 1-3 snapshots so we exercise multiple manifests.
            n_snaps = self.rnd.randint(1, 3)
            chunks = self._chunk(rows, n_snaps)
            for chunk in chunks:
                if chunk:
                    self._write_one_snapshot(table, chunk)

            pb = table.new_read_builder().new_predicate_builder()
            field = self.rnd.choice(['k', 'v'])
            sample = sorted({r[field] for r in rows})
            pred, oracle = self._gen_predicate(pb, field, sample)

            got = sorted(self._read_with(table, pred), key=lambda r: r['k'])
            want = sorted([r for r in rows if oracle(r)], key=lambda r: r['k'])
            self.assertEqual(got, want,
                             f"trial {trial} field={field} method={pred.method} mismatch")

    # -------------------------------------------------------------------
    # PK: oracle = post-merge state (latest write per PK wins). Random
    # multi-snapshot writes can update the same PK.
    # -------------------------------------------------------------------
    def test_property_pk_random(self):
        for trial in range(self.PK_TRIALS):
            table = self._make_pk_table(trial)
            n_snaps = self.rnd.randint(1, 3)
            merged: Dict[int, int] = {}
            for _ in range(n_snaps):
                m = self.rnd.randint(5, 20)
                # Within a single snapshot batch, Paimon's merge resolution
                # for duplicate PKs is not the "last-in-iteration" rule we
                # use as oracle. Avoid the ambiguity by guaranteeing PK
                # uniqueness inside each batch — the cross-snapshot update
                # semantics (the path we actually want to exercise) are
                # unaffected.
                ks = self.rnd.sample(range(40), min(m, 40))
                rows = []
                for k in ks:
                    v = self.rnd.randint(-50, 50)
                    rows.append({'k': k, 'v': v})
                    merged[k] = v  # latest snapshot wins
                self._write_one_snapshot(table, rows)

            pb = table.new_read_builder().new_predicate_builder()
            # PK predicates hit key_stats path; value predicates hit
            # post-merge filter path. Mix both.
            field = self.rnd.choice(['k', 'v'])
            if not merged:
                continue
            sample_values = sorted(merged.keys() if field == 'k'
                                   else set(merged.values()))
            if not sample_values:
                continue
            pred, oracle = self._gen_predicate(pb, field, sample_values)

            got = sorted(self._read_with(table, pred), key=lambda r: r['k'])
            want = sorted(
                [{'k': k, 'v': v} for k, v in merged.items()
                 if oracle({'k': k, 'v': v})],
                key=lambda r: r['k'])
            self.assertEqual(got, want,
                             f"trial {trial} field={field} method={pred.method} mismatch "
                             f"merged={merged}")

    @staticmethod
    def _chunk(items: List, n: int) -> List[List]:
        """Split into n roughly-equal chunks."""
        n = max(1, n)
        size = max(1, (len(items) + n - 1) // n)
        return [items[i:i + size] for i in range(0, len(items), size)]


if __name__ == '__main__':
    unittest.main()
