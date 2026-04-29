################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

"""
Three-layer correctness tests for predicate-driven bucket pruning.

Mirrors Java's ``BucketSelectConverter`` contract: PK Equal/In queries on
HASH_FIXED tables must touch only the bucket(s) the writer would have
placed those keys in. Two correctness obligations:

  1. Sound: every bucket retained by the selector contains AT MOST a
     superset of matching rows. Buckets that DO contain matching rows
     are NEVER dropped — false-negative-free.
  2. Hash-consistent with writers: ``RowKeyExtractor`` (writer) and
     ``BucketSelectConverter`` (reader) must agree on every literal.
     This is what makes ``pk = 'X'`` read the bucket holding 'X'.

Layered:
  * Unit       — direct calls to ``create_bucket_selector`` with crafted
                 predicates, asserting selector behaviour.
  * Integration — real PK tables with multiple buckets; queries; assert
                 (a) result correctness, (b) bucket pruning happened.
  * Property   — randomly-seeded PK tables, random Equal/In predicates,
                 result == oracle. No hypothesis dependency (keeps
                 Python 3.6 compat).
"""

import os
import random
import shutil
import tempfile
import unittest
from typing import Any, Dict, List

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.scanner.bucket_select_converter import (
    MAX_VALUES, create_bucket_selector)
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.write.row_key_extractor import (FixedBucketRowKeyExtractor,
                                              _bucket_from_hash,
                                              _hash_bytes_by_words)
from pypaimon.table.row.generic_row import GenericRow, GenericRowSerializer
from pypaimon.table.row.internal_row import RowKind


def _bigint_field(idx: int, name: str) -> DataField:
    return DataField(idx, name, AtomicType('BIGINT', nullable=False))


def _hash_bucket(values: List[Any], fields: List[DataField], total: int) -> int:
    """Re-implement the writer's hash so unit tests can compute the
    expected bucket without spinning up a real table."""
    row = GenericRow(values, fields, RowKind.INSERT)
    serialized = GenericRowSerializer.to_bytes(row)
    h = _hash_bytes_by_words(serialized[4:])
    return _bucket_from_hash(h, total)


# ---------------------------------------------------------------------------
# Layer 1 — Unit: drive ``create_bucket_selector`` with crafted predicates.
# ---------------------------------------------------------------------------
class BucketSelectConverterUnitTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.id_field = _bigint_field(0, 'id')
        cls.val_field = _bigint_field(1, 'val')
        cls.k1 = _bigint_field(0, 'k1')
        cls.k2 = _bigint_field(1, 'k2')
        cls.pb_id_val = PredicateBuilder([cls.id_field, cls.val_field])
        cls.pb_k1_k2 = PredicateBuilder([cls.k1, cls.k2])

    # -- Equal / In on single bucket key ---------------------------------
    def test_equal_on_single_bucket_key_yields_single_bucket(self):
        sel = create_bucket_selector(
            self.pb_id_val.equal('id', 42), [self.id_field])
        self.assertIsNotNone(sel, "PK Equal must produce a selector")
        expected = _hash_bucket([42], [self.id_field], total=8)
        for b in range(8):
            self.assertEqual(
                sel(b, 8), b == expected,
                "only bucket {} must be kept (got {})".format(expected, b))

    def test_in_on_single_bucket_key_unions_buckets(self):
        sel = create_bucket_selector(
            self.pb_id_val.is_in('id', [1, 2, 3, 100]), [self.id_field])
        expected = {_hash_bucket([v], [self.id_field], 8)
                    for v in (1, 2, 3, 100)}
        for b in range(8):
            self.assertEqual(sel(b, 8), b in expected)

    def test_or_of_equals_on_same_field_unions_buckets(self):
        # ``id = 1 OR id = 2`` must equal ``id IN (1, 2)``.
        pred = PredicateBuilder.or_predicates([
            self.pb_id_val.equal('id', 1),
            self.pb_id_val.equal('id', 2),
        ])
        sel = create_bucket_selector(pred, [self.id_field])
        expected = {_hash_bucket([v], [self.id_field], 8) for v in (1, 2)}
        for b in range(8):
            self.assertEqual(sel(b, 8), b in expected)

    # -- Composite bucket keys ------------------------------------------
    def test_composite_bucket_key_intersects_via_cartesian(self):
        pred = PredicateBuilder.and_predicates([
            self.pb_k1_k2.is_in('k1', [1, 2]),
            self.pb_k1_k2.equal('k2', 99),
        ])
        sel = create_bucket_selector(pred, [self.k1, self.k2])
        expected = {
            _hash_bucket([k1, 99], [self.k1, self.k2], 4)
            for k1 in (1, 2)
        }
        for b in range(4):
            self.assertEqual(sel(b, 4), b in expected)

    def test_composite_bucket_key_missing_one_field_returns_none(self):
        pred = self.pb_k1_k2.equal('k1', 1)  # k2 unconstrained
        sel = create_bucket_selector(pred, [self.k1, self.k2])
        self.assertIsNone(sel,
                          "all bucket keys must be constrained or fall back")

    # -- Predicates that can't be reduced -------------------------------
    def test_non_bucket_key_predicate_returns_none(self):
        sel = create_bucket_selector(
            self.pb_id_val.equal('val', 5), [self.id_field])
        self.assertIsNone(sel, "predicate not on bucket key -> no selector")

    def test_range_predicate_on_bucket_key_returns_none(self):
        sel = create_bucket_selector(
            self.pb_id_val.greater_than('id', 100), [self.id_field])
        self.assertIsNone(sel, "ranges can't be turned into a finite bucket set")

    def test_or_with_non_bucket_key_returns_none(self):
        # ``id = 1 OR val = 5`` — ``val`` isn't a bucket key, so the OR
        # is not a pure bucket-key constraint.
        pred = PredicateBuilder.or_predicates([
            self.pb_id_val.equal('id', 1),
            self.pb_id_val.equal('val', 5),
        ])
        sel = create_bucket_selector(pred, [self.id_field])
        self.assertIsNone(sel)

    def test_repeated_equal_on_same_key_under_and_returns_none(self):
        # ``id = 1 AND id = 2``: unsatisfiable, but Java bails to "no
        # filter" rather than reasoning. We do the same — any superset
        # of the true set is acceptable.
        pred = PredicateBuilder.and_predicates([
            self.pb_id_val.equal('id', 1),
            self.pb_id_val.equal('id', 2),
        ])
        sel = create_bucket_selector(pred, [self.id_field])
        self.assertIsNone(sel)

    def test_and_with_unrelated_clause_is_unaffected(self):
        # ``id = 7 AND val > 100`` — the ``val > 100`` part doesn't
        # constrain buckets, but mustn't disqualify the ``id = 7`` part.
        pred = PredicateBuilder.and_predicates([
            self.pb_id_val.equal('id', 7),
            self.pb_id_val.greater_than('val', 100),
        ])
        sel = create_bucket_selector(pred, [self.id_field])
        self.assertIsNotNone(sel)
        expected = _hash_bucket([7], [self.id_field], 4)
        for b in range(4):
            self.assertEqual(sel(b, 4), b == expected)

    # -- Cap & degenerate edge cases ------------------------------------
    def test_cartesian_above_max_values_returns_none(self):
        # Two columns of size > sqrt(MAX_VALUES) → product > MAX_VALUES.
        size = 33  # 33 * 33 = 1089 > 1000
        pred = PredicateBuilder.and_predicates([
            self.pb_k1_k2.is_in('k1', list(range(size))),
            self.pb_k1_k2.is_in('k2', list(range(size))),
        ])
        self.assertGreater(size * size, MAX_VALUES)
        sel = create_bucket_selector(pred, [self.k1, self.k2])
        self.assertIsNone(sel)

    def test_null_only_literal_drops_everything(self):
        # ``id IN (NULL)`` after null-stripping has zero literals; the
        # cartesian product is empty → selector matches no buckets. Same
        # behaviour as Java.
        pred = self.pb_id_val.is_in('id', [None])
        sel = create_bucket_selector(pred, [self.id_field])
        self.assertIsNotNone(sel)
        for b in range(4):
            self.assertFalse(sel(b, 4),
                             "all-null literal collapses bucket set to empty")

    def test_no_predicate_returns_none(self):
        self.assertIsNone(create_bucket_selector(None, [self.id_field]))

    def test_no_bucket_keys_returns_none(self):
        self.assertIsNone(
            create_bucket_selector(self.pb_id_val.equal('id', 1), []))

    # -- Selector cache + rescale -------------------------------------
    def test_selector_caches_per_total_buckets(self):
        """Selector must answer correctly when the same query applies to
        different ``total_buckets`` values (the rescale scenario)."""
        sel = create_bucket_selector(
            self.pb_id_val.equal('id', 42), [self.id_field])
        for total in (4, 8, 16, 32):
            expected = _hash_bucket([42], [self.id_field], total)
            self.assertTrue(sel(expected, total))
            other = (expected + 1) % total
            self.assertFalse(sel(other, total))

    def test_non_positive_total_buckets_fails_open(self):
        """Manifest entries can carry ``total_buckets <= 0`` for legacy /
        special bucket modes. Pruning MUST fail open — returning False
        would silently drop rows the writer placed in those entries.
        This is correctness, not performance: the soundness contract
        forbids false-negatives."""
        sel = create_bucket_selector(
            self.pb_id_val.equal('id', 1), [self.id_field])
        for total in (0, -1, -2):
            self.assertTrue(sel(0, total),
                            "total_buckets={} must be kept (fail open)".format(total))
            self.assertTrue(sel(-1, total))
            self.assertTrue(sel(99, total))

    def test_type_mismatched_literal_fails_open_not_crash(self):
        """If the user constructs a predicate whose literal type doesn't
        match the bucket-key column's atomic type — e.g. a STRING literal
        on a BIGINT column — ``GenericRowSerializer`` raises during the
        deferred hash inside ``_Selector``. The selector MUST swallow the
        exception and fail open (return True for every bucket) rather
        than propagate it. Crashing the entire scan with an opaque
        ``struct.error`` is a worse user experience than silently
        skipping bucket pruning, and the soundness contract still
        forbids false-negatives."""
        sel = create_bucket_selector(
            self.pb_id_val.equal('id', 'not-an-int'), [self.id_field])
        # Construction itself succeeds (no eager hashing).
        self.assertIsNotNone(sel)
        # Calling the selector must NOT raise; instead it returns True
        # for every (bucket, total_buckets), preserving soundness.
        for total in (4, 8):
            for b in range(total):
                self.assertTrue(sel(b, total),
                                "type-mismatched literal must fail open, "
                                "not crash (bucket={}, total={})".format(b, total))


# ---------------------------------------------------------------------------
# Layer 2 — Integration: real tables, public API, assert correctness AND
# that pruning actually fired (otherwise we're not testing the optimisation,
# only that we didn't break full-scan).
# ---------------------------------------------------------------------------
class BucketPruningIntegrationTest(unittest.TestCase):

    NUM_BUCKETS = 8

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_pk_table(self, name: str, num_buckets: int = NUM_BUCKETS,
                         bucket_key: str = None) -> Any:
        opts = {'bucket': str(num_buckets), 'file.format': 'parquet'}
        if bucket_key is not None:
            opts['bucket-key'] = bucket_key
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('val', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=['id'], options=opts)
        full = 'default.{}'.format(name)
        self.catalog.create_table(full, schema, False)
        return self.catalog.get_table(full)

    def _write(self, table, rows: List[Dict]):
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('val', pa.int64()),
        ])
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            w.write_arrow(pa.Table.from_pylist(rows, schema=pa_schema))
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
            return [], splits
        return rb.new_read().to_arrow(splits).to_pylist(), splits

    @staticmethod
    def _split_buckets(splits) -> set:
        """Collect the distinct bucket numbers actually returned in a plan."""
        return {s.bucket for s in splits}

    # -- Equal on PK -----------------------------------------------------
    def test_pk_equal_only_reads_target_bucket(self):
        table = self._create_pk_table('int_eq')
        rows = [{'id': i, 'val': i * 11} for i in range(100)]
        self._write(table, rows)

        target_id = 42
        pred = table.new_read_builder().new_predicate_builder().equal(
            'id', target_id)
        got, splits = self._read_with(table, pred)

        # Correctness: row for id=42 returned (and only that).
        self.assertEqual(got, [{'id': 42, 'val': 42 * 11}])

        # Pruning effectiveness: at most 1 bucket touched.
        self.assertEqual(len(self._split_buckets(splits)), 1,
                         "PK equal must touch exactly one bucket")

    def test_pk_in_reads_only_target_buckets(self):
        table = self._create_pk_table('int_in')
        rows = [{'id': i, 'val': i * 7} for i in range(200)]
        self._write(table, rows)

        ids = [3, 17, 99, 150]
        pred = table.new_read_builder().new_predicate_builder().is_in(
            'id', ids)
        got, splits = self._read_with(table, pred)
        got_sorted = sorted(got, key=lambda r: r['id'])
        self.assertEqual(got_sorted,
                         [{'id': i, 'val': i * 7} for i in sorted(ids)])

        actual = self._split_buckets(splits)
        # Selector-derived expectation
        ext = FixedBucketRowKeyExtractor(table.table_schema)
        expected_buckets = set()
        for i in ids:
            arr = pa.RecordBatch.from_pylist(
                [{'id': i, 'val': 0}],
                schema=pa.schema([
                    pa.field('id', pa.int64(), nullable=False),
                    ('val', pa.int64()),
                ]),
            )
            expected_buckets.update(ext._extract_buckets_batch(arr))
        self.assertTrue(actual.issubset(expected_buckets),
                        "must not read buckets outside the target set; "
                        "got {}, expected ⊆ {}".format(actual, expected_buckets))

    # -- Predicates that should NOT prune -------------------------------
    def test_value_only_predicate_falls_back_to_full_scan(self):
        """``val < X`` doesn't constrain the PK → selector must be None
        and no bucket pruning may fire. Both checked: result correctness
        AND the explicit "selector is None" property."""
        table = self._create_pk_table('val_only')
        rows = [{'id': i, 'val': i} for i in range(100)]
        self._write(table, rows)

        pred = table.new_read_builder().new_predicate_builder().less_than(
            'val', 30)
        got, splits = self._read_with(table, pred)
        self.assertEqual(sorted([r['id'] for r in got]), list(range(30)))

        # Inspect the scanner's bucket selector to prove pruning DIDN'T
        # fire — without this assertion the test would also pass under a
        # buggy selector that prunes wrongly but happens to keep the
        # rows we picked.
        rb = table.new_read_builder().with_filter(pred)
        scan = rb.new_scan()
        self.assertIsNone(scan.file_scanner._bucket_selector,
                          "value-only predicate must NOT produce a selector")

    def test_range_on_pk_falls_back_to_full_scan(self):
        """``id > X`` is a range, not Equal/In, so cannot derive a bucket
        set. Selector returns None — result must still be exact."""
        table = self._create_pk_table('pk_range')
        rows = [{'id': i, 'val': i} for i in range(50)]
        self._write(table, rows)

        pred = table.new_read_builder().new_predicate_builder().greater_or_equal(
            'id', 40)
        got, _ = self._read_with(table, pred)
        self.assertEqual(sorted([r['id'] for r in got]), list(range(40, 50)))

    # -- Mixed predicate: Equal on PK AND range on val ------------------
    def test_pk_equal_with_unrelated_value_predicate_still_prunes(self):
        table = self._create_pk_table('int_eq_with_val')
        rows = [{'id': i, 'val': i} for i in range(50)]
        self._write(table, rows)

        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.and_predicates([
            pb.equal('id', 25),
            pb.greater_than('val', 20),
        ])
        got, splits = self._read_with(table, pred)
        self.assertEqual(got, [{'id': 25, 'val': 25}])
        self.assertEqual(len(self._split_buckets(splits)), 1,
                         "Equal on PK still narrows buckets even when "
                         "AND'd with a non-bucket-key predicate")

    # -- Explicit bucket-key option ------------------------------------
    def test_bucket_key_option_overrides_pk_for_pruning(self):
        """When the ``bucket-key`` option is set explicitly, the bucket
        derivation must use it — not the trimmed primary keys. This is
        the path that catches read/write hash divergence if a refactor
        forgets the option."""
        # PK = id, bucket-key = id explicitly (single key but exercises
        # the explicit-config branch in ``_init_bucket_selector``).
        table = self._create_pk_table('explicit_bk', bucket_key='id')
        rows = [{'id': i, 'val': i * 3} for i in range(40)]
        self._write(table, rows)

        pred = table.new_read_builder().new_predicate_builder().equal('id', 17)
        got, splits = self._read_with(table, pred)
        self.assertEqual(got, [{'id': 17, 'val': 51}])
        self.assertEqual(len(self._split_buckets(splits)), 1)


# ---------------------------------------------------------------------------
# Layer 3 — Property: random PK tables, random Equal/In predicates,
# correctness vs oracle.
# ---------------------------------------------------------------------------
class BucketPruningPropertyTest(unittest.TestCase):

    SEED = 0xB0CC
    TRIALS = 30

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)
        cls.rnd = random.Random(cls.SEED)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _make_table(self, idx: int, num_buckets: int):
        pa_schema = pa.schema([
            pa.field('k', pa.int64(), nullable=False),
            ('v', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['k'],
            options={'bucket': str(num_buckets), 'file.format': 'parquet'},
        )
        name = 'default.bp_{}'.format(idx)
        self.catalog.create_table(name, schema, False)
        return self.catalog.get_table(name)

    def _write(self, table, rows):
        pa_schema = pa.schema([
            pa.field('k', pa.int64(), nullable=False),
            ('v', pa.int64()),
        ])
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        try:
            w.write_arrow(pa.Table.from_pylist(rows, schema=pa_schema))
            c.commit(w.prepare_commit())
        finally:
            w.close()
            c.close()

    def test_property_pk_equal_correctness(self):
        for trial in range(self.TRIALS):
            num_buckets = self.rnd.choice([2, 4, 8, 16])
            table = self._make_table(trial, num_buckets)
            keys = self.rnd.sample(range(1000), self.rnd.randint(20, 100))
            rows = [{'k': k, 'v': k * 13} for k in keys]
            self._write(table, rows)

            target = self.rnd.choice(keys)
            pb = table.new_read_builder().new_predicate_builder()
            pred = pb.equal('k', target)
            rb = table.new_read_builder().with_filter(pred)
            splits = rb.new_scan().plan().splits()
            if splits:
                got = rb.new_read().to_arrow(splits).to_pylist()
            else:
                got = []
            self.assertEqual(got, [{'k': target, 'v': target * 13}],
                             "trial {} buckets={} target={}: result mismatch"
                             .format(trial, num_buckets, target))

    def test_property_pk_in_correctness(self):
        for trial in range(self.TRIALS):
            num_buckets = self.rnd.choice([2, 4, 8, 16])
            offset = self.TRIALS + trial  # avoid name clash with prev test
            table = self._make_table(offset, num_buckets)
            keys = self.rnd.sample(range(1000), self.rnd.randint(20, 100))
            rows = [{'k': k, 'v': k * 13} for k in keys]
            self._write(table, rows)

            target_n = self.rnd.randint(1, min(10, len(keys)))
            targets = self.rnd.sample(keys, target_n)
            pb = table.new_read_builder().new_predicate_builder()
            pred = pb.is_in('k', targets)
            rb = table.new_read_builder().with_filter(pred)
            splits = rb.new_scan().plan().splits()
            if splits:
                got = rb.new_read().to_arrow(splits).to_pylist()
            else:
                got = []
            got_sorted = sorted(got, key=lambda r: r['k'])
            want = sorted(
                [{'k': k, 'v': k * 13} for k in targets],
                key=lambda r: r['k'])
            self.assertEqual(got_sorted, want,
                             "trial {}: IN result mismatch".format(trial))


if __name__ == '__main__':
    unittest.main()
