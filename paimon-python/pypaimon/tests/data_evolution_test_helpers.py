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

"""Helpers shared between the test suites that exercise
``StreamTableUpdate`` / ``BatchTableUpdate``
(``table_update_test.py``, ``table_upsert_by_key_test.py``).

Two distinct contracts are factored out here:

1. :class:`DataEvolutionTestBase` — table lifecycle, schema, and helpers
   common to every data-evolution table-modification test.
2. :class:`BatchModeMixin` / :class:`StreamModeMixin` — the
   write-builder-agnostic primitives that drive a test either through
   ``BatchWriteBuilder`` or ``StreamWriteBuilder``. Each suite only has
   to add its own one-line API-call primitive (e.g. ``_apply_update``).
3. :class:`StreamSnapshotAssertions` — snapshot-level assertions plus
   :meth:`StreamSnapshotAssertions._stream_commit_session` for opening a
   reusable ``StreamWriteBuilder`` / ``StreamTableCommit`` pair (shared by
   ``table_update_test`` / ``table_upsert_by_key_test`` stream-only cases).
"""

import itertools
import os
import shutil
import tempfile
import threading
import uuid

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


# ======================================================================
# Snapshot assertions for stream-mode tests
# ======================================================================

class StreamSnapshotAssertions:
    """Mixin providing snapshot-level assertions for stream-mode tests.

    Designed to be mixed into a :class:`StreamModeMixin` whose concrete test
    classes also inherit from :class:`unittest.TestCase` (so ``self`` exposes
    ``assertEqual``).

    The single contract these assertions enforce is the one that
    distinguishes stream mode from batch mode: every commit lands on its
    own snapshot tagged with the caller-supplied ``commit_identifier``
    under a stable ``commit_user``.

    Tests that drive a single :class:`StreamWriteBuilder` should prefer
    :meth:`_stream_commit_session` plus :meth:`_assert_stream_builder_snapshots`
    over repeating ``new_stream_write_builder`` /
    ``_latest_snapshot_id`` boilerplate.
    """

    @staticmethod
    def _latest_snapshot_id(table) -> int:
        """Latest snapshot id, or ``0`` for an empty table."""
        latest = table.snapshot_manager().get_latest_snapshot()
        return latest.id if latest is not None else 0

    def _stream_commit_session(self, table):
        """Open one ``StreamWriteBuilder`` + ``StreamTableCommit`` pair.

        Returns ``(write_builder, commit, base_snapshot_id)``. Caller must
        invoke ``commit.close()`` after finishing commits.
        """
        base_snapshot_id = self._latest_snapshot_id(table)
        wb = table.new_stream_write_builder()
        tc = wb.new_commit()
        return wb, tc, base_snapshot_id

    def _assert_snapshots_carry(
            self, table, base_snapshot_id, commit_user, commit_identifiers,
    ):
        """Assert that the snapshots produced after ``base_snapshot_id`` are
        tagged with the given ``commit_identifiers`` in order, all under
        ``commit_user``.

        Asserted snapshot-by-snapshot so failures pinpoint the exact
        mismatching snapshot id.
        """
        snap_mgr = table.snapshot_manager()
        for offset, expected_cid in enumerate(commit_identifiers, start=1):
            snap_id = base_snapshot_id + offset
            snap = snap_mgr.get_snapshot_by_id(snap_id)
            self.assertEqual(
                (commit_user, expected_cid),
                (snap.commit_user, snap.commit_identifier),
                msg=f"snapshot {snap_id}: expected "
                    f"(user={commit_user!r}, identifier={expected_cid})",
            )

    def _assert_stream_builder_snapshots(
            self, table, write_builder, base_snapshot_id, commit_identifiers,
    ):
        """Assert snapshots after ``base_snapshot_id`` carry
        ``commit_identifiers`` in order under ``write_builder.commit_user``.

        Convenience wrapper so stream tests do not repeat
        ``write_builder.commit_user`` at every call site.
        """
        self._assert_snapshots_carry(
            table,
            base_snapshot_id,
            write_builder.commit_user,
            commit_identifiers,
        )


# ======================================================================
# Shared test-class base
# ======================================================================

class DataEvolutionTestBase:
    """Shared lifecycle, schema, and helpers for data-evolution
    table-modification tests (update-by-row-id, upsert-by-key).

    Concrete subclasses must also inherit from :class:`unittest.TestCase`
    *and* one of :class:`BatchModeMixin` / :class:`StreamModeMixin`, which
    provide the mode-specific write/commit primitives.
    """

    # The canonical 4-column schema used by both suites.
    pa_schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
        ('age', pa.int32()),
        ('city', pa.string()),
    ])

    table_options = {
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
    }

    # ------------------------------------------------------------------
    # Class-level fixtures
    # ------------------------------------------------------------------

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Common helpers
    # ------------------------------------------------------------------

    def _unique_name(self, prefix='tbl'):
        return f'default.{prefix}_{uuid.uuid4().hex[:8]}'

    def _create_table(self, pa_schema=None, partition_keys=None, options=None):
        pa_schema = pa_schema if pa_schema is not None else self.pa_schema
        opts = options if options is not None else self.table_options
        s = Schema.from_pyarrow_schema(
            pa_schema, partition_keys=partition_keys, options=opts
        )
        name = self._unique_name()
        self.catalog.create_table(name, s, False)
        return self.catalog.get_table(name)

    def _write_arrow(self, table, data):
        """Write a single Arrow table and commit as one snapshot."""
        wb = self._make_write_builder(table)
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(data)
        cid = self._next_commit_id()
        msgs = self._prepare_write_commit(tw, cid)
        self._apply_commit(tc, msgs, cid)
        tw.close()
        tc.close()

    def _read_all(self, table):
        rb = table.new_read_builder()
        return rb.new_read().to_arrow(rb.new_scan().plan().splits())

    # ------------------------------------------------------------------
    # Mode-specific primitives (overridden by mixins)
    # ------------------------------------------------------------------

    def _make_write_builder(self, table):
        raise NotImplementedError

    def _next_commit_id(self):
        """Next ``commit_identifier`` for stream mode, or ``None`` for batch."""
        raise NotImplementedError

    def _prepare_write_commit(self, table_write, cid):
        """Invoke ``table_write.prepare_commit`` with the appropriate
        signature for this mode (stream takes ``cid``; batch takes nothing)."""
        raise NotImplementedError

    def _apply_commit(self, commit, msgs, cid):
        raise NotImplementedError


# ======================================================================
# Mode-specific mixins (write-builder + commit framework only)
# ======================================================================

class BatchModeMixin:
    """Drive shared tests through ``BatchWriteBuilder``.

    Each suite layers its own one-line API-call primitive on top
    (e.g. ``_apply_update``, ``_apply_upsert``).
    """

    def _make_write_builder(self, table):
        return table.new_batch_write_builder()

    def _next_commit_id(self):
        return None  # batch APIs do not take a commit_identifier

    def _prepare_write_commit(self, table_write, cid):
        return table_write.prepare_commit()

    def _apply_commit(self, commit, msgs, cid):
        commit.commit(msgs)


class StreamModeMixin(StreamSnapshotAssertions):
    """Drive shared tests through ``StreamWriteBuilder``.

    Owns a thread-safe monotonically-increasing counter so concurrent
    tests still get unique, well-ordered ``commit_identifier`` values.
    Inherits the stream-mode snapshot assertions from
    :class:`StreamSnapshotAssertions`.
    """

    def setUp(self):
        super().setUp()
        self._stream_id_counter = itertools.count(1)
        self._stream_id_lock = threading.Lock()

    def _make_write_builder(self, table):
        return table.new_stream_write_builder()

    def _next_commit_id(self):
        with self._stream_id_lock:
            return next(self._stream_id_counter)

    def _prepare_write_commit(self, table_write, cid):
        return table_write.prepare_commit(cid)

    def _apply_commit(self, commit, msgs, cid):
        commit.commit(msgs, cid)
