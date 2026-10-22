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

"""Preparation selection for reading and planning; execution is never replayed."""

import logging
from typing import Iterator, List, Optional, Tuple

import pyarrow

from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.read.plan import Plan
from pypaimon.read.split import Split
from pypaimon.read.scan_stats import ScanStats


def validate_read_backend(read):
    """Validate the explicitly selected Python backend without loading Native."""
    if not read.table.options.native_read_enabled():
        from pypaimon.execution.python.read.merge_engine_support import validate_read
        validate_read(read)


def to_iterator(self, splits: List[Split]) -> Iterator:
    """Lazily read InternalRows, using native reads when available.

    Rows retain their kind separately from projected fields. Like the
    Python split reader, the iterator reuses its row view and visits splits
    in order. Close the iterator when stopping before exhaustion.
    """
    if self.output_projection is not None and self.output_projection.named:
        raise RuntimeError(
            "Projection expressions are not supported by to_iterator(); "
            "use to_arrow() or to_arrow_batch_reader()")
    self._begin_auth_read(splits)
    limit = self.limit

    def _record_generator():
        if limit is not None and limit <= 0:
            return
        rows = self._try_native_rows(splits)
        if rows is not None:
            try:
                yield from rows
            finally:
                rows.close()
            return

        from pypaimon.execution.python.read.table_read import PythonTableRead
        yield from PythonTableRead(self).read_rows(splits)

    return _record_generator()


def _new_arrow_batch_reader(
        self,
        splits: List[Split],
        blob_parallelism: Optional[int] = None,
        parallelism: Optional[int] = None):
    schema = self._output_arrow_schema(splits)
    if self.include_row_kind:
        schema = self._add_row_kind_to_schema(schema)
    effective = self._resolve_parallelism(parallelism, len(splits))
    effective_bp = self._resolve_blob_parallelism(blob_parallelism)
    native_batches = self._try_native_batches(
        splits,
        schema,
        parallelism=effective,
        blob_parallelism=effective_bp,
        ordered=False,
    )
    if native_batches is not None:
        batch_iterator = iter(native_batches)
    else:
        from pypaimon.execution.python.read.table_read import PythonTableRead
        batch_iterator = PythonTableRead(self).read_batches(
            splits, schema, parallelism, effective_bp)
    reader_type = pyarrow.ipc.RecordBatchReader
    reader = reader_type.from_batches(schema, batch_iterator)
    return reader, batch_iterator


def to_arrow(
    self,
    splits: List[Split],
    parallelism: Optional[int] = None,
    blob_parallelism: Optional[int] = None,
) -> Optional[pyarrow.Table]:
    """Read ``splits`` into a single arrow ``Table``.

    Args:
        splits: scan-plan splits returned from a ``TableScan``.
        parallelism: optional runtime override of the
            ``read.parallelism`` table option. ``None`` (default) falls
            back to the table option; when that is also unset the read
            auto-scales to ``min(number of splits, CPU count)``. ``1``
            keeps reads serial; ``>= 2`` caps the thread pool that reads
            byte-balanced contiguous split groups concurrently and
            assembles the final table in input order. An unfiltered row
            limit caps this further to avoid speculative work. Must be
            ``>= 1``. Note that with ``>= 2`` (or auto)
            and a ``limit`` set, the returned rows are an arbitrary
            subset of the requested size, since which splits fill the row
            quota first is non-deterministic. Reads with BLOB payloads run
            serially when a limit may discard rows, so discarded payloads
            are not fetched by speculative split workers.
        blob_parallelism: maximum concurrent blob range reads within each
            split reader. ``None`` or ``1`` (default) reads blobs serially;
            ``>= 2`` enables concurrent ranged reads. On the
            parallel path, peak blob threads (``parallelism`` *
            ``blob_parallelism``) are capped at
            ``_MAX_TOTAL_BLOB_WORKERS``; per-split ``blob_parallelism`` is
            shrunk to stay within it.
    """
    effective_bp = self._resolve_blob_parallelism(blob_parallelism)
    effective = self._resolve_parallelism(parallelism, len(splits))
    schema = self._output_arrow_schema(splits)
    if self.include_row_kind:
        schema = self._add_row_kind_to_schema(schema)

    native_batches = self._try_native_batches(
        splits,
        schema,
        parallelism=effective,
        blob_parallelism=effective_bp,
        ordered=True,
    )
    if native_batches is not None:
        return self._batches_to_arrow(native_batches, schema)

    from pypaimon.execution.python.read.table_read import PythonTableRead
    return PythonTableRead(self)._materialize(splits, schema, parallelism, effective_bp)


def plan_scan(self) -> Plan:
    native_auth = (self.table.options.query_auth_enabled
                   and self._native_requested() and self._native_plan_supported())
    if native_auth:
        # Rust asks the REST server and carries its complete response in
        # QueryAuthSplit. Both readers then enforce that captured policy.
        native = self._try_native_plan()
        if native is not None:
            return native
    auth_result = self._TableScan__auth_query()
    if self._query_auth_fn is not None and self._file_scanner is not None:
        from pypaimon.execution.python.read.auth_support import prune_scanner_by_auth
        prune_scanner_by_auth(self.table, self._file_scanner, auth_result)
    # The native planner bypasses the auth-aware file scanner. Resolve auth
    # before selecting a planning backend.
    if (not native_auth and auth_result is None and self._native_requested()
            and self._native_plan_supported()):
        native = self._try_native_plan()
        if native is not None:
            return native
    from pypaimon.execution.python.read.table_scan import PythonTableScan
    return PythonTableScan(self).plan(auth_result)


def plan_all_files(self) -> Plan:
    """Plan matching files including L0 and all data-evolution column files.

    Existing filters and limits still apply. Query authorization must be
    unrestricted, as required by writer restore, overwrite and indexing.
    """
    if self._native_requested():
        from pypaimon.execution.native.table import _native_query_auth_supported
        native_supported = _native_query_auth_supported(self.table) and self._native_plan_supported()
    else:
        native_supported = False
    if native_supported:
        plan = self._try_native_plan(all_files=True)
        if plan is not None:
            return plan
    if self._TableScan__auth_query(all_columns=True) is not None:
        raise TableNoPermissionException(self.table.identifier)
    from pypaimon.execution.python.read.table_scan import PythonTableScan
    return PythonTableScan(self).plan_all_files()


def scan_with_stats(self) -> Tuple[Plan, Optional[ScanStats]]:
    """Run :meth:`plan` while recording manifest / pruning counters.

    Only used by :meth:`ReadBuilder.explain`; the regular read path
    keeps going through :meth:`plan`. Native scans always collect lightweight
    counters in the same core planning pass, without opening data files.
    """
    native_auth = (self.table.options.query_auth_enabled
                   and self._native_requested() and self._native_plan_supported())
    if native_auth:
        from pypaimon.execution.native.read.plan import NativeTableScan
        native = NativeTableScan(self).scan_with_stats()
        if native is not None:
            return native
    auth_result = self._TableScan__auth_query()
    if self._query_auth_fn is not None and self._file_scanner is not None:
        from pypaimon.execution.python.read.auth_support import prune_scanner_by_auth
        prune_scanner_by_auth(self.table, self._file_scanner, auth_result)
    if (not native_auth and auth_result is None and self._native_requested()
            and self._native_plan_supported()):
        from pypaimon.execution.native.read.plan import NativeTableScan
        native = NativeTableScan(self).scan_with_stats()
        if native is not None:
            return native
    from pypaimon.execution.python.read.table_scan import PythonTableScan
    return PythonTableScan(self).scan_with_stats(auth_result)


def new_streaming_scan(self):
    """Create a stateful streaming scan using the configured backend."""
    from pypaimon.write.backend_dispatch import propagate_fork_safety_error

    projection = self._projection_builder()
    projection._validate_map_key_filter()
    if native_plan_requested(self):
        from pypaimon.execution.native.table import _native_query_auth_supported
        native_supported = _native_query_auth_supported(self.table)
    else:
        native_supported = False
    if native_supported:
        try:
            from pypaimon.execution.native.read.streaming_table_scan import StreamTableScan
            return StreamTableScan(
                self.table, predicate=self._predicate, read_type=projection.read_type(),
                poll_interval_ms=self._poll_interval_ms, bucket_filter=self._bucket_filter,
                consumer_id=self._consumer_id)
        except Exception as error:
            propagate_fork_safety_error(error)
            logging.warning("Cannot create Native stream scan, using Python: %s", error)
    from pypaimon.execution.python.read.streaming_table_scan import AsyncStreamingTableScan
    scan = AsyncStreamingTableScan(
        table=self.table,
        predicate=self._predicate,
        poll_interval_ms=self._poll_interval_ms,
        bucket_filter=self._bucket_filter,
        consumer_id=self._consumer_id
    )
    scan._read_type = projection.read_type()
    return scan


def native_plan_requested(scan) -> bool:
    """Resolve Native planning preference without loading a disabled backend."""
    if scan.table.options.native_plan_enabled():
        return True
    if not scan.table.options.native_read_enabled():
        return False
    from pypaimon.execution.native.runtime import native_runtime_available
    return native_runtime_available()


def native_read_fallback(read, reason, error=None, log=False):
    """Validate features that cannot fall back before importing either executor."""
    from pypaimon.read.variant_read_type import has_variant_extractions
    if has_variant_extractions(read.read_type):
        raise RuntimeError(
            "Variant extraction requires a compatible native reader: %s" % reason) from error
    if log:
        logging.getLogger('pypaimon.execution.native.read.table_read').warning(
            'Native read failed, falling back to the Python reader: %s', reason)
    return None


def plan_system_table(scan) -> Plan:
    """Choose a metadata provider before importing its read implementation."""
    table = scan.system_table
    options = getattr(getattr(table, 'base_table', None), 'options', None)
    if options is not None and options.native_read_enabled():
        from pypaimon.execution.native.read.system_table import try_read_native_system_table
        arrow_table = try_read_native_system_table(table)
        if arrow_table is not None:
            from pypaimon.table.system.system_table_scan import SystemSplit
            return Plan(_splits=[SystemSplit(arrow_table)])
    from pypaimon.table.system.system_table_scan import SystemSplit
    return Plan(_splits=[SystemSplit(scan.system_table._build_arrow_table())])
