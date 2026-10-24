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
from contextlib import contextmanager
from threading import local
from typing import Iterator, List, Optional, Tuple

import pyarrow

from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.read.plan import Plan
from pypaimon.read.split import Split
from pypaimon.read.scan_stats import ScanStats


_file_scanner_calls = local()


@contextmanager
def file_scanner_execution(configuration, delegate):
    """Keep a public hook's base calls inside its authorized Python operation."""
    previous = getattr(_file_scanner_calls, 'delegates', ())
    _file_scanner_calls.delegates = previous + ((configuration, delegate),)
    try:
        yield
    finally:
        _file_scanner_calls.delegates = previous


@contextmanager
def file_scanner_hook(configuration, method):
    previous = getattr(_file_scanner_calls, 'hook', None)
    _file_scanner_calls.hook = (configuration, method)
    try:
        yield
    finally:
        _file_scanner_calls.hook = previous


def file_scanner_call(scanner, method):
    """Delegate an explicit public base method without invoking its hook again."""
    hook = getattr(_file_scanner_calls, 'hook', None)
    if hook is not None and hook[0] is scanner and hook[1] == method:
        for configuration, delegate in reversed(getattr(_file_scanner_calls, 'delegates', ())):
            if configuration is scanner:
                return delegate(method)
    # Calling a different public operation is a new authorization boundary:
    # a projected-row grant must never authorize an explicit raw-file request.
    if method == 'scan':
        return plan_scan(scanner._scan, _file_scanner_base=True)
    if method == 'scan_with_stats':
        return scan_with_stats(scanner._scan, _file_scanner_base=True)
    return plan_files(scanner, _file_scanner_base=True)


def validate_read_backend(read):
    """Validate the explicitly selected Python backend without loading Native."""
    if not read.table.options.native_read_enabled():
        from pypaimon.execution.python.read.merge_engine_support import validate_read
        validate_read(read)


def _native_read_execution(read):
    """Select Native before importing its reader; disabled reads stay Python."""
    if not read.table.options.native_read_enabled():
        return native_read_fallback(read, 'read.native.enabled is false')
    from pypaimon.execution.native.read.table_read import NativeTableRead
    return NativeTableRead(read)


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
        native = _native_read_execution(self)
        rows = None if native is None else native.read_rows(splits)
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
    native = _native_read_execution(self)
    native_batches = None if native is None else native.read_batches(
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

    native = _native_read_execution(self)
    native_batches = None if native is None else native.read_batches(
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


def _python_scan_selected(scan):
    execution = vars(scan).get('_execution')
    if execution is not None and not execution._is_native:
        return True
    scanner = vars(scan).get('_file_scanner')
    if scanner is None:
        return False
    from pypaimon.read.file_scanner import FileScanner
    return type(scanner) is not FileScanner or any(name in vars(scanner) for name in FileScanner._EXECUTION_HOOKS)


def _python_scan_execution(scan):
    execution = vars(scan).get('_execution')
    if execution is None or execution._is_native:
        from pypaimon.execution.python.read.table_scan import PythonTableScan
        execution = scan._execution = PythonTableScan(scan)
    return execution


def _native_scan_execution(scan):
    if _python_scan_selected(scan) or not native_plan_requested(scan):
        return None
    from pypaimon.execution.native.read.plan import NativeTableScan
    execution = vars(scan).get('_execution') or NativeTableScan(scan)
    if execution._native_plan_supported():
        scan._execution = execution
        return execution
    return None


def plan_scan(self, *, _file_scanner_base=False) -> Plan:
    # A catalog-issued grant travels with Core's plan. Other authorization
    # callbacks run before discovering a planning backend.
    native_auth = self.table.options.query_auth_enabled
    native = _native_scan_execution(self) if native_auth else None
    if native is not None:
        plan = native.plan()
        if plan is not None:
            return plan
    auth_result = self._TableScan__auth_query()
    if not native_auth and auth_result is None:
        native = _native_scan_execution(self)
        if native is not None:
            plan = native.plan()
            if plan is not None:
                return plan
    execution = _python_scan_execution(self)
    if _file_scanner_base:
        return execution._run_file_scan('scan', auth_result, base=True)
    return execution.plan(auth_result)


def configure_indexed_read(table):
    """Keep explicit preferences; otherwise prefer the available Native reader."""
    from pypaimon.common.options.core_options import CoreOptions
    options = table.options.options
    if not any(options.contains(option) for option in (
            CoreOptions.READ_NATIVE_ENABLED, CoreOptions.SCAN_NATIVE_PLAN_ENABLED)):
        from pypaimon.execution.native.runtime import native_runtime_available
        if native_runtime_available():
            return table.copy_without_time_travel({CoreOptions.READ_NATIVE_ENABLED.key(): 'true'})
    return table


def plan_indexed_read(scan):
    """Try Core lookup planning before selecting the Python index locator."""
    native = _native_scan_execution(scan)
    if native is None:
        return None
    # Retain the complete public authorization path when a grant is required.
    if scan._query_auth_fn is not None or scan.table.options.query_auth_enabled:
        return scan.plan()
    return native.plan()


def new_file_scanner(scan):
    """Create public file configuration without constructing either executor."""
    from pypaimon.read.file_scanner import FileScanner
    return FileScanner(scan)


def python_file_scanner(scan):
    """Enter the selected Python executor's internal file scanner."""
    return _python_scan_execution(scan)._file_scanner()


def file_scan_has_bucket_pruning(scan):
    """Inspect Python statistics only after that backend planned the scan."""
    scanner = python_file_scanner(scan)
    return scanner._bucket_selector is not None or scanner.only_read_real_buckets


def plan_files(scanner, *, _file_scanner_base=False):
    """Plan raw file metadata only with unrestricted whole-table permission."""
    scan = scanner._scan
    if not _python_scan_selected(scan) and native_plan_requested(scan):
        from pypaimon.execution.native.table import _native_query_auth_supported
        # Raw file metadata precedes query result transformations. Do not let
        # their capability checks (shuffle, global indexes or slices) select
        # Python for an operation which does not consume those settings.
        native_supported = _native_query_auth_supported(scan.table)
    else:
        native_supported = False
    native_auth = native_supported and scan.table.options.query_auth_enabled
    if not native_auth and scan._TableScan__auth_query(all_columns=True) is not None:
        raise TableNoPermissionException(scan.table.identifier)
    if native_supported:
        from pypaimon.execution.native.read.plan import NativeTableScan
        result = NativeTableScan(scan).plan_files(scanner.skip_level0, scanner.only_read_real_buckets)
        if result is not None:
            entries, snapshot_id = result
            scanner._scanned_snapshot = None
            scanner._scanned_snapshot_id = snapshot_id
            return entries
    if native_auth and scan._TableScan__auth_query(all_columns=True) is not None:
        raise TableNoPermissionException(scan.table.identifier)
    # Let the selected executor refresh cached rules for this unrestricted scope.
    execution = _python_scan_execution(scan)
    entries = execution._run_file_scan('plan_files', None, base=_file_scanner_base)
    source = execution._file_scanner()
    scanner._scanned_snapshot = source._scanned_snapshot
    scanner._scanned_snapshot_id = source._scanned_snapshot_id
    return entries


def plan_all(self) -> Plan:
    """Plan complete splits only with unrestricted whole-table permission."""
    native = None
    if not _python_scan_selected(self) and native_plan_requested(self):
        from pypaimon.execution.native.table import _native_query_auth_supported
        if _native_query_auth_supported(self.table):
            native = _native_scan_execution(self)
    native_auth = native is not None and self.table.options.query_auth_enabled
    if not native_auth and self._TableScan__auth_query(all_columns=True) is not None:
        raise TableNoPermissionException(self.table.identifier)
    if native is not None:
        plan = native.plan_all()
        if plan is not None:
            return plan
    if native_auth and self._TableScan__auth_query(all_columns=True) is not None:
        raise TableNoPermissionException(self.table.identifier)
    return _python_scan_execution(self).plan_all()


def scan_with_stats(self, *, _file_scanner_base=False) -> Tuple[Plan, Optional[ScanStats]]:
    """Record pruning counters through the selected planner."""
    native_auth = self.table.options.query_auth_enabled
    native = _native_scan_execution(self) if native_auth else None
    if native is not None:
        result = native.scan_with_stats()
        if result is not None:
            return result
    auth_result = self._TableScan__auth_query()
    if not native_auth and auth_result is None:
        native = _native_scan_execution(self)
        if native is not None:
            result = native.scan_with_stats()
            if result is not None:
                return result
    execution = _python_scan_execution(self)
    if _file_scanner_base:
        return execution._run_file_scan('scan_with_stats', auth_result, base=True)
    return execution.scan_with_stats(auth_result)


def new_streaming_scan(self):
    """Create the public streaming facade with this builder's read request."""
    projection = self._projection_builder()
    projection._validate_map_key_filter()
    from pypaimon.read.streaming_table_scan import StreamTableScan
    return StreamTableScan(
        self.table, predicate=self._predicate, read_type=projection.read_type(),
        poll_interval_ms=self._poll_interval_ms, bucket_filter=self._bucket_filter,
        consumer_id=self._consumer_id)


def create_streaming_scan(scan, poll_interval_ms=1000, prefetch_enabled=True,
                          diff_threshold=10):
    """Prepare one executor before any frame or checkpoint is consumed."""
    from pypaimon.write.backend_dispatch import propagate_fork_safety_error

    if native_plan_requested(scan):
        from pypaimon.execution.native.table import _native_query_auth_supported
        native_supported = _native_query_auth_supported(scan.table)
    else:
        native_supported = False
    if native_supported:
        try:
            from pypaimon.execution.native.read.streaming_table_scan import StreamTableScan
            return StreamTableScan(
                scan.table, predicate=scan.predicate, read_type=scan._read_type,
                poll_interval_ms=poll_interval_ms, bucket_filter=scan._bucket_filter,
                consumer_id=scan._consumer_id)
        except Exception as error:
            propagate_fork_safety_error(error)
            logging.warning("Cannot create Native stream scan, using Python: %s", error)
    from pypaimon.execution.python.read.streaming_table_scan import AsyncStreamingTableScan
    execution = AsyncStreamingTableScan(
        table=scan.table,
        predicate=scan.predicate,
        poll_interval_ms=poll_interval_ms,
        bucket_filter=scan._bucket_filter,
        prefetch_enabled=prefetch_enabled,
        diff_threshold=diff_threshold,
        consumer_id=scan._consumer_id
    )
    execution._read_type = scan._read_type
    return execution


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
