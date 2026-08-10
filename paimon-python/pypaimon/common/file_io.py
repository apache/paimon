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

import logging
import os
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

import pyarrow  # noqa: F401
import pyarrow.fs as pafs

from pypaimon.common.options import Options

_LOG = logging.getLogger(__name__)


def supports_pread(stream) -> bool:
    """Check if the stream supports position-based reads (thread-safe I/O)."""
    if hasattr(stream, 'read_at'):
        return True
    if hasattr(stream, 'fileno'):
        try:
            stream.fileno()
            return True
        except Exception:
            pass
    return False


def pread(stream, length: int, offset: int) -> bytes:
    """Position-based read without changing the stream cursor. Thread-safe."""
    if hasattr(stream, 'read_at'):
        return stream.read_at(length, offset)
    return os.pread(stream.fileno(), length, offset)


# Coalescing bounds: merge same-file ranges whose gap is within GAP, capping a
# merged read at SPAN so threads stay busy and memory stays bounded.
_COALESCE_GAP = 1 << 20
_COALESCE_SPAN = 8 << 20
_COALESCE_VIEW_MAX_RETAINED_AMPLIFICATION = 2.0
# Bound per-object opens; 16 cuts them by 75% for default 64-range batches.
_MAX_RANGE_LANES_PER_PATH = 16


def create_temp_path(path: str) -> str:
    """Create the hidden temporary path used for an atomic write."""
    separator = max(path.rfind('/'), path.rfind('\\'))
    return f"{path[:separator + 1]}.{path[separator + 1:]}.{uuid.uuid4()}.tmp"


def _coalesce_ranges(items, max_gap, max_span):
    """Group ``(idx, path, offset, length)`` (length >= 0) into merged spans:
    ``[(path, span_offset, span_length, [(idx, offset, length), ...])]``."""
    from collections import defaultdict
    by_path = defaultdict(list)
    for it in items:
        by_path[it[1]].append(it)
    spans = []
    for path, group in by_path.items():
        group.sort(key=lambda x: x[2])
        cur, start, end = [], None, None
        for idx, _, off, length in group:
            stop = off + length
            if cur and off - end <= max_gap and stop - start <= max_span:
                cur.append((idx, off, length))
                end = max(end, stop)
            else:
                if cur:
                    spans.append((path, start, end - start, cur))
                cur, start, end = [(idx, off, length)], off, stop
        if cur:
            spans.append((path, start, end - start, cur))
    return spans


class FileIO(ABC):
    """
    File IO interface to read and write files.
    """

    @abstractmethod
    def new_input_stream(self, path: str):
        pass

    @abstractmethod
    def new_output_stream(self, path: str):
        pass

    @abstractmethod
    def get_file_status(self, path: str):
        pass

    @abstractmethod
    def list_status(self, path: str):
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass

    def exists_batch(self, paths: List[str]) -> Dict[str, bool]:
        """Check existence of multiple paths, returning {path: bool}."""
        return {path: self.exists(path) for path in paths}

    @abstractmethod
    def delete(self, path: str, recursive: bool = False) -> bool:
        pass

    @abstractmethod
    def mkdirs(self, path: str) -> bool:
        pass

    @abstractmethod
    def rename(self, src: str, dst: str) -> bool:
        pass

    def delete_quietly(self, path: str):
        logger = logging.getLogger(__name__)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Ready to delete {path}")

        try:
            if not self.delete(path, False) and self.exists(path):
                logger.warning(f"Failed to delete file {path}")
        except Exception:
            logger.warning(f"Exception occurs when deleting file {path}", exc_info=True)

    def delete_files_quietly(self, files: List[str]):
        for file_path in files:
            self.delete_quietly(file_path)

    def delete_directory_quietly(self, directory: str):
        logger = logging.getLogger(__name__)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Ready to delete {directory}")

        try:
            if not self.delete(directory, True) and self.exists(directory):
                logger.warning(f"Failed to delete directory {directory}")
        except Exception:
            logger.warning(f"Exception occurs when deleting directory {directory}", exc_info=True)

    def get_file_size(self, path: str) -> int:
        file_info = self.get_file_status(path)
        if file_info.size is None:
            raise ValueError(f"File size not available for {path}")
        return file_info.size

    def is_dir(self, path: str) -> bool:
        file_info = self.get_file_status(path)
        return file_info.type == pafs.FileType.Directory

    def check_or_mkdirs(self, path: str):
        if self.exists(path):
            if not self.is_dir(path):
                raise ValueError(f"The path '{path}' should be a directory.")
        else:
            self.mkdirs(path)

    def read_file_range(self, path, offset, length):
        """Read a byte range. Thread-safe. ``length < 0`` = read to EOF (pread
        can't express that, so seek + read)."""
        stream = self.new_input_stream(path)
        try:
            if length >= 0 and supports_pread(stream):
                return pread(stream, length, offset)
            stream.seek(offset)
            return stream.read() if length < 0 else stream.read(length)
        finally:
            stream.close()

    def read_ranges_coalesced(self, ranges, parallelism,
                              max_gap=_COALESCE_GAP, max_span=_COALESCE_SPAN):
        """Read ``ranges`` (each ``None`` or ``(path, offset, length)``), returning
        bytes in the same order. Same-file nearby ranges are merged into one read
        to cut round trips, then sliced. Each worker lane reuses one exclusive
        stream for consecutive spans of the same path. Negative length (read to
        EOF) is read on its own, never merged.

        A failed read propagates and aborts the whole batch (unlike a per-row
        ``file.open()`` loop that fails one row at a time).
        """
        return self._read_ranges_coalesced(
            ranges, parallelism, max_gap, max_span,
            max_retained_amplification=0, return_views=False)

    def read_ranges_coalesced_views(self, ranges, parallelism,
                                    max_gap=_COALESCE_GAP, max_span=_COALESCE_SPAN,
                                    max_retained_amplification=(
                                        _COALESCE_VIEW_MAX_RETAINED_AMPLIFICATION)):
        """Read coalesced ranges as zero-copy ``memoryview`` slices.

        Member ranges from the same merged span share the span's backing buffer
        instead of allocating one ``bytes`` object per member. Callers must
        accept the Python buffer protocol. Use :meth:`read_ranges_coalesced`
        when concrete ``bytes`` results are required. Each view keeps its backing
        buffer alive. Sparse spans use independent views so they do not retain
        excessive gap bytes; set ``max_retained_amplification`` to a non-positive
        value to always share the merged buffer.
        """
        return self._read_ranges_coalesced(
            ranges, parallelism, max_gap, max_span, max_retained_amplification,
            return_views=True)

    def _read_ranges_coalesced(self, ranges, parallelism, max_gap, max_span,
                               max_retained_amplification, return_views):
        from concurrent.futures import ThreadPoolExecutor
        # Threads write disjoint results[idx]; safe under the GIL (no list resize).
        results = [None] * len(ranges)
        coalescible, singletons = [], []
        for index, value in enumerate(ranges):
            # None path/offset/length => null blob, leave result None.
            if (value is None or value[0] is None
                    or value[1] is None or value[2] is None):
                continue
            path, offset, length = value
            if length < 0:  # unknown length => read to EOF, never coalesced
                singletons.append((index, path, offset, length))
            else:
                coalescible.append((index, path, offset, length))

        spans = _coalesce_ranges(coalescible, max_gap, max_span)
        tasks_by_path = {}
        for span in spans:
            tasks_by_path.setdefault(span[0], []).append(("span", span))
        for singleton in singletons:
            tasks_by_path.setdefault(singleton[1], []).append(
                ("one", singleton))
        task_count = sum(len(path_tasks)
                         for path_tasks in tasks_by_path.values())
        if task_count == 0:
            return results

        workers = max(1, min(parallelism, task_count))

        lanes = [[] for _ in range(workers)]
        lane_loads = [0] * workers
        path_task_groups = list(tasks_by_path.values())
        path_capacities = [
            min(len(path_tasks), _MAX_RANGE_LANES_PER_PATH)
            for path_tasks in path_task_groups
        ]
        path_lane_counts = [
            min(
                capacity,
                max(
                    1,
                    (workers * len(path_tasks) + task_count - 1)
                    // task_count,
                ),
            )
            for path_tasks, capacity in zip(
                path_task_groups, path_capacities)
        ]
        remaining_lanes = max(
            0,
            min(workers, sum(path_capacities)) - sum(path_lane_counts),
        )
        for _ in range(remaining_lanes):
            candidates = [
                index for index in range(len(path_task_groups))
                if path_lane_counts[index] < path_capacities[index]
            ]
            if not candidates:
                break
            index = max(
                candidates,
                key=lambda value: (
                    len(path_task_groups[value])
                    / path_lane_counts[value]
                ),
            )
            path_lane_counts[index] += 1

        for path_tasks, path_lanes in zip(
                path_task_groups, path_lane_counts):
            selected = sorted(
                range(workers), key=lane_loads.__getitem__)[:path_lanes]
            for index, task in enumerate(path_tasks):
                lane = selected[index % path_lanes]
                lanes[lane].append(task)
                lane_loads[lane] += 1
        lanes = [lane for lane in lanes if lane]

        class _RangeLane:
            def __init__(self, file_io):
                self._file_io = file_io
                self._path = None
                self._stream = None
                self._close_error = None

            def _close_current(self):
                stream = self._stream
                self._stream = None
                self._path = None
                if stream is None:
                    return
                try:
                    stream.close()
                except BaseException as error:
                    if self._close_error is None:
                        self._close_error = error

            def _stream_for(self, path):
                if self._stream is not None and self._path == path:
                    return self._stream
                self._close_current()
                self._stream = self._file_io.new_input_stream(path)
                self._path = path
                return self._stream

            def read(self, path, offset, length):
                try:
                    stream = self._stream_for(path)
                    if length >= 0 and supports_pread(stream):
                        return pread(stream, length, offset)
                    stream.seek(offset)
                    return (stream.read() if length < 0
                            else stream.read(length))
                except Exception:
                    self._close_current()
                    return self._file_io.read_file_range(
                        path, offset, length)

            def close(self):
                self._close_current()
                if self._close_error is not None:
                    raise self._close_error

        def _run_task(reader, task):
            kind, payload = task
            if kind == "span":
                path, span_off, span_len, members = payload
                buf = reader.read(path, span_off, span_len)
                if return_views:
                    buf = memoryview(buf)
                    useful = sum(length for _, _, length in members)
                    share_buffer = (
                        max_retained_amplification <= 0
                        or span_len <= useful * max_retained_amplification
                    )
                for idx, off, length in members:
                    start = off - span_off
                    value = buf[start:start + length]
                    if return_views and not share_buffer:
                        value = memoryview(bytes(value))
                    results[idx] = value
            else:
                idx, path, offset, length = payload
                result = reader.read(path, offset, length)
                results[idx] = (
                    memoryview(result) if return_views else result)

        def _run_lane(lane):
            reader = _RangeLane(self)
            read_error = None
            try:
                for task in lane:
                    _run_task(reader, task)
            except BaseException as error:
                read_error = error
            close_error = None
            try:
                reader.close()
            except BaseException as error:
                close_error = error
            return read_error, close_error

        with ThreadPoolExecutor(len(lanes)) as pool:
            outcomes = list(pool.map(_run_lane, lanes))
        read_error = next(
            (error for error, _ in outcomes if error is not None), None)
        close_error = next(
            (error for _, error in outcomes if error is not None), None)
        if read_error is not None:
            if close_error is not None:
                _LOG.warning(
                    "Failed to close a range input stream",
                    exc_info=(type(close_error), close_error,
                              close_error.__traceback__),
                )
            raise read_error
        if close_error is not None:
            raise close_error
        return results

    def read_blobs_concurrent(self, blobs, parallelism):
        """Read a list of Blobs concurrently, coalescing same-file ranged reads.

        ``BlobRef`` values expose a file range and are coalesced; in-memory
        ``BlobData`` values are returned directly.
        """
        from pypaimon.table.row.blob import BlobRef
        results: List[Optional[bytes]] = [None] * len(blobs)
        ranges: List[Optional[tuple]] = [None] * len(blobs)
        inmem = []
        for i, b in enumerate(blobs):
            if b is None:
                continue
            if isinstance(b, BlobRef):
                d = b.to_descriptor()
                ranges[i] = (d.uri, d.offset, d.length)
            else:
                inmem.append((i, b))
        for i, v in enumerate(self.read_ranges_coalesced(ranges, parallelism)):
            if v is not None:
                results[i] = v
        for idx, b in inmem:
            results[idx] = b.to_data()
        return results

    def read_file_utf8(self, path: str) -> str:
        with self.new_input_stream(path) as input_stream:
            return input_stream.read().decode('utf-8')

    def try_to_write_atomic(self, path: str, content: str) -> bool:
        if self.exists(path):
            if self.is_dir(path):
                return False

        temp_path = create_temp_path(path)
        success = False
        try:
            self.write_file(temp_path, content, False)
            success = self.rename(temp_path, path)
        finally:
            if not success:
                self.delete_quietly(temp_path)
        return success

    def write_file(self, path: str, content: str, overwrite: bool = False):
        if not overwrite and self.exists(path):
            raise FileExistsError(f"File {path} already exists and overwrite=False")

        with self.new_output_stream(path) as output_stream:
            output_stream.write(content.encode('utf-8'))

    def overwrite_file_utf8(self, path: str, content: str):
        with self.new_output_stream(path) as output_stream:
            output_stream.write(content.encode('utf-8'))

    def copy_file(self, source_path: str, target_path: str, overwrite: bool = False):
        if not overwrite and self.exists(target_path):
            raise FileExistsError(f"Target file {target_path} already exists and overwrite=False")

        target_str = self.to_filesystem_path(target_path)
        target_parent = Path(target_str).parent

        if str(target_parent) and not self.exists(str(target_parent)):
            self.mkdirs(str(target_parent))

        with self.new_input_stream(source_path) as input_stream:
            with self.new_output_stream(target_path) as output_stream:
                output_stream.write(input_stream.read())

    def copy_files(self, source_directory: str, target_directory: str, overwrite: bool = False):
        file_infos = self.list_status(source_directory)
        for file_info in file_infos:
            if file_info.type == pafs.FileType.File:
                source_file = file_info.path
                file_name = source_file.split('/')[-1]
                target_file = f"{target_directory.rstrip('/')}/{file_name}" if target_directory else file_name
                self.copy_file(source_file, target_file, overwrite)

    def read_overwritten_file_utf8(self, path: str) -> Optional[str]:
        retry_number = 0
        exception = None
        while retry_number < 5:
            try:
                return self.read_file_utf8(path)
            except FileNotFoundError:
                return None
            except Exception as e:
                if not self.exists(path):
                    return None

                if (str(type(e).__name__).endswith("RemoteFileChangedException") or
                        (str(e) and "Blocklist for" in str(e) and "has changed" in str(e))):
                    exception = e
                    retry_number += 1
                else:
                    raise e

        if exception:
            if isinstance(exception, Exception):
                raise exception
            else:
                raise RuntimeError(exception)

        return None

    def to_filesystem_path(self, path: str) -> str:
        return path

    def parse_location(self, location: str):
        import os
        from urllib.parse import urlparse

        uri = urlparse(location)
        if not uri.scheme:
            return "file", uri.netloc, os.path.abspath(location)
        elif uri.scheme in ("hdfs", "viewfs"):
            return uri.scheme, uri.netloc, uri.path
        else:
            return uri.scheme, uri.netloc, f"{uri.netloc}{uri.path}"

    def write_parquet(self, path: str, data, compression: str = 'zstd',
                      zstd_level: int = 1, **kwargs):
        raise NotImplementedError("write_parquet must be implemented by FileIO subclasses")

    @staticmethod
    def _cast_time_columns_for_orc(data):
        """Cast time32 columns to int32 before writing ORC.

        PyArrow's ORC writer does not support time types.
        """
        has_time = any(pyarrow.types.is_time(f.type) for f in data.schema)
        if not has_time:
            return data
        columns = []
        for i, field in enumerate(data.schema):
            col = data.column(i)
            if pyarrow.types.is_time(field.type):
                if not pyarrow.types.is_time32(field.type) \
                        or field.type != pyarrow.time32('ms'):
                    raise ValueError(
                        "Column '{}' has type {} which cannot be safely cast to int32 "
                        "for ORC writing. Use time32('ms') instead."
                        .format(field.name, field.type)
                    )
                col = col.cast(pyarrow.int32())
            columns.append(col)
        orc_schema = pyarrow.schema([
            pyarrow.field(f.name, pyarrow.int32(), f.nullable) if pyarrow.types.is_time(f.type)
            else f
            for f in data.schema
        ])
        return pyarrow.table(columns, schema=orc_schema)

    def write_orc(self, path: str, data, compression: str = 'zstd',
                  zstd_level: int = 1, **kwargs):
        raise NotImplementedError("write_orc must be implemented by FileIO subclasses")

    def write_avro(self, path: str, data, avro_schema=None,
                   compression: str = 'zstd', zstd_level: int = 1, **kwargs):
        raise NotImplementedError("write_avro must be implemented by FileIO subclasses")

    def write_lance(self, path: str, data, **kwargs):
        raise NotImplementedError("write_lance must be implemented by FileIO subclasses")

    def write_blob(self, path: str, data, **kwargs):
        """Write Blob format file."""
        raise NotImplementedError("write_blob must be implemented by FileIO subclasses")

    def write_vortex(self, path: str, data, **kwargs):
        raise NotImplementedError("write_vortex must be implemented by FileIO subclasses")

    def write_row(self, path: str, data, fields=None, zstd_level: int = 1, **kwargs):
        raise NotImplementedError("write_row must be implemented by FileIO subclasses")

    def close(self):
        pass

    @staticmethod
    def get(path: str, catalog_options: Optional[Options] = None) -> 'FileIO':
        """
        Returns a FileIO instance for accessing the file system identified by the given path.
        - LocalFileIO for local file system (file:// or no scheme)
        - HdfsNativeFileIO for HDFS/ViewFS (default; pure protocol client, no Hadoop install)
        - PyArrowFileIO for other remote file systems (oss://, s3://, gs://, ...),
          and for HDFS when explicitly requested via hdfs.client.impl=pyarrow
        """
        import os as _os
        from urllib.parse import urlparse

        from pypaimon.common.options.config import CatalogOptions

        opts = catalog_options or Options({})
        if opts.get(CatalogOptions.RESOLVING_FILE_IO_ENABLED):
            from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
            return ResolvingFileIO(opts)

        uri = urlparse(path)
        scheme = uri.scheme

        if not scheme or scheme == "file":
            from pypaimon.filesystem.local_file_io import LocalFileIO
            return LocalFileIO(path, catalog_options)

        if scheme in ("hdfs", "viewfs"):
            from pypaimon.common.options.config import HdfsOptions
            impl_source = "hdfs.client.impl option"
            # Treat an empty option value the same as "unset" so callers can
            # blank it out (common in templated configs) without tripping
            # the unsupported-impl branch.
            impl_value = opts.to_map().get(HdfsOptions.HDFS_CLIENT_IMPL.key())
            if not impl_value:
                impl_value = _os.environ.get("PYPAIMON_HDFS_IMPL")
                impl_source = "PYPAIMON_HDFS_IMPL env var"
            if not impl_value:
                impl_value = HdfsOptions.HDFS_CLIENT_IMPL.default_value()
                impl_source = "default"
            impl = impl_value.lower()
            if impl == "native":
                try:
                    from pypaimon.filesystem.hdfs_native_file_io import \
                        HdfsNativeFileIO
                    return HdfsNativeFileIO(path, opts)
                except (ImportError, RuntimeError) as e:
                    fallback = opts.get(HdfsOptions.HDFS_CLIENT_FALLBACK_TO_PYARROW)
                    if not fallback:
                        raise
                    logging.getLogger(__name__).warning(
                        "Native HDFS backend init failed, falling back to "
                        "pyarrow: %s", e,
                    )
            elif impl != "pyarrow":
                raise ValueError(
                    f"Unsupported hdfs.client.impl '{impl_value}' "
                    f"(from {impl_source}). Supported: 'native', 'pyarrow'."
                )

        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
        return PyArrowFileIO(path, opts)
