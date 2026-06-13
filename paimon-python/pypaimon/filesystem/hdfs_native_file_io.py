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

"""HDFS FileIO backed by the hdfs-native protocol client (no JVM, no libhdfs)."""

import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Dict, Optional
from urllib.parse import urlparse

import pyarrow
import pyarrow.fs as pafs

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import HdfsOptions, SecurityOptions
from pypaimon.common.uri_reader import UriReaderFactory
from pypaimon.filesystem import _kerberos
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser
from pypaimon.write.blob_format_writer import BlobFormatWriter


class _HdfsFileInfo:
    """pafs.FileInfo-shaped adapter built from hdfs_native.FileStatus."""
    __slots__ = ('path', 'size', 'type', 'mtime', 'base_name')

    def __init__(self, path: str, size: Optional[int], file_type, mtime):
        self.path = path
        self.size = size
        self.type = file_type
        self.mtime = mtime
        self.base_name = path.rsplit('/', 1)[-1] if path else ''


class _HdfsWriterAdapter:
    """File-like wrapper over hdfs_native.FileWriter."""

    def __init__(self, fw):
        self._fw = fw
        self._pos = 0
        self._closed = False

    def write(self, buf) -> int:
        n = self._fw.write(buf)
        if n is None:
            n = len(buf) if hasattr(buf, '__len__') else 0
        self._pos += n
        return n

    def tell(self) -> int:
        return self._pos

    def flush(self):
        pass

    def close(self):
        if not self._closed:
            try:
                self._fw.close()
            finally:
                self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class _HdfsReaderAdapter:
    """File-like wrapper over hdfs_native.FileReader.

    Delegates read/seek/tell straight to the underlying reader (which is an
    io.RawIOBase subclass with full seek/tell support). The wrapper only
    exists so that exiting a `with` block guarantees the underlying handle
    is closed — hdfs-native's own FileReader.__exit__ is a no-op.
    """

    def __init__(self, fr):
        self._fr = fr
        self._closed = False

    def read(self, size: int = -1) -> bytes:
        return self._fr.read(-1 if size is None else size)

    def read1(self, size: int = -1) -> bytes:
        return self.read(size)

    def seek(self, pos: int, whence: int = 0) -> int:
        self._fr.seek(pos, whence)
        return self._fr.tell()

    def tell(self) -> int:
        return self._fr.tell()

    def close(self):
        if self._closed:
            return
        try:
            close = getattr(self._fr, 'close', None)
            if close is not None:
                close()
        finally:
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class HdfsNativeFileIO(FileIO):
    """HDFS FileIO that speaks the HDFS RPC protocol directly.

    No JVM, no libhdfs, no Hadoop install required. Hadoop xml is still
    consumed if present (HADOOP_CONF_DIR or `hdfs.conf-dir` option) for
    viewfs mount tables and HA NameNode lists; alternatively the same
    key/values can be delivered via the catalog options channel (a REST
    catalog can therefore push the cluster wiring with the response).
    """

    NATIVE_KEY_PREFIXES = HdfsOptions.HDFS_NATIVE_CONFIG_KEY_PREFIXES
    NS_PREFIX = HdfsOptions.HDFS_CONFIG_PREFIX

    def __init__(self, path: str, catalog_options: Options):
        self.properties = catalog_options or Options({})
        self.logger = logging.getLogger(__name__)
        self.uri_reader_factory = UriReaderFactory(self.properties)

        scheme, netloc, _ = self.parse_location(path)
        if scheme not in {"hdfs", "viewfs"}:
            raise ValueError(
                f"HdfsNativeFileIO does not support scheme '{scheme}'"
            )
        self._scheme = scheme
        self._netloc = netloc

        try:
            from hdfs_native import Client, WriteOptions
        except ImportError as e:
            raise ImportError(
                "hdfs-native is not installed. "
                "Install with: pip install 'pypaimon[hdfs]'"
            ) from e
        self._WriteOptions = WriteOptions

        self._setup_kerberos()

        config_dir = (
            self.properties.get(HdfsOptions.HDFS_CONF_DIR)
            or os.environ.get("HADOOP_CONF_DIR")
        )
        hadoop_xml = self._load_hadoop_xml(config_dir)

        config = self._build_config_dict()
        self._maybe_inject_viewfs_fallback(scheme, netloc, config, hadoop_xml)

        # Stash for the lazy `filesystem` property (the fsspec/pyarrow facade
        # is only built if a caller asks for it).
        self._config = config
        self._hadoop_xml = hadoop_xml
        self._config_dir = config_dir
        self._filesystem = None

        client_kwargs = {}
        url = self._build_url(scheme, netloc)
        if url:
            client_kwargs["url"] = url
        if config:
            client_kwargs["config"] = config
        if config_dir:
            client_kwargs["config_dir"] = config_dir

        self._client = Client(**client_kwargs)

    def __reduce__(self):
        """Pickle support for Ray / multiprocessing.

        hdfs_native.Client is a Rust binding that can't be pickled; rather
        than try to serialise live handles, we serialise the constructor
        inputs and let workers re-init their own Client. Same pattern
        pyarrow.fs.HadoopFileSystem uses.

        Pin the resolved config_dir into the carried options. If the
        driver resolved it from $HADOOP_CONF_DIR, a worker on a host with
        a different env var would otherwise pick up the worker's value
        and silently talk to a different cluster.
        """
        netloc = self._netloc or ""
        path = f"{self._scheme}://{netloc}"
        props_map = dict(self.properties.to_map())
        if self._config_dir and not props_map.get(
            HdfsOptions.HDFS_CONF_DIR.key()
        ):
            props_map[HdfsOptions.HDFS_CONF_DIR.key()] = self._config_dir
        return (type(self), (path, Options(props_map)))

    @property
    def filesystem(self):
        """pyarrow.fs.FileSystem facade backed by hdfs_native.fsspec.

        Lazily constructed: FileIO-only call paths
        (exists/list_status/new_input_stream/...) never pay the fsspec init
        cost; only ds.dataset / open_input_file callers do.
        """
        if self._filesystem is None:
            import pyarrow.fs as pafs
            try:
                from hdfs_native.fsspec import (
                    HdfsFileSystem,
                    ViewfsFileSystem,
                )
            except ImportError as e:
                raise RuntimeError(
                    "hdfs-native fsspec adapter is required to bridge "
                    "HdfsNativeFileIO to a pyarrow.fs filesystem; upgrade "
                    "hdfs-native (>=0.13)."
                ) from e
            cls = (ViewfsFileSystem if self._scheme == "viewfs"
                   else HdfsFileSystem)
            # Merge xml + overrides so the fsspec instance can connect
            # without relying on HADOOP_CONF_DIR (BaseFileSystem.__init__
            # only forwards storage_options to Client, not config_dir).
            merged_config = {**self._hadoop_xml, **self._config}
            fsspec_fs = cls(host=self._netloc, **merged_config)
            self._filesystem = pafs.PyFileSystem(
                pafs.FSSpecHandler(fsspec_fs))
        return self._filesystem

    @staticmethod
    def parse_location(location: str):
        uri = urlparse(location)
        if not uri.scheme:
            return "file", uri.netloc, os.path.abspath(location)
        return uri.scheme, uri.netloc, uri.path

    @staticmethod
    def _build_url(scheme: str, netloc: Optional[str]) -> Optional[str]:
        if not netloc:
            return None
        return f"{scheme}://{netloc}"

    @staticmethod
    def _load_hadoop_xml(config_dir: Optional[str]) -> Dict[str, str]:
        """Parse core-site.xml + hdfs-site.xml from a Hadoop config dir into a
        flat {name: value} dict. Returns empty dict if the dir is missing or
        unreadable.

        Used only to discover viewfs mount-table state so we can polyfill the
        linkFallback mount that hdfs-native requires but libhdfs tolerates.
        The final config dir is still handed to hdfs-native for its own
        (more complete) xml parsing.
        """
        result: Dict[str, str] = {}
        if not config_dir or not os.path.isdir(config_dir):
            return result
        for fname in ("core-site.xml", "hdfs-site.xml"):
            path = os.path.join(config_dir, fname)
            if not os.path.isfile(path):
                continue
            try:
                tree = ET.parse(path)
            except (ET.ParseError, OSError):
                continue
            for prop in tree.getroot().findall("property"):
                name_el = prop.find("name")
                value_el = prop.find("value")
                if name_el is None or name_el.text is None:
                    continue
                value = (
                    value_el.text.strip()
                    if value_el is not None and value_el.text
                    else ""
                )
                result[name_el.text.strip()] = value
        return result

    @staticmethod
    def _maybe_inject_viewfs_fallback(
        scheme: str,
        netloc: Optional[str],
        overrides: Dict[str, str],
        hadoop_xml: Dict[str, str],
    ) -> None:
        """If we're opening a viewfs URI and no linkFallback is configured for
        the cluster, pick a usable nameservice URI from existing link.*
        targets or dfs.nameservices and inject one into `overrides`.

        hdfs-native rejects viewfs init without a fallback mount; libhdfs
        tolerates it. This bridges the gap without touching cluster xml.

        The mount-table state is read from the merged view of hadoop xml and
        catalog-option overrides, so a zero-file viewfs setup (link.* /
        dfs.nameservices pushed purely through catalog options) gets a
        fallback too; the injected key is still only written back to
        `overrides`.
        """
        if scheme != "viewfs" or not netloc:
            return
        cluster = netloc
        fallback_key = f"fs.viewfs.mounttable.{cluster}.linkFallback"
        if fallback_key in overrides or fallback_key in hadoop_xml:
            return

        merged = {**hadoop_xml, **overrides}

        link_prefix = f"fs.viewfs.mounttable.{cluster}.link."
        for key, value in merged.items():
            if key.startswith(link_prefix) and value:
                parsed = urlparse(value)
                if parsed.scheme == "hdfs" and parsed.netloc:
                    overrides[fallback_key] = f"hdfs://{parsed.netloc}/"
                    return

        nameservices = [
            ns.strip()
            for ns in merged.get("dfs.nameservices", "").split(",")
            if ns.strip()
        ]
        if nameservices:
            overrides[fallback_key] = f"hdfs://{nameservices[0]}/"

    def _setup_kerberos(self):
        principal = (
            self.properties.get(SecurityOptions.KERBEROS_PRINCIPAL)
            or self.properties.to_map().get("security.principal")
        )
        keytab = (
            self.properties.get(SecurityOptions.KERBEROS_KEYTAB)
            or self.properties.to_map().get("security.keytab")
        )
        if bool(principal) != bool(keytab):
            raise ValueError(
                "security.kerberos.login.principal and "
                "security.kerberos.login.keytab "
                "must be both set or both unset"
            )
        if principal and keytab:
            _kerberos.kerberos_login_from_keytab(principal, keytab)
            cache_path = _kerberos.get_ticket_cache_path()
            if not cache_path:
                raise RuntimeError(
                    "kinit succeeded but no ticket cache path could be "
                    "determined. Set the KRB5CCNAME environment variable "
                    "to specify the cache location."
                )
            # hdfs-native's GSSAPI layer reads KRB5CCNAME from the process
            # env, which is global state. If a different cache was already
            # configured (typically because another HdfsNativeFileIO with
            # a different principal lives in the same process), warn — the
            # last writer wins and earlier instances will start using the
            # new ticket, which is almost certainly not what the caller
            # wanted.
            existing = os.environ.get("KRB5CCNAME")
            existing_stripped = (
                existing[5:] if existing and existing.startswith("FILE:")
                else existing
            )
            if existing_stripped and existing_stripped != cache_path:
                self.logger.warning(
                    "Overwriting process-global KRB5CCNAME from %r to %r; "
                    "concurrent HdfsNativeFileIO instances with different "
                    "Kerberos principals share env state and will clobber "
                    "each other's ticket caches.",
                    existing, cache_path,
                )
            # Preserve the `FILE:` qualifier if the existing value carried
            # it — some GSSAPI tooling distinguishes cache types by prefix.
            os.environ["KRB5CCNAME"] = (
                f"FILE:{cache_path}"
                if existing and existing.startswith("FILE:")
                else cache_path
            )

    def _build_config_dict(self) -> Dict[str, str]:
        config: Dict[str, str] = {}
        for key, value in self.properties.to_map().items():
            if value is None:
                continue
            if any(key.startswith(p) for p in self.NATIVE_KEY_PREFIXES):
                config[key] = str(value)
            elif key.startswith(self.NS_PREFIX):
                config[key[len(self.NS_PREFIX):]] = str(value)
        return config

    def to_filesystem_path(self, path: str) -> str:
        # hdfs-native expects an absolute path within the cluster the Client is
        # bound to; passing a full URI makes its Rust-side MountTable::resolve
        # treat the string as a relative path (since it doesn't start with '/')
        # and prepend the user's home dir, producing nonsense like
        # `/user/foo/viewfs://cluster/...`. Strip the matching scheme+authority
        # so a plain absolute path reaches the client.
        parsed = urlparse(path)
        if parsed.scheme in ("hdfs", "viewfs"):
            if parsed.scheme == self._scheme and (
                not parsed.netloc or parsed.netloc == self._netloc
            ):
                return parsed.path or "/"
        return path

    def _adapt_status(self, status, fallback_path: str = '') -> _HdfsFileInfo:
        path = getattr(status, 'path', None) or fallback_path
        is_dir = bool(getattr(status, 'isdir', False))
        length = getattr(status, 'length', 0)
        mtime_ms = getattr(status, 'modification_time', None)
        mtime = (
            datetime.fromtimestamp(mtime_ms / 1000.0, tz=timezone.utc)
            if mtime_ms else None
        )
        size = None if is_dir else int(length or 0)
        ftype = pafs.FileType.Directory if is_dir else pafs.FileType.File
        return _HdfsFileInfo(path, size, ftype, mtime)

    def new_input_stream(self, path: str):
        path_str = self.to_filesystem_path(path)
        reader = self._client.read(path_str)
        return _HdfsReaderAdapter(reader)

    def new_output_stream(self, path: str):
        path_str = self.to_filesystem_path(path)
        writer = self._client.create(
            path_str,
            self._WriteOptions(create_parent=True, overwrite=True),
        )
        return _HdfsWriterAdapter(writer)

    def get_file_status(self, path: str):
        path_str = self.to_filesystem_path(path)
        try:
            status = self._client.get_file_info(path_str)
        except FileNotFoundError:
            raise FileNotFoundError(f"File {path} does not exist")
        return self._adapt_status(status, path_str)

    def list_status(self, path: str):
        path_str = self.to_filesystem_path(path)
        return [self._adapt_status(s) for s in self._client.list_status(path_str)]

    def exists(self, path: str) -> bool:
        path_str = self.to_filesystem_path(path)
        try:
            self._client.get_file_info(path_str)
            return True
        except FileNotFoundError:
            return False

    def delete(self, path: str, recursive: bool = False) -> bool:
        path_str = self.to_filesystem_path(path)
        try:
            status = self._client.get_file_info(path_str)
        except FileNotFoundError:
            return False
        if bool(getattr(status, 'isdir', False)) and not recursive:
            if next(iter(self._client.list_status(path_str)), None) is not None:
                raise OSError(f"Directory {path} is not empty")
        return bool(self._client.delete(path_str, recursive))

    def mkdirs(self, path: str) -> bool:
        path_str = self.to_filesystem_path(path)
        try:
            status = self._client.get_file_info(path_str)
        except FileNotFoundError:
            self._client.mkdirs(path_str, create_parent=True)
            return True
        if bool(getattr(status, 'isdir', False)):
            return True
        raise FileExistsError(f"Path exists but is not a directory: {path}")

    def rename(self, src: str, dst: str) -> bool:
        src_str = self.to_filesystem_path(src)
        dst_str = self.to_filesystem_path(dst)
        dst_parent = str(PurePosixPath(dst_str).parent)
        if dst_parent and dst_parent != '.':
            try:
                self._client.get_file_info(dst_parent)
            except FileNotFoundError:
                self._client.mkdirs(dst_parent, create_parent=True)
        try:
            dst_status = self._client.get_file_info(dst_str)
            if not getattr(dst_status, 'isdir', False):
                return False
            src_name = PurePosixPath(src_str).name
            dst_str = str(PurePosixPath(dst_str) / src_name)
            try:
                self._client.get_file_info(dst_str)
                return False
            except FileNotFoundError:
                pass
        except FileNotFoundError:
            pass
        try:
            self._client.rename(src_str, dst_str)
            return True
        except FileNotFoundError:
            return False
        except (PermissionError, OSError):
            return False

    def write_parquet(self, path: str, data: pyarrow.Table,
                      compression: str = 'zstd', zstd_level: int = 1, **kwargs):
        try:
            import pyarrow.parquet as pq
            if compression.lower() == 'zstd':
                kwargs['compression_level'] = zstd_level
            with self.new_output_stream(path) as raw_stream:
                stream = pyarrow.PythonFile(raw_stream, mode='wb')
                try:
                    pq.write_table(
                        data, stream, compression=compression, **kwargs)
                finally:
                    stream.close()
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Parquet file {path}: {e}") from e

    def write_orc(self, path: str, data: pyarrow.Table,
                  compression: str = 'zstd', zstd_level: int = 1, **kwargs):
        try:
            import sys
            import pyarrow.orc as orc
            data = self._cast_time_columns_for_orc(data)
            with self.new_output_stream(path) as raw_stream:
                stream = pyarrow.PythonFile(raw_stream, mode='wb')
                try:
                    if sys.version_info[:2] == (3, 6):
                        orc.write_table(data, stream, **kwargs)
                    else:
                        orc.write_table(
                            data, stream, compression=compression, **kwargs)
                finally:
                    stream.close()
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write ORC file {path}: {e}") from e

    def write_avro(self, path: str, data: pyarrow.Table,
                   avro_schema=None, compression: str = 'zstd',
                   zstd_level: int = 1, **kwargs):
        import fastavro
        if avro_schema is None:
            avro_schema = PyarrowFieldParser.to_avro_schema(data.schema)

        records_dict = data.to_pydict()

        def record_generator():
            num_rows = len(list(records_dict.values())[0])
            for i in range(num_rows):
                record = {}
                for col in records_dict.keys():
                    value = records_dict[col][i]
                    if isinstance(value, datetime) and value.tzinfo is None:
                        value = value.replace(tzinfo=timezone.utc)
                    record[col] = value
                yield record

        codec_map = {
            'null': 'null', 'deflate': 'deflate', 'snappy': 'snappy',
            'bzip2': 'bzip2', 'xz': 'xz', 'zstandard': 'zstandard',
            'zstd': 'zstandard',
        }
        codec = codec_map.get(compression.lower())
        if codec is None:
            raise ValueError(
                f"Unsupported compression '{compression}' for Avro format. "
                f"Supported compressions: {', '.join(sorted(codec_map.keys()))}."
            )
        if codec == 'zstandard':
            kwargs['codec_compression_level'] = zstd_level
        with self.new_output_stream(path) as output_stream:
            fastavro.writer(output_stream, avro_schema,
                            record_generator(), codec=codec, **kwargs)

    def write_blob(self, path: str, data: pyarrow.Table, **kwargs):
        try:
            if data.num_columns != 1:
                raise RuntimeError(
                    f"Blob format only supports a single column, "
                    f"got {data.num_columns} columns")
            field = data.schema[0]
            if pyarrow.types.is_large_binary(field.type):
                fields = [DataField(0, field.name, AtomicType("BLOB"))]
            else:
                paimon_type = PyarrowFieldParser.to_paimon_type(
                    field.type, field.nullable)
                fields = [DataField(0, field.name, paimon_type)]
            records_dict = data.to_pydict()
            num_rows = data.num_rows
            field_name = fields[0].name
            with self.new_output_stream(path) as output_stream:
                writer = BlobFormatWriter(output_stream)
                for i in range(num_rows):
                    writer.write_value(records_dict[field_name][i],
                                       fields, self.uri_reader_factory)
                writer.close()
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write blob file {path}: {e}") from e

    def write_lance(self, path: str, data: pyarrow.Table, **kwargs):
        # Mirror the remote-scheme writer: lance/vortex talk to the backend
        # through their own object_store, so we hand them the URI plus any
        # storage options the FileIO exposes rather than routing through the
        # native client. Without these two methods, an HDFS table configured
        # with file.format=lance/vortex would hit FileIO's NotImplementedError
        # now that this class is the default hdfs:// backend.
        try:
            import lance

            from pypaimon.read.reader.lance_utils import to_lance_specified
            file_path_for_lance, storage_options = to_lance_specified(self, path)

            writer = lance.file.LanceFileWriter(
                file_path_for_lance, data.schema,
                storage_options=storage_options, **kwargs)
            try:
                for batch in data.to_batches():
                    writer.write_batch(batch)
            finally:
                writer.close()
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Lance file {path}: {e}") from e

    def write_mosaic(self, path: str, data: pyarrow.Table, **kwargs):
        try:
            import mosaic
            with self.new_output_stream(path) as output_stream:
                mosaic.write_table(data, output_stream)
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Mosaic file {path}: {e}") from e

    def write_vortex(self, path: str, data: pyarrow.Table, **kwargs):
        try:
            import vortex
            from vortex import store

            from pypaimon.read.reader.vortex_utils import to_vortex_specified
            file_path_for_vortex, store_kwargs = to_vortex_specified(self, path)

            if store_kwargs:
                vortex_store = store.from_url(file_path_for_vortex, **store_kwargs)
                vortex_store.write(vortex.array(data))
            else:
                from vortex._lib.io import write as vortex_write
                vortex_write(vortex.array(data), file_path_for_vortex)
        except Exception as e:
            self.delete_quietly(path)
            raise RuntimeError(f"Failed to write Vortex file {path}: {e}") from e

    def close(self):
        self._client = None
