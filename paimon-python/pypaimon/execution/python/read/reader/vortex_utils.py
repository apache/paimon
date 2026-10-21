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
import threading
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from pypaimon.common.file_io import FileIO
from pypaimon.common.options.config import OssOptions


def to_vortex_specified(file_io: FileIO, file_path: str) -> Tuple[str, Optional[Dict[str, str]]]:
    """Convert path and extract storage options for Vortex store.from_url().

    Returns (url, store_kwargs) where store_kwargs can be passed as
    keyword arguments to ``vortex.store.from_url(url, **store_kwargs)``.
    For local paths store_kwargs is None.
    """
    if hasattr(file_io, 'file_io'):
        file_io = file_io.file_io()

    if hasattr(file_io, 'get_merged_properties'):
        properties = file_io.get_merged_properties()
    else:
        properties = file_io.properties if hasattr(file_io, 'properties') and file_io.properties else None

    scheme, _, _ = file_io.parse_location(file_path)
    file_path_for_vortex = file_io.to_filesystem_path(file_path)

    store_kwargs = None

    if scheme in {'file', None} or not scheme:
        if not os.path.isabs(file_path_for_vortex):
            file_path_for_vortex = os.path.abspath(file_path_for_vortex)
        return file_path_for_vortex, None

    # For remote schemes, keep the original URI so vortex can parse it
    file_path_for_vortex = file_path

    if scheme == 'oss' and properties:
        parsed = urlparse(file_path)
        bucket = parsed.netloc

        store_kwargs = {
            'endpoint': f"https://{bucket}.{properties.get(OssOptions.OSS_ENDPOINT)}",
            'access_key_id': properties.get(OssOptions.OSS_ACCESS_KEY_ID),
            'secret_access_key': properties.get(OssOptions.OSS_ACCESS_KEY_SECRET),
            'virtual_hosted_style_request': 'true',
        }
        if properties.contains(OssOptions.OSS_SECURITY_TOKEN):
            store_kwargs['session_token'] = properties.get(OssOptions.OSS_SECURITY_TOKEN)

        file_path_for_vortex = file_path_for_vortex.replace('oss://', 's3://')

    return file_path_for_vortex, store_kwargs


# Paimon never rewrites a data file in place, and every data file has a unique name. So a footer,
# read once, stays valid for the path, and a later open of the same file needs no footer IO.
_FOOTER_CACHE_SIZE = 1024
_footer_cache: "OrderedDict[str, Any]" = OrderedDict()
_store_cache: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Any] = {}
_cache_lock = threading.Lock()


def _store_for(url: str, store_kwargs: Dict[str, str]) -> Tuple[Any, str]:
    """Return a shared store for the bucket of ``url``, and the key of ``url`` in it.

    A store reaches every object in its bucket, so one client serves all files with the same
    credentials. Creating a client per file costs a connection setup every time.
    """
    from vortex import store

    parsed = urlparse(url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    key = parsed.path.lstrip('/')
    cache_key = (root, tuple(sorted((k, str(v)) for k, v in store_kwargs.items())))
    with _cache_lock:
        vortex_store = _store_cache.get(cache_key)
        if vortex_store is None:
            vortex_store = store.from_url(root, **store_kwargs)
            _store_cache[cache_key] = vortex_store
    return vortex_store, key


def _cached_footer(cache_key: str) -> Any:
    with _cache_lock:
        footer = _footer_cache.get(cache_key)
        if footer is not None:
            _footer_cache.move_to_end(cache_key)
        return footer


def _cache_footer(cache_key: str, footer: Any) -> None:
    with _cache_lock:
        _footer_cache[cache_key] = footer
        _footer_cache.move_to_end(cache_key)
        while len(_footer_cache) > _FOOTER_CACHE_SIZE:
            _footer_cache.popitem(last=False)


def open_vortex_file(file_io: FileIO, file_path: str) -> Any:
    """Open a Vortex data file of a Paimon table for reading.

    A local file is memory-mapped: the page cache serves its segments, with no read threads and
    no copies. A remote file is read through a shared store client. Either way, the footer of
    a file that was opened before comes from a cache.
    """
    import vortex

    file_path_for_vortex, store_kwargs = to_vortex_specified(file_io, file_path)
    footer = _cached_footer(file_path_for_vortex)

    if store_kwargs:
        vortex_store, key = _store_for(file_path_for_vortex, store_kwargs)
        vortex_file = vortex.open(key, store=vortex_store, footer=footer)
    elif urlparse(file_path_for_vortex).scheme:
        vortex_file = vortex.open(file_path_for_vortex, footer=footer)
    else:
        vortex_file = vortex.open(file_path_for_vortex, footer=footer, memory_map=True)

    if footer is None:
        _cache_footer(file_path_for_vortex, vortex_file.footer)
    return vortex_file


def write_vortex_file(file_io: FileIO, path: str, data: Any, compact: bool = False) -> None:
    """Write a pyarrow table as a Vortex file, through a shared store client when remote.

    ``compact`` selects the compact encodings, which make files smaller and reads slower.
    """
    import vortex
    from vortex.io import VortexWriteOptions

    options = VortexWriteOptions.compact() if compact else VortexWriteOptions.default()
    file_path_for_vortex, store_kwargs = to_vortex_specified(file_io, path)
    if store_kwargs:
        vortex_store, key = _store_for(file_path_for_vortex, store_kwargs)
        options.write(vortex.array(data), key, store=vortex_store)
    else:
        if not urlparse(file_path_for_vortex).scheme:
            os.makedirs(os.path.dirname(file_path_for_vortex), exist_ok=True)
        options.write(vortex.array(data), file_path_for_vortex)
