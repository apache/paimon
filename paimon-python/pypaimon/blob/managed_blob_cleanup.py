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

"""Reference-aware cleanup of shared primary-key BLOB packs."""

import logging

from pypaimon.blob.managed_blob_payload import resolve_table_blob_uri
from pypaimon.blob.managed_blob_reference_file import ManagedBlobReferenceFile
from pypaimon.utils.path import path_identity, to_file_io_path

logger = logging.getLogger(__name__)


def _reference_identities(table, reference):
    uri = reference.storage_root_id + '/' + reference.relative_path
    # Java Path strings contain literal URI characters. Older Python references
    # may instead contain encoded file URIs or table-relative paths. A surviving
    # reference must protect every possible interpretation of that path.
    paths = {uri, resolve_table_blob_uri(table.table_path, uri)}
    return {path_identity(candidate, table.file_io)
            for path in paths for candidate in (path, to_file_io_path(path))}


def managed_blob_deletion_paths(table, paths):
    """Keep shared packs while deleting exclusively owned data files and sidecars."""
    deleting = list(paths)
    deleting.extend(_managed_pack_paths(table, deleting))
    protected = _protected_managed_packs(table, deleting)
    return [path for path in deleting if path_identity(path, table.file_io) not in protected]


def _managed_pack_paths(table, paths):
    """Collect colocated pack candidates before deleting aborted sidecars."""
    packs = set()
    for path in paths:
        path = str(path)
        if not path.endswith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX):
            continue
        try:
            references = ManagedBlobReferenceFile.read(table.file_io, path)
            parent = path_identity(path, table.file_io).rpartition('/')[0]
            for reference in references:
                for pack in _reference_identities(table, reference):
                    if (pack.rpartition('/')[0] == parent
                            and pack.endswith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)):
                        packs.add(pack)
        except Exception as error:
            logger.warning(
                "Failed to read managed BLOB references from %s during abort: %s",
                path, error)
    return packs


def _protected_managed_packs(table, paths):
    """Keep a pack when a blobref outside this abort still names it.

    One pack can be referenced by several data files. Deleting it
    with the first aborted file would make the other files unreadable.
    """
    pack_suffix = ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX
    ref_suffix = ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX
    deleting = {path_identity(path, table.file_io) for path in paths}
    packs_by_dir = {}
    for path in deleting:
        if not path.endswith(pack_suffix):
            continue
        parent, _, name = path.rpartition("/")
        if parent and name:
            packs_by_dir.setdefault(parent, set()).add(path)
    if not packs_by_dir:
        return set()

    list_status = getattr(table.file_io, "list_status", None)
    if list_status is None:
        return set(path for packs in packs_by_dir.values() for path in packs)

    protected = set()
    for parent, packs in packs_by_dir.items():
        try:
            statuses = list(list_status(parent))
        except Exception as error:
            logger.warning(
                "Failed to list %s while checking managed BLOB packs: %s",
                parent,
                error,
            )
            # Discovery did not finish. Deleting the packs could remove
            # payloads that a committed file still references.
            protected.update(packs)
            continue
        for status in statuses:
            name = getattr(status, "base_name", None)
            if not name:
                path = getattr(status, "path", None) or getattr(status, "original_path", None)
                name = str(path).replace("\\", "/").rsplit("/", 1)[-1] if path else None
            if not name:
                protected.update(packs)
                break
            if not str(name).endswith(ref_suffix):
                continue
            blobref_path = parent + "/" + str(name)
            if blobref_path in deleting:
                continue
            try:
                references = ManagedBlobReferenceFile.read(table.file_io, blobref_path)
                referenced = {identity for reference in references
                              for identity in _reference_identities(table, reference)}
            except Exception as error:
                logger.warning(
                    "Failed to read %s while checking managed BLOB packs: %s",
                    blobref_path,
                    error,
                )
                protected.update(packs)
                break
            protected.update(packs & referenced)
    return protected
