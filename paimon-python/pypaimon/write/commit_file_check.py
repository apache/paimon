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

"""Validation of prepared files before recovering an uncommitted write."""

from copy import copy
from itertools import chain

from pypaimon.utils.file_store_path_factory import canonical_data_file_path
from pypaimon.utils.path import to_file_io_path


def check_commit_files(table, entries, changelog_entries=(), index_entries=()) -> None:
    """Check ADD files without changing metadata or deleting prepared files.

    Callers must establish that the commit is not already published before
    checking. Existence checks do not fence concurrent removal or rollback.
    """
    checked_paths = set()

    def check_path(path):
        path = to_file_io_path(path)
        if path in checked_paths:
            return
        if not table.file_io.exists(path):
            raise RuntimeError(
                "Cannot recover commit for table {}: required file is missing: {}".format(
                    table.identifier, path))
        checked_paths.add(path)

    for entry in chain(entries, changelog_entries):
        if entry.kind != 0:
            continue
        data_file = entry.file
        if not data_file.physical_path():
            data_file = copy(data_file)
            data_file.file_path = canonical_data_file_path(
                table, tuple(entry.partition.values), entry.bucket, data_file.file_name)
        for path in data_file.collect_files():
            check_path(path)

    for entry in index_entries:
        if entry.kind != 0:
            continue
        index_file = entry.index_file
        if index_file.index_type in ('DELETION_VECTORS', 'HASH'):
            path = table.path_factory().bucket_index_path(
                tuple(entry.partition.values), entry.bucket, index_file)
        else:
            path = (index_file.external_path
                    or table.path_factory().global_index_path_factory().to_path(index_file.file_name))
        check_path(path)
