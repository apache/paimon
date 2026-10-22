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


"""Python grouping for distributed row-ID plans, selected before execution."""

from pypaimon.execution.python.write.table_update import PythonTableUpdate
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.query_auth_split import QueryAuthSplit


def _file_groups(split):
    if isinstance(split, QueryAuthSplit):
        for group in _file_groups(split.split):
            yield QueryAuthSplit(group, split.auth_result)
    else:
        yield from PythonTableUpdate._predicate_update_file_groups([split])


def row_id_groups(plan):
    groups = []
    for planned in plan.splits():
        for split in _file_groups(planned):
            ranges = [file.row_id_range() for file in split.files
                      if file.first_row_id is not None and not DataFileMeta.is_blob_file(file.file_name)]
            if ranges:
                groups.append((min(r.from_ for r in ranges), max(r.to for r in ranges), split))
    return sorted(groups, key=lambda group: group[0])
