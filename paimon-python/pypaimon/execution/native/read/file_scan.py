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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Lossless metadata adaptation for Core file scans."""

from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.file_store_path_factory import canonical_data_file_path
from pypaimon.write.commit_message_serializer import deserialize_commit_message


def from_native_file_plan(table, plan):
    entries = []
    for entry in plan.files():
        # Reuse the existing v14 file-metadata bridge, including historical
        # stats, external paths, BLOB descriptors and row-tracking metadata.
        message = deserialize_commit_message(
            entry._to_commit_message().serialize(), table.partition_keys_fields,
            table.trimmed_primary_keys_fields)
        file = message.new_files[0]
        file.file_path = (file.physical_path() if file.external_path else canonical_data_file_path(
            table, message.partition, entry.bucket, file.file_name))
        entries.append(ManifestEntry(
            entry.kind, GenericRow(list(message.partition), table.partition_keys_fields),
            entry.bucket, entry.total_buckets, file))
    return entries, plan.snapshot_id()
