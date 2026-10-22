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

from pypaimon.write.bucket_planning import PostponeBucketPlanning


class PythonPostponeBucketPlanner(PostponeBucketPlanning):
    """Load Python metadata before applying shared bucket calculations."""

    @staticmethod
    def _load_bucket_metadata(table):
        from pypaimon.execution.python.read.table_scan import PythonTableScan
        scan = PythonTableScan(table.new_read_builder().new_scan())._file_scanner()
        manifest_files, _ = scan.manifest_scanner()
        entries = scan.manifest_file_manager.read_entries_parallel(
            manifest_files,
            max_workers=table.options.scan_manifest_parallelism(),
        )

        known = {}
        for entry in entries:
            partition = tuple(entry.partition.values)
            if entry.bucket >= 0 and entry.total_buckets > 0:
                previous = known.get(partition)
                if previous is not None and previous != entry.total_buckets:
                    raise RuntimeError(
                        "Partition {} has different total buckets {} and {}"
                        .format(partition, previous, entry.total_buckets)
                    )
                known[partition] = entry.total_buckets
        return known
