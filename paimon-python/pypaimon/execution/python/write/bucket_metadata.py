################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Python live-file metadata lookup used by persistent input assignment."""


def dynamic_bucket_data_buckets(table, partition, snapshot):
    from pypaimon.index.dynamic_bucket import bucket_read_builder

    read_builder = bucket_read_builder(table, partition, snapshot)
    # This internal manifest lookup returns no rows and must include every
    # live bucket, independently of a SELECT policy or level-zero read option.
    scanner = read_builder.new_scan().file_scanner
    scanner.skip_level0 = False
    return {entry.bucket for entry in scanner.plan_files()}
