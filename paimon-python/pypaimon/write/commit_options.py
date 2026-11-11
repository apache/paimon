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

"""Validate commit configuration without creating either execution backend."""


def validate_commit_options(options):
    # These values were parsed when constructing the Python committer. Keep
    # invalid configuration failures before writers can prepare data, including
    # when commit execution will use Rust or remain lazy until publication.
    from pypaimon.manifest import avro_codec

    avro_codec(options.manifest_compression())
    options.manifest_target_size()
    options.commit_max_retries()
    options.commit_timeout()
    options.commit_min_retry_wait()
    options.commit_max_retry_wait()
    options.data_evolution_row_id_conflict_rewrite_max_size()
    options.data_evolution_enabled()
