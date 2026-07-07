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

from pypaimon.common.options.core_options import CoreOptions


def create_mosaic_writer_options(options: CoreOptions):
    import mosaic

    compression = options.file_compression()
    if compression is not None and compression.lower() != "zstd":
        raise ValueError(f"Mosaic format only supports zstd compression, but got: {compression}")

    kwargs = {
        "zstd_level": options.file_compression_zstd_level(),
    }

    num_buckets = options.mosaic_num_buckets()
    if num_buckets is not None:
        kwargs["num_buckets"] = num_buckets

    block_size = options.file_block_size()
    if block_size is not None:
        kwargs["row_group_max_size"] = block_size.get_bytes()

    stats_columns = options.mosaic_stats_columns()
    if stats_columns:
        kwargs["stats_columns"] = stats_columns

    return mosaic.WriterOptions(**kwargs)
