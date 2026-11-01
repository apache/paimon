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

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions


def create_parquet_writer_options(options: CoreOptions):
    enabled = options.parquet_write_page_index_enabled()
    if int(pa.__version__.split('.')[0]) >= 13:
        return {'write_page_index': True if enabled is None else enabled}
    if enabled:
        raise ValueError(
            "parquet.write-page-index.enabled requires PyArrow >= 13, got {}".format(pa.__version__))
    # Older PyArrow versions do not accept even write_page_index=False.
    return {}
