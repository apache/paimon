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

from typing import Optional

import pyarrow as pa

from pypaimon.function.builtins import make_first_frame
from pypaimon_rust.datafusion import udf


def register_first_frame(
    ctx,
    catalog_options: Optional[dict] = None,
    name: str = "first_frame",
    image_format: str = "JPEG",
) -> None:
    ctx.register_udf(
        udf(
            make_first_frame(catalog_options, image_format),
            [pa.binary()],
            pa.binary(),
            "volatile",
            name,
        )
    )


def register_builtins(ctx, catalog_options: Optional[dict] = None) -> None:
    register_first_frame(ctx, catalog_options)
