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

"""High-level APIs for mutable multimodal Paimon tables."""

from pypaimon.multimodal.blob_store import (
    BlobObject,
    BlobStore,
    NoSuchKey,
    ObjectInfo,
    PutObjectResult,
)
from pypaimon.multimodal.connection import MultimodalConnection, connect
from pypaimon.multimodal.table import (
    MultimodalTable,
    TextRoute,
    VectorRoute,
    text_route,
    vector_route,
)
from pypaimon.table.data_evolution_merge_into import (
    lit,
    source_col,
    target_col,
)

__all__ = [
    "BlobObject",
    "BlobStore",
    "MultimodalConnection",
    "MultimodalTable",
    "NoSuchKey",
    "ObjectInfo",
    "PutObjectResult",
    "TextRoute",
    "VectorRoute",
    "connect",
    "lit",
    "source_col",
    "target_col",
    "text_route",
    "vector_route",
]
