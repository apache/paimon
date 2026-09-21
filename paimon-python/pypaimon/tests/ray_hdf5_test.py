# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.ray.hdf5 import _TransformHdf5File


h5py = pytest.importorskip("h5py")


def test_ray_hdf5_worker_uses_shared_file_transform():
    schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])
    expected = pa.table({"value": [1]}, schema=schema)
    transform = object()
    worker = _TransformHdf5File(
        transform=transform,
        source_options={},
        target_schema=schema,
    )

    with patch(
            "pypaimon.multimodal.hdf5._transform_hdf5_file",
            return_value=iter([expected])) as shared_transform:
        actual = list(worker(pa.table({"path": ["file:///tmp/source.h5"]})))

    assert len(actual) == 1
    assert actual[0].equals(expected)
    args = shared_transform.call_args.args
    assert args[0].path == "file:///tmp/source.h5"
    assert args[1] is transform
    assert args[4] == schema


def test_ray_hdf5_worker_requires_one_source_per_batch():
    worker = _TransformHdf5File(
        transform=object(),
        source_options={},
        target_schema=pa.schema([pa.field("value", pa.int64())]),
    )

    with pytest.raises(ValueError, match="requires one source per batch"):
        list(worker(pa.table({"path": ["a.h5", "b.h5"]})))


def test_ray_hdf5_worker_filters_empty_transform_batches():
    schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])
    empty = pa.table({"value": pa.array([], type=pa.int64())}, schema=schema)
    expected = pa.table({"value": [1]}, schema=schema)
    worker = _TransformHdf5File(
        transform=object(),
        source_options={},
        target_schema=schema,
    )

    with patch(
            "pypaimon.multimodal.hdf5._transform_hdf5_file",
            return_value=iter([empty, expected, empty])):
        actual = list(worker(pa.table({"path": ["file:///tmp/source.h5"]})))

    assert len(actual) == 1
    assert actual[0].equals(expected)
