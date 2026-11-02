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

"""Python batch streams align authorization results before Arrow transport."""

import json

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.table_query_auth import TableQueryAuthResult


@pytest.mark.parametrize("method", [
    "to_arrow_batch_reader", "_to_managed_arrow_batch_reader",
])
@pytest.mark.parametrize("target, source, source_type, expected", [
    ("id", "text", "STRING", {"id": [7, 8], "text": ["7", "8"]}),
    ("text", "id", "INT", {"id": [100, 200], "text": ["100", "200"]}),
])
def test_python_stream_casts_mask_results_to_declared_schema(
        tmp_path, method, target, source, source_type, expected):
    catalog = CatalogFactory.create({"warehouse": str(tmp_path / "warehouse")})
    catalog.create_database("default", True)
    schema = pa.schema([("id", pa.int32()), ("text", pa.string())])
    catalog.create_table("default.masked", Schema.from_pyarrow_schema(
        schema, options={"native-read.enabled": "false", "read.batch-size": "1"}),
        False)
    table = catalog.get_table("default.masked")
    writer = table.new_batch_write_builder().new_write()
    committer = table.new_batch_write_builder().new_commit()
    try:
        writer.write_arrow(pa.table(
            {"id": [100, 200], "text": ["7", "8"]}, schema=schema))
        committer.commit(writer.prepare_commit())
    finally:
        writer.close()
        committer.close()
    auth = TableQueryAuthResult(None, {target: json.dumps({
        "name": "FIELD_REF",
        "fieldRef": {"index": schema.get_field_index(source),
                     "name": source, "type": source_type},
    })})
    table.catalog_environment.table_query_auth = (
        lambda options, identifier: lambda select: auth)
    builder = table.new_read_builder()
    splits = builder.new_scan().plan().splits()
    assert builder.new_read().to_arrow(splits).to_pydict() == expected

    with getattr(builder.new_read(), method)(splits, parallelism=1) as reader:
        batches = []
        for batch in reader:
            assert batch.schema.equals(reader.schema, check_metadata=True)
            batches.append(batch)
        assert pa.Table.from_batches(batches, schema=reader.schema).to_pydict() == expected
