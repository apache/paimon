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

import pyarrow as pa

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema


def oss_read_and_write():
    warehouse = 'oss://<your-bucket>/<warehouse-path>'
    catalog = CatalogFactory.create({
        'warehouse': warehouse,
        'fs.oss.endpoint': 'oss-<your-region>.aliyuncs.com',
        'fs.oss.accessKeyId': '<your-ak>',
        'fs.oss.accessKeySecret': '<your-sk>',
        'fs.oss.region': '<your-region>'
    })
    simple_pa_schema = pa.schema([
        ('f0', pa.int32()),
        ('f1', pa.string()),
        ('f2', pa.string())
    ])
    data = {
        'f0': [1, 2, 3],
        'f1': ['a', 'b', 'c'],
        'f2': ['X', 'Y', 'Z']
    }
    catalog.create_database("test_db", False)
    catalog.create_table("test_db.test_table", Schema.from_pyarrow_schema(simple_pa_schema), False)
    table = catalog.get_table("test_db.test_table")

    # write data
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    write_data = pa.Table.from_pydict(data, simple_pa_schema)
    table_write.write_arrow(write_data)
    commit_messages = table_write.prepare_commit()
    table_commit.commit(commit_messages)
    table_write.close()
    table_commit.close()

    # read data
    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    table_read = read_builder.new_read()
    splits = table_scan.plan().splits()

    result = table_read.to_arrow(splits)
    print(result)


if __name__ == '__main__':
    oss_read_and_write()
