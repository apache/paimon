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
#  limitations under the License.
################################################################################

import logging
import pyarrow as pa

from pypaimon.catalog.catalog_factory import CatalogFactory

# Enable debug logging for catalog operations
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(name)s: %(message)s')
from pypaimon.schema.schema import Schema
from pypaimon.table.row.blob import BlobDescriptor, Blob


def oss_blob_as_descriptor():
    warehouse = 'oss://<your-bucket>/<warehouse-path>'
    catalog = CatalogFactory.create({
        'warehouse': warehouse,
        'fs.oss.endpoint': 'oss-<your-region>.aliyuncs.com',
        'fs.oss.accessKeyId': '<your-ak>',
        'fs.oss.accessKeySecret': '<your-sk>',
        'fs.oss.region': '<your-region>'
    })

    pa_schema = pa.schema([
        ('id', pa.int32()),
        ('blob_data', pa.large_binary()),
    ])

    schema = Schema.from_pyarrow_schema(
        pa_schema,
        options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-as-descriptor': 'true',
            'target-file-size': '100MB'
        }
    )

    catalog.create_database("test_db", True)
    catalog.create_table("test_db.blob_uri_scheme_test", schema, True)
    table = catalog.get_table("test_db.blob_uri_scheme_test")

    # Create external blob file in OSS
    external_blob_uri = f"{warehouse.rstrip('/')}/external_blob_scheme_test.bin"
    blob_content = b'This is external blob data'

    with table.file_io.new_output_stream(external_blob_uri) as out_stream:
        out_stream.write(blob_content)

    # Create BlobDescriptor with OSS scheme
    blob_descriptor = BlobDescriptor(external_blob_uri, 0, len(blob_content))
    descriptor_bytes = blob_descriptor.serialize()

    # Write the descriptor bytes to the table
    test_data = pa.Table.from_pydict({
        'id': [1],
        'blob_data': [descriptor_bytes]
    }, schema=pa_schema)

    write_builder = table.new_batch_write_builder()
    writer = write_builder.new_write()
    writer.write_arrow(test_data)
    commit_messages = writer.prepare_commit()
    commit = write_builder.new_commit()
    commit.commit(commit_messages)
    writer.close()

    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    table_read = read_builder.new_read()
    result = table_read.to_arrow(table_scan.plan().splits())

    picture_bytes = result.column('blob_data').to_pylist()[0]
    new_blob_descriptor = BlobDescriptor.deserialize(picture_bytes)

    print(f"Original URI: {external_blob_uri}")
    print(f"Read URI: {new_blob_descriptor.uri}")
    assert new_blob_descriptor.uri.startswith('oss://'), \
        f"URI scheme should be preserved. Got: {new_blob_descriptor.uri}"

    from pypaimon.common.uri_reader import FileUriReader
    uri_reader = FileUriReader(table.file_io)
    blob = Blob.from_descriptor(uri_reader, new_blob_descriptor)

    blob_descriptor_from_blob = blob.to_descriptor()
    print(f"Blob descriptor URI from Blob.from_descriptor: {blob_descriptor_from_blob.uri}")

    read_data = blob.to_data()
    assert read_data == blob_content, "Blob data should match original content"

    print("✅ All assertions passed!")


if __name__ == '__main__':
    oss_blob_as_descriptor()
