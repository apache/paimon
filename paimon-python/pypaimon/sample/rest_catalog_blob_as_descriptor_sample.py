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
"""
Sample demonstrating how to use blob-as-descriptor mode with REST catalog.
"""
from pypaimon import CatalogFactory
import pyarrow as pa

from pypaimon import Schema
from pypaimon.table.row.blob import BlobDescriptor, Blob
from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options


def write_table_with_blob(catalog, video_file_path: str, external_oss_options: dict):
    database_name = 'blob_demo'
    table_name = 'test_table_blob_' + str(int(__import__('time').time()))

    catalog.create_database(
        name=database_name,
        ignore_if_exists=True,
    )

    pa_schema = pa.schema([
        ('text', pa.string()),
        ('names', pa.list_(pa.string())),
        ('video', pa.large_binary())  # Blob column
    ])

    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=None,
        primary_keys=None,
        options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-field': 'video',
            'blob-as-descriptor': 'true'
        },
        comment='Table with blob column using blob-as-descriptor mode')

    table_identifier = f'{database_name}.{table_name}'
    catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        ignore_if_exists=True
    )

    table = catalog.get_table(table_identifier)
    print(f"✓ Table created: {table_identifier}")

    # Access external OSS file to get file size
    try:
        external_file_io = FileIO(video_file_path, Options(external_oss_options))
        video_file_size = external_file_io.get_file_size(video_file_path)
    except Exception as e:
        raise FileNotFoundError(
            f"Failed to access external OSS file: {video_file_path}\n"
            f"Error: {e}\n"
            f"Please check your external_oss_options credentials."
        ) from e

    # Create BlobDescriptor
    blob_descriptor = BlobDescriptor(video_file_path, 0, video_file_size)
    descriptor_bytes = blob_descriptor.serialize()
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()

    table_write.write_arrow(pa.Table.from_pydict({
        'text': ['Sample video'],
        'names': [['video1.mp4']],
        'video': [descriptor_bytes]
    }, schema=pa_schema))

    table_commit.commit(table_write.prepare_commit())
    print("✓ Data committed successfully")
    table_write.close()
    table_commit.close()
    
    return f'{database_name}.{table_name}'


def read_table_with_blob(catalog, table_name: str):
    table = catalog.get_table(table_name)

    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    splits = table_scan.plan().splits()
    table_read = read_builder.new_read()
    
    result = table_read.to_arrow(splits)
    print(f"✓ Read {result.num_rows} rows")
    
    video_bytes_list = result.column('video').to_pylist()
    for video_bytes in video_bytes_list:
        if video_bytes is None:
            continue
        blob_descriptor = BlobDescriptor.deserialize(video_bytes)
        from pypaimon.common.uri_reader import FileUriReader
        uri_reader = FileUriReader(table.file_io)
        blob = Blob.from_descriptor(uri_reader, blob_descriptor)
        blob_data = blob.to_data()
        print(f"✓ Blob data verified: {len(blob_data) / 1024 / 1024:.2f} MB")
        break
    
    return result


if __name__ == '__main__':
    external_oss_options = {
        'fs.oss.accessKeyId': "YOUR_EXTERNAL_OSS_ACCESS_KEY_ID",
        'fs.oss.accessKeySecret': "YOUR_EXTERNAL_OSS_ACCESS_KEY_SECRET",
        'fs.oss.endpoint': "oss-cn-hangzhou.aliyuncs.com",
        'fs.oss.region': "cn-hangzhou",
    }
    
    video_file_path = "oss://your-bucket/blob_test/video.mov"
    
    catalog_options = {
        'metastore': 'rest',
        'uri': "http://your-rest-catalog-uri",
        'warehouse': "your_warehouse",
        'dlf.region': 'cn-hangzhou',
        "token.provider": "dlf",
        'dlf.access-key-id': "YOUR_DLF_ACCESS_KEY_ID",
        'dlf.access-key-secret': "YOUR_DLF_ACCESS_KEY_SECRET",
        'dlf.oss-endpoint': "oss-cn-hangzhou.aliyuncs.com",
        **external_oss_options
    }

    catalog = CatalogFactory.create(catalog_options)
    
    try:
        table_name = write_table_with_blob(catalog, video_file_path, external_oss_options)
        result = read_table_with_blob(catalog, table_name)
        print("✓ Test completed successfully!")
    except Exception as e:
        print(f'✗ Error: {e}')
        raise

