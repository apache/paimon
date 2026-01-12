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

This sample shows how to:
1. Import blob data from external OSS into Paimon table using REST catalog
2. Handle different credentials for REST catalog (token) and external OSS (user credentials)
3. Use blob-as-descriptor mode for memory-efficient blob import

Key points:
- REST catalog uses token-based authentication (managed by RESTTokenFileIO)
- External OSS uses user-provided credentials (accessKeyId/accessKeySecret)
- FileIO.copy() ensures these configurations don't interfere with each other
"""
from pypaimon import CatalogFactory
import pyarrow as pa

from pypaimon import Schema
from pypaimon.table.row.blob import BlobDescriptor, Blob
from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options


def write_table_with_blob(catalog, video_file_path: str, external_oss_options: dict):
    database_name = 'pai_vla_demo'
    table_name = 'test_table_blob_' + str(int(__import__('time').time()))

    catalog.create_database(
        name=database_name,
        ignore_if_exists=True,
    )

    print('===========1: Creating table with blob column=============')
    pa_schema = pa.schema([
        ('text', pa.string()),
        ('videos', pa.list_(pa.string())),
        ('picture', pa.large_binary())  # Blob column
    ])

    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=None,
        primary_keys=None,
        options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-field': 'picture',  # Specify blob field
            'blob-as-descriptor': 'true'  # Enable blob-as-descriptor mode
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
    print(f"  Schema fields: {[f.name for f in table.table_schema.fields]}")
    print(f"  Schema types: {[str(f.type) for f in table.table_schema.fields]}")

    print('\n===========2: Accessing external OSS file with user credentials=============')
    print('Note: External OSS uses user-provided credentials (different from REST token)')
    try:
        external_file_io = FileIO(video_file_path, Options(external_oss_options))
        video_file_size = external_file_io.get_file_size(video_file_path)
        print(f"✓ External file accessible: {video_file_path}")
        print(f"  File size: {video_file_size / 1024 / 1024:.2f} MB")
        print(f"  Using credentials: accessKeyId={external_oss_options.get('fs.oss.accessKeyId', 'N/A')[:10]}...")
    except Exception as e:
        raise FileNotFoundError(
            f"Failed to access external OSS file: {video_file_path}\n"
            f"Error: {e}\n"
            f"Please check your external_oss_options credentials."
        ) from e

    print('\n===========3: Creating BlobDescriptor=============')
    print('BlobDescriptor contains reference to external file, not the file content itself')
    external_blob_uri = video_file_path
    blob_descriptor = BlobDescriptor(external_blob_uri, 0, video_file_size)
    descriptor_bytes = blob_descriptor.serialize()
    print(f"✓ BlobDescriptor created:")
    print(f"  URI: {external_blob_uri}")
    print(f"  Offset: 0")
    print(f"  Length: {video_file_size} bytes")
    print(f"  Descriptor size: {len(descriptor_bytes)} bytes (much smaller than file!)")

    print('\n===========4: Writing data to Paimon table=============')
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()

    table_write.write_arrow(pa.Table.from_pydict({
        'text': ['Sample video'],
        'videos': [['video1.mp4']],
        'picture': [descriptor_bytes]  # Store BlobDescriptor, not file content
    }, schema=pa_schema))

    table_commit.commit(table_write.prepare_commit())
    print("✓ Data committed successfully")
    print("  Blob data was streamed from external OSS into Paimon storage")
    table_write.close()
    table_commit.close()
    
    return f'{database_name}.{table_name}'


def read_table_with_blob(catalog, table_name: str):
    print('\n===========5: Reading table with blob=============')
    table = catalog.get_table(table_name)

    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    splits = table_scan.plan().splits()
    table_read = read_builder.new_read()
    
    result = table_read.to_arrow(splits)
    
    print(f"✓ Read {result.num_rows} rows")
    print(f"  Columns: {result.column_names}")
    
    picture_bytes_list = result.column('picture').to_pylist()
    
    print('\n===========6: Verifying blob data=============')
    for i, picture_bytes in enumerate(picture_bytes_list):
        if picture_bytes is None:
            print(f"Row {i}: picture is None")
            continue
            
        blob_descriptor = BlobDescriptor.deserialize(picture_bytes)
        print(f"Row {i}: BlobDescriptor:")
        print(f"  URI: {blob_descriptor.uri}")
        print(f"  Offset: {blob_descriptor.offset}")
        print(f"  Length: {blob_descriptor.length} bytes")
        
        from pypaimon.common.uri_reader import FileUriReader
        uri_reader = FileUriReader(table.file_io)
        blob = Blob.from_descriptor(uri_reader, blob_descriptor)
        
        blob_data = blob.to_data()
        print(f"  Blob data size: {len(blob_data) / 1024 / 1024:.2f} MB")
        print(f"  ✓ Blob data successfully read from Paimon storage")
    
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
        'dlf.access-key-id': "YOUR_DLF_ACCESS_KEY_ID",  # For token refresh
        'dlf.access-key-secret': "YOUR_DLF_ACCESS_KEY_SECRET",  # For token refresh
        'dlf.oss-endpoint': "oss-cn-hangzhou.aliyuncs.com",
    }

    print('=' * 70)
    print('REST Catalog Blob-as-Descriptor Sample')
    print('=' * 70)
    print('\nThis sample demonstrates:')
    print('1. Using REST catalog with token-based authentication')
    print('2. Importing blob data from external OSS with different credentials')
    print('3. How FileIO.copy() prevents credential confusion')
    print('=' * 70)
    
    catalog = CatalogFactory.create(catalog_options)
    
    try:
        table_name = write_table_with_blob(catalog, video_file_path, external_oss_options)
        result = read_table_with_blob(catalog, table_name)
        print('\n' + '=' * 70)
        print('✓ Test completed successfully!')
        print('=' * 70)
        print('\nKey takeaways:')
        print('- REST catalog uses token-based authentication (auto-refreshed)')
        print('- External OSS uses user-provided credentials (static)')
        print('- FileIO.copy() ensures these don\'t interfere with each other')
        print('- Blob-as-descriptor mode enables memory-efficient blob import')
    except Exception as e:
        print(f'\n✗ Error: {e}')
        raise

