import os
from pypaimon import CatalogFactory
import pyarrow as pa

from pypaimon import Schema
from pypaimon.table.row.blob import BlobDescriptor, Blob
from pypaimon.common.file_io import FileIO


def write_table_with_blob(catalog, external_oss_options: dict, database_name, table_name):
    # table_name = 'test_table_blob_' + str(int(__import__('time').time()))

    catalog.create_database(
        name=database_name,
        ignore_if_exists=True,
    )

    print('===========1: Creating table with blob column=============')
    pa_schema = pa.schema([
        ('text', pa.string()),
        # ('video_path', pa.list_(pa.string())),
        # ('video_bytes', pa.list_(pa.large_binary()))  # Blob column
        ('video_path', pa.string()),
        ('video_bytes', pa.large_binary())
    ])

    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=None,
        primary_keys=None,
        options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
            'blob-field': 'video_bytes',  # Specify blob field
            'blob-as-descriptor': 'true'  # Enable blob-as-descriptor mode
        },
        comment='my test table with blob')

    table_identifier = f'{database_name}.{table_name}'
    catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        ignore_if_exists=True
    )

    table = catalog.get_table(table_identifier)
    print(f"Table schema fields: {[f.name for f in table.table_schema.fields]}")
    print(f"Table schema field types: {[str(f.type) for f in table.table_schema.fields]}")

    from pypaimon.schema.data_types import PyarrowFieldParser
    table_pa_schema = PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)
    print(f"Table PyArrow schema: {table_pa_schema}")
    print(f"Input PyArrow schema: {pa_schema}")

    print('===========2: Processing video file from OSS=============')

    def to_blob_data(video_path):
        try:
            from pypaimon.common.file_io import FileIO
            external_file_io = FileIO(video_path, external_oss_options)
            video_file_size = external_file_io.get_file_size(video_path)
            # print(f"Video file size: {video_file_size / 1024 / 1024:.2f} MB")
        except Exception as e:
            raise FileNotFoundError(f"Failed to access video file from OSS: {video_path}, error: {e}")

        # print('===========3: Creating BlobDescriptor=============')
        external_blob_uri = video_path
        # print(f"Using OSS path as blob URI: {external_blob_uri}")

        blob_descriptor = BlobDescriptor(external_blob_uri, 0, video_file_size)
        descriptor_bytes = blob_descriptor.serialize()
        # print(f"BlobDescriptor created: URI={external_blob_uri}, Length={video_file_size}")
        return descriptor_bytes

    print('===========4: Writing data to table=============')
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()

    data_dir = '/mnt/data/data/Youku-AliceMind/caption/validation/videos'
    videos_list = os.listdir(data_dir)
    print(f'>>>>>>found {len(videos_list)} videos')

    videos_list = [os.path.join(data_dir, video) for video in videos_list]
    data_dict = {
        'text': [''] * len(videos_list),
        'video_path': videos_list,
        'video_bytes': [to_blob_data(v) for v in videos_list]
    }

    table_write.write_arrow(pa.Table.from_pydict(
        # {
        # 'text': ['test1'], 'video_path': [['video1.mp4']], 'video_bytes': [descriptor_bytes]
        # },
        data_dict,
        schema=pa_schema))

    table_commit.commit(table_write.prepare_commit())
    print("Data committed successfully")
    table_write.close()
    table_commit.close()

    return f'{database_name}.{table_name}'


def read_table_with_blob(catalog, table_name: str):
    print('===========5: Reading table with blob=============')
    table = catalog.get_table(table_name)

    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    splits = table_scan.plan().splits()
    table_read = read_builder.new_read()

    result = table_read.to_arrow(splits)

    print(f"Read {result.num_rows} rows")
    print(f"Columns: {result.column_names}")

    return

    video_bytes_bytes_list = result.column('video_bytes').to_pylist()

    print('===========6: Verifying blob data=============')
    for i, video_bytes_bytes in enumerate(video_bytes_bytes_list):
        if video_bytes_bytes is None:
            print(f"Row {i}: video_bytes is None")
            continue

        blob_descriptor = BlobDescriptor.deserialize(video_bytes_bytes)
        print(f"Row {i}: BlobDescriptor URI={blob_descriptor.uri}, "
              f"Offset={blob_descriptor.offset}, Length={blob_descriptor.length}")

        from pypaimon.common.uri_reader import FileUriReader
        uri_reader = FileUriReader(table.file_io)
        blob = Blob.from_descriptor(uri_reader, blob_descriptor)

        blob_data = blob.to_data()
        print(f"Row {i}: Blob data size: {len(blob_data) / 1024 / 1024:.2f} MB")

        blob_descriptor_from_blob = blob.to_descriptor()
        print(f"Row {i}: BlobDescriptor from Blob: URI={blob_descriptor_from_blob.uri}")

    return result


def read_table_to_ray_ds(catalog, table_name: str):
    print('===========5: Reading table with blob=============')
    table = catalog.get_table(table_name)

    read_builder = table.new_read_builder()
    table_scan = read_builder.new_scan()
    splits = table_scan.plan().splits()

    # convert the splits into a Ray Dataset and handle it by Ray Data API for distributed processing
    table_read = read_builder.new_read()
    ray_dataset = table_read.to_ray(splits)

    print(f'===================total samples: {ray_dataset.count_rows()}====================')
    # return
    i = 0

    path_keys = []
    for item in ray_dataset.iter_rows():
        path_keys.append(item['video_path'])
        continue

        print(f'===================item: {item}====================')
        video_bytes = item['video_bytes']

        i += 1
        blob_descriptor = BlobDescriptor.deserialize(video_bytes)
        print(f"Row {i}: BlobDescriptor URI={blob_descriptor.uri}, "
              f"Offset={blob_descriptor.offset}, Length={blob_descriptor.length}")

        from pypaimon.common.uri_reader import FileUriReader
        uri_reader = FileUriReader(table.file_io)
        blob = Blob.from_descriptor(uri_reader, blob_descriptor)

        blob_data = blob.to_data()
        print(f"Row {i}: Blob data size: {len(blob_data) / 1024 / 1024:.2f} MB")

        blob_descriptor_from_blob = blob.to_descriptor()
        print(f"Row {i}: BlobDescriptor from Blob: URI={blob_descriptor_from_blob.uri}")

        import av
        import io
        container = av.open(io.BytesIO(blob_data))
        stream = container.streams.video[0]
        print(f'======================duration: {stream.duration * stream.time_base} {stream.frames}==================')
        break

    print(f'========= path keys: {len(path_keys)} {len(list(set(path_keys)))}')


if __name__ == '__main__':
    # video_file_path = "oss://pai-test-beijing/ximo-test/data/Youku-AliceMind/caption/validation/videos/video_00000000_0.mp4"
    video_file_path = "/mnt/data_cpfs/data/Youku-AliceMind/caption/validation/videos/video_00000000_0.mp4"

    external_oss_options = {
        'fs.oss.accessKeyId': os.environ['accessKeyID'],
        'fs.oss.accessKeySecret': os.environ['accessKeySecret'],
        'fs.oss.endpoint': "oss-cn-beijing.aliyuncs.com",
        'fs.oss.region': "cn-beijing",
    }

    catalog_options = {
        'metastore': 'rest',
        'uri': "http://cn-beijing-vpc.dlf.aliyuncs.com",
        'warehouse': "pai_vla_demo",
        'dlf.region': 'cn-beijing',
        "token.provider": "dlf",
        'dlf.access-key-id': os.environ['accessKeyID'],
        'dlf.access-key-secret': os.environ['accessKeySecret'],
        'dlf.oss-endpoint': "oss-cn-beijing.aliyuncs.com",
        **external_oss_options
    }

    catalog = CatalogFactory.create(catalog_options)
    # print('===========0: Starting blob test with REST catalog=============')

    database_name = 'pai_vla_demo'
    table_name = 'demo_data'

    # try:
    #   catalog.drop_table(f'{database_name}.{table_name}')
    #   print(f'>>>> remove table: {database_name}.{table_name}')
    # except:
    #   print(f'>>>> table: {database_name}.{table_name} does not exist')

    # table_name = write_table_with_blob(catalog, external_oss_options, database_name, table_name)

    table_name = 'pai_vla_demo.demo_data'
    result = read_table_with_blob(catalog, table_name)
    read_table_to_ray_ds(catalog, table_name)

    # print('===========7: Test completed successfully!=============')
