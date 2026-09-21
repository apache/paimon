---
title: "DLF BLOB Presigned URL Quick Start"
description: "Store a video in a DLF Paimon table, create a temporary OSS URL with DLF STS credentials, and submit it to Alibaba Cloud Model Studio."
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# DLF BLOB Presigned URL Quick Start

This guide creates a DLF Paimon table, writes a local MP4 file as a managed
BLOB, creates a temporary OSS URL, and sends that URL to Alibaba Cloud Model
Studio for video understanding. The application does not need permanent OSS
credentials: DLF returns temporary OSS STS credentials to PyPaimon and
PyPaimon refreshes them when required.

## Prerequisites

Prepare the following resources:

- A DLF catalog backed by OSS.
- A RAM identity that can access the DLF catalog and its data.
- The DLF Paimon REST endpoint and OSS endpoint for the catalog's region.
- A local MP4 file.
- A Model Studio API key if you want to run the final video-understanding call.
- PyPaimon 2.2 or later. PyPaimon 2.0 does not provide
  `Blob.to_presigned_url`.

Use the public DLF endpoint when running outside an Alibaba Cloud VPC. Inside
an authorized VPC, you can use the VPC endpoint instead:

| Network | DLF endpoint example |
| --- | --- |
| Public network | `https://dlfnext.cn-hangzhou.aliyuncs.com` |
| VPC | `http://cn-hangzhou-vpc.dlf.aliyuncs.com` |

The region in these examples is only a placeholder. Set the DLF endpoint, OSS
endpoint, and `dlf.region` to the region that owns your catalog.

## Install PyPaimon

Install PyPaimon with the OSS dependencies:

```shell
python -m pip install -U 'pypaimon[oss]>=2.2'
```

## Configure credentials

Export an AccessKey for a RAM identity that can access DLF. If the AccessKey is
an STS credential, also export its security token. The security-token variable
can be omitted when using a long-lived AccessKey.

```shell
export ALIBABA_CLOUD_ACCESS_KEY_ID='<access-key-id>'
export ALIBABA_CLOUD_ACCESS_KEY_SECRET='<access-key-secret>'
export ALIBABA_CLOUD_SECURITY_TOKEN='<security-token>'
export DASHSCOPE_API_KEY='<model-studio-api-key>'
```

Do not add these values to source control.

## Run the complete example

Copy the following code to `dlf_video_quickstart.py`. Change the values in the
configuration block, then run `python dlf_video_quickstart.py`.

```python
import os
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import requests

from pypaimon import CatalogFactory, Schema


# ---------- Change these values ----------
REGION_ID = "cn-hangzhou"
DLF_ENDPOINT = "https://dlfnext.cn-hangzhou.aliyuncs.com"
DLF_CATALOG = "my_dlf_catalog"
OSS_ENDPOINT = "oss-cn-hangzhou.aliyuncs.com"

DATABASE = "demo"
TABLE = "video_understanding"
VIDEO_FILE = "/absolute/path/to/video.mp4"

MODEL = "qwen3.8-flash"
URL_VALIDITY = timedelta(minutes=30)
# -----------------------------------------


# Connect to DLF. DLF supplies temporary OSS credentials for table data.
catalog_options = {
    "metastore": "rest",
    "uri": DLF_ENDPOINT,
    "warehouse": DLF_CATALOG,
    "dlf.region": REGION_ID,
    "token.provider": "dlf",
    "dlf.access-key-id": os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
    "dlf.access-key-secret": os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
    "dlf.oss-endpoint": OSS_ENDPOINT,
}
security_token = os.getenv("ALIBABA_CLOUD_SECURITY_TOKEN")
if security_token:
    catalog_options["dlf.security-token"] = security_token

catalog = CatalogFactory.create(catalog_options)

# Create an append-only table with one managed BLOB column.
catalog.create_database(DATABASE, ignore_if_exists=True)
table_id = "{}.{}".format(DATABASE, TABLE)
arrow_schema = pa.schema([
    ("id", pa.string()),
    ("file_name", pa.string()),
    ("content_type", pa.string()),
    ("video", pa.large_binary()),
])
schema = Schema.from_pyarrow_schema(
    arrow_schema,
    options={
        "bucket": "-1",
        "row-tracking.enabled": "true",
        "data-evolution.enabled": "true",
    },
)
catalog.create_table(table_id, schema, ignore_if_exists=True)
table = catalog.get_table(table_id)

# Write one local video. PyPaimon stores the large_binary value in managed
# BLOB storage and commits its descriptor with the table row.
video_path = Path(VIDEO_FILE)
row_id = uuid4().hex
with video_path.open("rb") as video_file:
    batch = pa.Table.from_pydict({
        "id": [row_id],
        "file_name": [video_path.name],
        "content_type": ["video/mp4"],
        "video": [video_file.read()],
    }, schema=arrow_schema)

write_builder = table.new_batch_write_builder()
writer = write_builder.new_write()
commit = write_builder.new_commit()
try:
    writer.write_arrow(batch)
    commit.commit(writer.prepare_commit())
finally:
    writer.close()
    commit.close()

# Read the row as a descriptor so the complete video is not loaded again.
table = catalog.get_table(table_id).copy({"blob-as-descriptor": "true"})
read_builder = table.new_read_builder()
predicate = read_builder.new_predicate_builder().equal("id", row_id)
read_builder = read_builder.with_filter(predicate)
splits = read_builder.new_scan().plan().splits()
rows = list(read_builder.new_read().to_iterator(splits))
if len(rows) != 1:
    raise RuntimeError("Expected exactly one video row, found {}".format(len(rows)))

video_position = arrow_schema.get_field_index("video")
blob = rows[0].get_blob(video_position)
video_url = blob.to_presigned_url(
    table.file_io,
    table.table_path,
    URL_VALIDITY,
)

# Submit the URL immediately. Do not print or persist it because it contains
# temporary authorization parameters.
response = requests.post(
    "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
    headers={
        "Authorization": "Bearer {}".format(os.environ["DASHSCOPE_API_KEY"]),
        "Content-Type": "application/json",
    },
    json={
        "model": MODEL,
        "messages": [{
            "role": "user",
            "content": [
                {
                    "type": "video_url",
                    "video_url": {"url": video_url},
                },
                {
                    "type": "text",
                    "text": "Describe this video in one short sentence.",
                },
            ],
        }],
    },
    timeout=180,
)
response.raise_for_status()
print(response.json()["choices"][0]["message"]["content"])
```

The example keeps the table after it exits. To remove the demo table, run:

```python
catalog.drop_table("demo.video_understanding", ignore_if_not_exists=True)
```

## Use an ECS instance role

On an ECS instance with an attached RAM role, replace the AccessKey options in
`catalog_options` with the ECS token loader:

```python
catalog_options["dlf.token-loader"] = "ecs"
catalog_options.pop("dlf.access-key-id", None)
catalog_options.pop("dlf.access-key-secret", None)
```

PyPaimon loads and refreshes the ECS role credential. DLF separately supplies
the temporary OSS credential used to read the BLOB and create the URL.

## How the temporary URL works

A managed BLOB descriptor can point to a byte range inside a larger Paimon
`.blob` object. PyPaimon validates that the descriptor belongs to the source
table, copies exactly that range to an OSS object under the table directory,
and signs a temporary HTTPS GET URL with the current DLF-issued OSS STS
credential. Repeated calls for the same descriptor reuse the materialized
object and create a new URL. If OSS server-side encryption is configured for
the catalog, PyPaimon applies the same setting to the materialized object and
does not reuse a cached object whose encryption setting is incompatible.

The URL has no media suffix and the materialized object uses
`application/octet-stream`. Keep the original file name and media type in table
columns as shown above. For Model Studio, send an MP4 URL using the
`video_url` content type; the service reads the video bytes from the URL.

Treat the URL as a bearer credential. Use a short validity period, submit it to
the consumer immediately, and do not write it or its query parameters to logs.

## Troubleshooting

| Error | Action |
| --- | --- |
| `No module named oss2` | Install `pypaimon[oss]` in the Python environment that runs the script. |
| DLF request returns 401 or 403 | Check the DLF endpoint, region, AccessKey or STS token, and DLF permissions. |
| OSS operation returns 403 | Check that the DLF identity can access the table data and that `dlf.oss-endpoint` matches the OSS region. |
| Model Studio cannot fetch the URL | Use a public HTTPS OSS endpoint and a validity period long enough for the model request. |
| The model rejects the media | Confirm that the selected model accepts `video_url` and supports the video's container and codecs. |

See [BLOB Storage](./blob) for the Python BLOB API and
[BLOB References](../multimodal-table/blob-references#presigned-urls-for-oss-blobs)
for descriptor and presigned-URL semantics.
