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

Read a video from a DLF multimodal table, create an OSS presigned URL, and submit
it to Alibaba Cloud Model Studio. DLF supplies the temporary OSS credentials.

## Setup

Use PyPaimon with the multimodal and presigned-URL APIs (introduced in 2.2),
a DLF catalog, a RAM identity with table access, and a Model Studio API key.
Run this example inside a VPC that can reach DLF and OSS.

```shell
python -m pip install -U 'pypaimon[oss]>=2.2' openai
export ALIBABA_CLOUD_ACCESS_KEY_ID='<access-key-id>'
export ALIBABA_CLOUD_ACCESS_KEY_SECRET='<access-key-secret>'
# Set only when using RAM STS credentials:
export ALIBABA_CLOUD_SECURITY_TOKEN='<security-token>'
export DASHSCOPE_API_KEY='<model-studio-api-key>'
```

Run the following sections in one script or notebook. Skip table creation if
using existing data.

## Connect to DLF

Replace the region, catalog, database, and table names. OSS uses the Internal
endpoint supplied by DLF; no `dlf.oss-endpoint` override is needed.

```python
import os
from pypaimon.multimodal import connect

DATABASE = "demo"
TABLE = "video_understanding"
options = {
    "metastore": "rest",
    "uri": "http://cn-hangzhou-vpc.dlf.aliyuncs.com",
    "warehouse": "my_dlf_catalog",
    "dlf.region": "cn-hangzhou",
    "token.provider": "dlf",
    "dlf.access-key-id": os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
    "dlf.access-key-secret": os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
}
if os.getenv("ALIBABA_CLOUD_SECURITY_TOKEN"):
    options["dlf.security-token"] = os.environ["ALIBABA_CLOUD_SECURITY_TOKEN"]

conn = connect(database=DATABASE, options=options)
```

For public-network access, add these overrides **before** `connect()`:

```python
options["uri"] = "https://dlfnext.cn-hangzhou.aliyuncs.com"
options["dlf.oss-endpoint"] = "oss-cn-hangzhou.aliyuncs.com"
```

## Create and populate a table (optional)

```python
from pathlib import Path
import pyarrow as pa

table = conn.create_table(TABLE, schema=pa.schema([
    ("id", pa.string()),
    ("video", pa.large_binary()),
]))
table.add([{
    "id": "video-001",
    "video": Path("/absolute/path/to/video.mp4").read_bytes(),
}])
```

## Read a video and call Model Studio

For an existing table, adjust the column names and video ID. The table must
have `data-evolution.enabled=true`, no primary key, a BLOB video column, and a
unique value for the selected ID. Set the API base URL and video-capable model
to match your Model Studio deployment.

```python
from datetime import timedelta
from openai import OpenAI

table = conn.get_table(TABLE)
video = table.blobs(column="video", key_column="id").get_object("video-001")
video_url = video.to_presigned_url(timedelta(minutes=30))

client = OpenAI(
    api_key=os.environ["DASHSCOPE_API_KEY"],
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)
response = client.chat.completions.create(
    model="qwen3.5-plus",
    messages=[{
        "role": "user",
        "content": [
            {"type": "video_url", "video_url": {"url": video_url}},
            {"type": "text", "text": "Describe this video in one short sentence."},
        ],
    }],
)
print(response.choices[0].message.content)
```

`video` is a `BlobObject`. Presigning materializes its descriptor byte range in
OSS without downloading the video to Python. The URL preserves the OSS endpoint;
Internal URLs require network access from both the application and the consumer.

## URL validity

OSS V4 limits validity to 7 days for AccessKeys and 12 hours for STS credentials.
DLF credentials are refreshed if their remaining lifetime is too short; signing
fails if the refreshed credentials still cannot cover the requested validity.
Refreshing credentials does not extend an existing URL. Keep signed URLs private.

See [Blob Store](./blob-store) for the object API and
[BLOB References](../multimodal-table/blob-references#presigned-urls-for-oss-blobs)
for presigning details.
