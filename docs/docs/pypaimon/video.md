---
title: "Video Frames"
description: "Store complete encoded videos and read their logical frame rows for training."
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

# Video Frames

Store complete encoded videos and read their logical frame rows for training. First create a multimodal `conn` as shown in [Multimodal Tables](./multimodal-tables#connect). Provide your own video files and frame metadata for the examples below.

## Video Frame Storage

For a frame table, set `video-frame-field` to one or more comma-separated
scalar BLOB columns. Paimon stores frame ordinals in `VideoFrameDescriptor`
and compact `.video` indexes, so the normal data file does not need a
`frame_index` or `frame_timestamp` column:

```python
import pyarrow as pa
import pypaimon.multimodal as pm

frames = conn.create_table(
    "video_frames",
    schema=pa.schema([
        pa.field("episode_id", pa.int64()),
        pa.field("state", pa.list_(pa.float32())),
        pa.field("action", pa.list_(pa.float32())),
        pa.field("video", pa.large_binary()),
    ]),
    options={
        "video-frame-field": "video",
    },
)
```

`add_video` accepts a complete encoded-video source plus the ordinary columns
for its logical frame rows. `Blob.from_local` is descriptor-backed and streams
the source; the MP4 is not first loaded into Python memory:

```python
video = pm.Blob.from_local("/data/episode-42.mp4")
frames.add_video(video, frame_rows, first_frame=0)
```

Use `add_videos` to keep one writer and one commit open across several source
videos. This is the path that lets one `.video` object pack multiple complete
MP4 payloads and reduces object/manifest count:

```python
frames.add_videos([
    (pm.Blob.from_local("/data/episode-42.mp4"), episode_42_rows),
    (pm.Blob.from_local("/data/episode-43.mp4"), episode_43_rows),
])
```

Each item may also be `(video, frame_rows, first_frame)`. Frame ordinals are
generated consecutively from `first_frame`. If application semantics require
PTS, wall-clock time, or a non-unit sampling map, retain that value in a normal
column; `.video` version 1 addresses presentation-order frames with stride one.

Remote sources can use an explicit descriptor. Its URI must be readable with
the table's configured `FileIO` credentials:

```python
video = pm.BlobDescriptor(
    "oss://source-bucket/episode-43.mp4",
    offset=0,
    length=video_size,
)
frames.add_video(video, episode_43_rows)
```

The writer deduplicates exact payload descriptor identity inside each `.video`
file. Its video grouping policy coordinates normal, BLOB, and vector rolling
at payload boundaries. A file may exceed its target before the next boundary.
The normal `.blob` format is unchanged.

### Update frame rows and replace a video

Frame rows use the ordinary table update API for non-video columns. This writes
only the changed data-evolution columns and keeps the existing `.video` object
and frame descriptors unchanged:

```python
frames.update(
    where="episode_id = 42",
    values={"state": corrected_states},
)
```

Writing a complete encoded video uses the specialized API. `replace_video`
selects the matching logical rows in `_ROW_ID` order, assigns consecutive frame
ordinals, and writes a video-column delta without rewriting the normal data
file:

```python
frames.replace_video(
    where="episode_id = 42",
    video=pm.Blob.from_local("/data/episode-42-corrected.mp4"),
    first_frame=0,
)
```

The generic `update()` API rejects assignments to the configured
`video-frame-field`; use `replace_video()` so Paimon can preserve the complete
video payload and its embedded row-to-frame mapping.

### Read with PyTorch DataLoader

Install the PyTorch extra, then convert the multimodal scan directly. Unlike a
regular BLOB materialization, `ScanQuery.to_torch` always returns serialized
descriptors for BLOB columns. DataLoader workers receive Paimon splits and open
the selected video ranges themselves:

```shell
pip install 'pypaimon[torch]'
```

```python
from torch.utils.data import DataLoader

dataset = (
    frames.scan()
    .select(["episode_id", "state", "action", "video"])
    .to_torch(streaming=True)
)

loader = DataLoader(
    dataset,
    batch_size=32,
    num_workers=4,
    shuffle=False,
)
```

The `video` value is serialized `VideoFrameDescriptor` bytes, not repeated MP4
bytes. `VideoFrameCollator` owns a bounded, process-local LRU cache keyed by the
physical video range, so different frame descriptors reuse one decoder session
in each DataLoader worker. `decoder_factory(stream)` opens any codec library;
`decode_fn(decoder, frame_index, row)` receives the embedded frame ordinal.

```python
import av
import torch
import pypaimon.multimodal as pm
from torch.utils.data import DataLoader


class SequentialPyAvDecoder:
    def __init__(self, stream):
        self.container = av.open(stream)
        self._reset()

    def _reset(self):
        self.frames = iter(self.container.decode(video=0))
        self.next_index = 0

    def frame(self, index):
        if index < self.next_index:
            self.container.seek(0)
            self._reset()
        while self.next_index <= index:
            frame = next(self.frames)
            self.next_index += 1
        array = frame.to_ndarray(format="rgb24")
        return torch.from_numpy(array).permute(2, 0, 1)

    def close(self):
        self.container.close()


def decode_frame(decoder, frame_index, row):
    return decoder.frame(frame_index)


collator = pm.VideoFrameCollator(
    frames,
    video_column="video",
    decoder_factory=SequentialPyAvDecoder,
    decode_fn=decode_frame,
    output_column="frame",
    max_open_videos=4,
)

loader = DataLoader(
    dataset,
    batch_size=32,
    num_workers=4,
    shuffle=False,
    collate_fn=collator,
    persistent_workers=True,
)

for batch in loader:
    # batch["frame"] is [N, C, H, W].
    train(batch["frame"], batch["episode_id"])
```

The example decoder additionally requires `pip install av`; PyAV is not a
PyPaimon dependency.

Define the decoder class and function at module scope when DataLoader uses the
`spawn` multiprocessing method. For sequential video decoding, keep
`shuffle=False`; row-buffer shuffle can turn monotonic frame access into seeks
and require more open decoder sessions. Paimon's worker sharding is split-level,
so it does not duplicate a split across workers.
