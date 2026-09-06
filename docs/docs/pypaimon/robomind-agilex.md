---
title: "RoboMIND AgileX"
sidebar_position: 7
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

# RoboMIND AgileX

The RoboMIND AgileX sample turns a downloaded HDF5 directory into three
Paimon tables:

- `episodes_agilex` stores episode metadata derived from the dataset layout;
- `frames_agilex` stores ordered robot state, raw action, RGB, and depth rows;
- `feature_stats_agilex` versions the train-split statistics consumed by
  policy training.

The local and Ray paths use the same `RoboMindAgileXEpisodeTransform` and
`RoboMindAgileXFrameTransform` contracts and table schemas. Ray assigns each
complete HDF5 file to one transform task, while the Paimon sink performs one
coordinated commit. Discovery does not open or hash HDF5 contents; validation
and frame counting happen after a transform task has opened the file.

`split` is RoboMIND dataset metadata derived from the `train` or `val`
directory component. It is not a required field of every Paimon multimodal
table. This sample uses successful train episodes to select frame rows for
normalization statistics.

## Run the local pipeline

After downloading RoboMIND, use Python 3.11 or later, install the HDF5 and
Vortex extras, and provide the source and warehouse directories to one command:

```bash
pip install 'pypaimon[hdf5,vortex]'
python -m pypaimon.sample.robomind_agilex \
  --input /data/RoboMIND/h5_agilex_3rgb \
  --warehouse /data/warehouse
```

The command discovers and validates every `**/data/trajectory.hdf5`, ingests
the episode and frame tables locally, materializes the canonical action, and
writes versioned train-split normalization statistics. It prints a JSON result
with row counts and committed snapshot IDs. The input must already be present
locally; the command does not download RoboMIND or contact Hugging Face.

Use a new warehouse for each ingestion run. Ingestion is append-only, so
repeating the same input against existing tables would create duplicate rows.
Canonical-action backfill is independently retryable after schema creation.

The current Hugging Face example data includes `language_raw` and
`language_distilbert`, but these datasets are not part of the published AgileX
HDF5 schema. The episode transform therefore validates and stores them when
present, and writes null instruction metadata when they are absent.

Pytest generates several small HDF5 episodes with the real AgileX field names,
shapes, dtypes, split layout, and success layout, so the default test needs no
download. To exercise a downloaded customer dataset explicitly, run:

```bash
pytest -q pypaimon/tests/robomind_agilex_pipeline_test.py \
  --robomind-agilex-input /data/RoboMIND/h5_agilex_3rgb
```

## Python API

```python
from pypaimon.sample.robomind_agilex import (
    backfill_canonical_action,
    backfill_canonical_action_ray,
    ingest_local,
    ingest_ray,
    run_local_pipeline,
    run_ray_pipeline,
)

# Run local ingestion and canonical-action backfill together.
pipeline = run_local_pipeline(
    "/data/RoboMIND/h5_agilex_3rgb",
    "/data/warehouse",
)

# Run distributed ingestion and backfill on one managed Ray cluster.
pipeline = run_ray_pipeline(
    "/data/RoboMIND/h5_agilex_3rgb",
    "/data/warehouse",
    concurrency=8,
    statistics_version="robomind-agilex-joint-position@1",
    num_partitions=8,
    ray_address="ray://cluster:10001",
)
```

Episode and frame ingestion commit separately and use the generic
`pypaimon.ray.load_from_hdf5` API in Ray mode. Canonical action materialization
and statistics refresh also commit separately. The Ray backfill uses the
optimized self-merge path: each target file group is processed by one task, so
updates originating from multiple input batches cannot produce competing delta
files for the same target file. The driver coordinates one commit after all
file groups finish. If statistics need to be regenerated, call
`refresh_action_statistics` without repeating ingestion or the row-id update.

Use `backfill_canonical_action` for the local iterable path and
`run_ray_pipeline` for managed distributed ingestion and backfill. The Ray
pipeline requires Ray 2.50 or newer. The lower-level `ingest_ray` and
`backfill_canonical_action_ray` stages assume that Ray has already been
initialized, which allows either stage to be retried independently.

The canonical `action` is `float32(concat(master/joint_position_left,
master/joint_position_right))`. The backfill materializes only this consumed
14-dimensional column. It does not materialize normalized actions. Instead,
the stats table stores the train-only population mean and standard deviation,
the `1e-2` standard-deviation floor, the train split manifest digest, and the
source `frames_agilex` snapshot. A training reader normalizes `action` at read
time with that versioned row.

The tables are non-primary-key append tables. Repeating ingestion therefore
appends duplicate rows by design; it does not mean row-level update/delete is
disabled. The sample keeps deletion vectors enabled, stores vectors with
Vortex, and sets `blob-as-descriptor=false` because its transforms emit raw
image/depth bytes rather than external BLOB descriptors. Parquet data format,
dynamic bucket mode, and global-index search mode are inherited defaults and
are not repeated in the sample options.

Run local and Ray modes against separate new warehouses when comparing them.
