---
title: "RoboMIND ACT Storage Benchmark"
sidebar_position: 8
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

# RoboMIND ACT Storage Benchmark

This benchmark measures the same CPU LeRobot ACT training workload over an
original RoboMIND AgileX HDF5 dataset or an already ingested and
canonical-action-backfilled Paimon warehouse. Ingestion and backfill are outside
the timed scope.

The backends run independently. A resolved experiment document preserves the
shared configuration, normalization, seed, episode selection, Paimon snapshot,
and logical window sequence. Result comparison verifies that contract before it
calculates performance ratios.

## Install

```shell
pip install 'pypaimon[act,hdf5]'
```

## 1. Prepare the experiment

```shell
python -m pypaimon.benchmark.act prepare \
  --input /data/RoboMIND/h5_agilex_3rgb \
  --warehouse /data/warehouse \
  --output /data/results/experiment.json
```

Preparation is not timed. It verifies that HDF5 discovery matches the Paimon
episodes table, checks versioned action statistics against train-only HDF5
moments, selects eligible train and validation episodes, pins the frames
snapshot, and materializes deterministic measurement, training, and validation
window indices.

Without `--experiment`, preparation starts from the packaged
`default_experiment.json`. A custom JSON file can change the defaults, and
individual values can be overridden on the command line:

```shell
python -m pypaimon.benchmark.act prepare \
  --experiment my-experiment.json \
  --input /data/RoboMIND/h5_agilex_3rgb \
  --warehouse /data/warehouse \
  --action-horizon 32 \
  --batch-size 2 \
  --fetch-batches 8 \
  --rounds 3 \
  --output /data/results/experiment.json
```

The resolved experiment embeds the effective parameters as well as:

- portable source episode metadata and its SHA-256;
- normalization values, scope, version, frame count, and SHA-256;
- selected train and validation episode IDs;
- every logical window index, the window-plan SHA-256, and the
  episode-qualified sample-sequence SHA-256;
- the Paimon database, frames table, and pinned snapshot ID.

## 2. Run each backend

```shell
python -m pypaimon.benchmark.act run \
  --backend hdf5 \
  --experiment /data/results/experiment.json \
  --input /data/RoboMIND/h5_agilex_3rgb \
  --results-dir /data/results

python -m pypaimon.benchmark.act run \
  --backend paimon \
  --experiment /data/results/experiment.json \
  --warehouse /data/warehouse \
  --results-dir /data/results
```

Use `--output` to choose an exact result path. Otherwise the command writes an
automatically named JSON file below `--results-dir` and prints its absolute
path as a compact JSON object.

Each result contains the complete resolved experiment and experiment SHA-256,
backend identity, runtime environment, model metadata, planned-sample tensor
fingerprint, three or more raw measurement rounds, and median/minimum/maximum
summary metrics.

Both adapters produce the same shared sample contract. State and camera images
come from the anchor frame; action covers the complete horizon. HDF5 reads a
window on demand from one episode file. Paimon uses a lazy, snapshot-pinned
`ContiguousWindowDataset`; image columns are anchor-only, and plural
`__getitems__` access coalesces multiple logical batches into a physical
fetch before splitting them back into the unchanged model batch size.

## 3. Compare results

Compare explicit files:

```shell
python -m pypaimon.benchmark.act compare \
  /data/results/robomind-act-hdf5-20260901T010000Z-a1b2c3d4.json \
  /data/results/robomind-act-paimon-20260901T011000Z-e5f6a7b8.json \
  --output /data/results/comparison.json
```

Or discover all ACT result documents in a directory:

```shell
python -m pypaimon.benchmark.act compare \
  --results-dir /data/results \
  --output /data/results/comparison.json
```

Directory discovery ignores experiment and prior comparison JSON files.
Results are grouped by experiment SHA-256. Different experiments remain
separate entries in one comparison artifact; only compatible repeated results
for the same experiment and backend are aggregated.

Within one experiment group, comparison requires identical runtime environment,
model metadata, tensor fingerprint, train-loss trace, and validation-loss trace.
An environment mismatch marks the group `INCOMPATIBLE`; a model, tensor, or
loss mismatch marks it `FAILED`. Neither case produces performance ratios.

For compatible HDF5 and Paimon results, higher-is-better metrics report
`paimon_over_hdf5`. Lower-is-better latency, time, and memory metrics report
`hdf5_over_paimon`, which is the Paimon speedup or reduction factor.

## Measurements

Every backend repeat records:

- dataset construction time;
- first-batch latency after construction;
- batch-fetch samples per second after warm-up;
- fixed ACT optimizer-step time and per-step loss trace;
- validation loss;
- total measured wall time;
- Python peak allocation from a separate dataset-first-batch replay.

The shared harness resets Python, NumPy, and Torch random generators before
model construction and enables deterministic Torch algorithms. The logical
window plan is explicit rather than delegated to a streaming reader.

Python peak allocation uses `tracemalloc` after wall-clock measurement so
tracing overhead does not distort throughput. It does not include every native
Arrow or Torch allocation. The benchmark does not drop the OS page cache.
GPU, multi-worker loading, distributed training, recovery, and policy quality
remain outside this benchmark.

## Code organization

- `benchmark.act.harness`: shared ACT tensors, model, trainer, window plan, and
  measurement lifecycle;
- `benchmark.act.hdf5`: HDF5 window dataset and train normalization moments;
- `benchmark.act.paimon`: Paimon adapter, snapshot-pinned datasets, and
  versioned statistics access;
- `benchmark.act.runner`: experiment preparation and one-backend execution;
- `benchmark.act.compare`: result discovery, compatibility checks, grouping by
  experiment, and aggregation of compatible repeated runs;
- `benchmark.act.__main__`: the `prepare`, `run`, and `compare`
  command-line interface.
