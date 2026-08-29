---
title: "RoboMIND Paired ACT Benchmark"
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

# RoboMIND Paired ACT Benchmark

The paired benchmark compares original RoboMIND AgileX HDF5 with an already
ingested and canonical-action-backfilled Paimon warehouse. It does not include
ingestion or backfill time. Install the ACT and HDF5 extras, run the
[RoboMIND AgileX pipeline](robomind-agilex), and then execute:

```shell
pip install 'pypaimon[act,hdf5]'
python -m pypaimon.benchmark.paired_act \
  --input /data/RoboMIND/h5_agilex_3rgb \
  --warehouse /data/warehouse \
  --report /data/results/paired-act.json
```

A successful run prints a compact `SUCCEEDED` result and writes the full JSON
report. A source, parity, or configuration mismatch raises an error and does
not write a successful report.

One immutable configuration controls both paths. The runner computes train-only
normalization once, verifies its canonical action values against the requested
version in `feature_stats_agilex`, and passes the same object to both adapters.
A seeded window plan fixes every warmup, loader, training, and validation
anchor. Before training, the runner compares `sample_id`, `episode_id`, and
`step_idx` by value and requires exact `torch.equal` parity for state, action,
image, and padding tensors.

The Paimon adapter uses `ContiguousWindowDataset`, not an ACT-specific table
reader. Dataset construction indexes only episode, frame, and row IDs. Window
payloads remain lazy until `__getitem__`, and all train and validation reads are
pinned to the exact frames snapshot recorded by the normalization statistics.
PyTorch batch access coalesces overlapping row IDs into one payload read. The
image columns are marked as anchor-only, so each sample loads the observation
images once rather than once per action-horizon row. The adapter maps each
generic window to the same tensor contract as the HDF5 adapter without
materializing episodes in memory.

Each backend then uses the same CPU LeRobot ACT policy, initial seed, AdamW
optimizer, batch size, window sequence, and optimizer step count. At least
three rounds run in alternating order (`HDF5 → Paimon`, then
`Paimon → HDF5`) to expose ordering effects. The benchmark does not drop the OS
page cache and records `cache_control=uncontrolled`.

The JSON report contains:

- input manifest, table snapshot, normalization, configuration, and window
  sequence digests;
- exact tensor, train-loss, and validation-loss parity gates;
- first-batch latency, DataLoader samples per second, fixed-step time, and a
  separate dataset-build-plus-first-batch Python allocation replay for every
  run;
- per-backend median, minimum, and maximum across rounds;
- explicit unverified scope, including native-memory completeness, GPU,
  multi-worker loading, distributed training, recovery, and policy quality.

Python peak allocation uses `tracemalloc` after wall-clock measurement so its
overhead does not distort throughput. The replay covers dataset construction
and one first batch; it does not include every native Arrow or Torch allocation.
Treat it as a reproducible engineering diagnostic, not total process RSS.
