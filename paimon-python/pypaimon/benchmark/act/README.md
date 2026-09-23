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

# RoboMIND AgileX: HDF5 ingestion and ACT training

This benchmark imports a downloaded RoboMIND AgileX directory, publishes a
versioned Paimon table group, and trains LeRobot ACT directly from the frame
wide table with `ContiguousWindowDataset`. Original JPEG payloads are retained;
this workflow does not transcode them to video. HDF5 remains an independent
reference backend for checking sample tensors, losses, and throughput.

Use Python 3.10 or newer and install the local package with its ACT dependencies:

```sh
pip install -e 'paimon-python[act,hdf5]'
```

The input root must contain both successful `train` and `val` episodes in the
original RoboMIND directory layout. In the published contract, `val` becomes
`eval`. Use a new warehouse for ingestion; existing tables are not overwritten.

```sh
python -m pypaimon.benchmark.act ingest \
  --input /path/to/robomind-agilex \
  --warehouse /path/to/act-warehouse \
  --statistics-version act-release-1

python -m pypaimon.benchmark.act prepare \
  --input /path/to/robomind-agilex \
  --warehouse /path/to/act-warehouse \
  --statistics-version act-release-1 \
  --output act-results/experiment.json

python -m pypaimon.benchmark.act run \
  --backend paimon --warehouse /path/to/act-warehouse \
  --experiment act-results/experiment.json \
  --output act-results/paimon.json
```

`ingest` writes exactly four tables: `info`, `episode`, `frame`, and `stat`.
The stat row holds raw feature moments and the nested `stats.act` normalization
scope. It materializes the
14-dimensional canonical action from the two master joint-position vectors,
then publishes all table members under `--statistics-version` as one Tag name.
The `info.tables` map identifies the group members. Joint positions include the
source gripper dimension; no new resampling or fabricated sensor values are
introduced.

`prepare` reads the published Tag, verifies the HDF5 source identity and
train-only normalization from `stat.stats.act`, and records a fixed frame
snapshot and window plan.
By default it selects one successful training episode and one successful eval
episode; `--train-episode-id` and `--validation-episode-id` override selection.
`--action-horizon`, `--batch-size`, and `--optimizer-steps` configure the run;
`--experiment` accepts a customized experiment definition. Run any subcommand
with `--help` for the complete options.

`run` groups frames by immutable `episode_index`, orders by `frame_index`, and
reads state and three cameras only at the window anchor. `action` spans the
configured horizon; incomplete terminal windows are dropped. The adapter
normalizes these values into ACT tensors and preserves source episode IDs for
comparison. It uses the prepared snapshot even if new data is subsequently
committed. No intermediate training sample table is needed.

Only valid, successful episodes are eligible for training and normalization.
The source manifest still retains excluded episodes. The comparison requires
all frames in the selected training/eval episodes to be valid: a dirty selected
episode is rejected instead of silently changing the HDF5/Paimon window
sequence. Opening a group also verifies that every registered member has the
published Tag, including the stat table.

To run the same experiment against the original HDF5 and compare results:

```sh
python -m pypaimon.benchmark.act run \
  --backend hdf5 --input /path/to/robomind-agilex \
  --experiment act-results/experiment.json \
  --output act-results/hdf5.json

python -m pypaimon.benchmark.act compare \
  act-results/hdf5.json act-results/paimon.json \
  --output act-results/comparison.json
```

Results include tensor fingerprints, fixed-step loss traces, fetch and training
timings, and runtime identity. The default small CPU experiment checks pipeline
correctness and engineering parity; it does not establish task success rates.
Warehouse data and result artifacts remain on disk for inspection.
