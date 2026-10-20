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

# CWD planning and batch-read benchmark

This CPU microbenchmark measures construction, first batch, and steady batch
reads of the same snapshot and sample sequence. It does not run a model, decode
images, compare HDF5, or establish GPU training performance. OS caches are
uncontrolled; a fresh process is not a cold-cache run.

The generated Parquet table is partitioned by task. Every episode is written in
a separate commit, with scalar values and deterministic BLOB bytes. Samples read
scalar offsets `[-2, 0, 1, ..., 15]` and only the anchor BLOB. Every returned ID,
scalar, BLOB and padding mask is checked against an independent generator.
Preparation and sample verification are outside the read timers.

```bash
python -m pypaimon.benchmark.cwd_read selfcheck
python -m pypaimon.benchmark.cwd_read prepare --warehouse /tmp/cwd-warehouse
python -m pypaimon.benchmark.cwd_read run --warehouse /tmp/cwd-warehouse --scope task
python -m pypaimon.benchmark.cwd_read run --warehouse /tmp/cwd-warehouse --scope all
```

Preparation refuses an existing directory. The manifest pins its snapshot and
data dimensions. The run seed shuffles the full eligible index space; requests
cycle that order if the requested count exceeds the dataset. The first batch is
reported separately and excluded from steady throughput. Construction includes
opening the catalog/table and building the CWD index. `build_and_read_s` is their
sum, excluding sample verification and process/import startup.

For a source comparison, invoke **this same script by its absolute path** with
`PYTHONPATH` set separately to each checkout's `paimon-python` directory. This
also works when the baseline does not contain this benchmark. Check the reported
`source` and `window_dataset_sha256`; record the corresponding commit and diff
alongside the results. Use one immutable prepared warehouse, the same CLI
arguments and interpreter, and fresh processes in alternating baseline/candidate
order. Perform one discarded warm-up pair and at least five measured pairs per
scope. Compare median and range and require identical `sample_digest` and
`manifest_sha256` before comparing throughput. Task-local and all-task results
must be reported separately.

Run `--diagnostic` separately to record every `TableScan.plan` call's phase,
split count and distinct data-file count. These are **planned** files, not opened
files or transferred bytes. Diagnostic timings are not performance results.

A 30% throughput increase means `candidate samples_per_s / baseline >= 1.30`;
it is not a 30% latency reduction. Report construction, first batch and steady
reads separately: reducing planning time alone does not establish higher steady
throughput. This small synthetic workload and its scalar/BLOB sizes are explicit
inputs, not a substitute for a real workload or a claim about object storage.
