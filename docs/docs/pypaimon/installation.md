---
title: "Installation"
description: "Install PyPaimon, select optional dependencies, or build the Python package from this source checkout."
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

# Installation

Install PyPaimon in the Python environment used by your application. The base
package supports Python 3.6 and newer; optional integrations have higher minimum
versions. For the examples in this guide, use Python 3.10 or newer, or Python 3.11
or newer if you need Vortex.

## Install the package

Create and activate a virtual environment. On macOS or Linux:

```shell
python3 -m venv .venv
source .venv/bin/activate
python -m pip install pypaimon
```

On Windows, activate the environment with `.venv\Scripts\activate` instead.

The package includes the core catalog, table, Arrow, and pandas APIs, as well as
the `paimon` command. Core table reads and writes do not require a JVM or a
running Flink or Spark cluster.

## Optional dependencies

Install only the extras needed by your workload. Quote package names containing
brackets so shells such as zsh do not interpret them as filename patterns.
The minimum versions below reflect this checkout's package requirements;
dependency availability can also depend on the operating system.

| Workload | Install | Python requirement |
| --- | --- | --- |
| Distributed Ray processing | `python -m pip install 'pypaimon[ray]'` | 3.8+ |
| Daft DataFrames | `python -m pip install 'pypaimon[daft]'` | 3.10+ |
| PyTorch datasets | `python -m pip install 'pypaimon[torch]'` | Must also satisfy the selected PyTorch version |
| SQL queries | `python -m pip install 'pypaimon[sql]'` | 3.10+ |
| Conditional merge expressions | `python -m pip install 'pypaimon[datafusion]'` | 3.10+ |
| HDF5 import | `python -m pip install 'pypaimon[hdf5]'` | 3.8+ |
| ROSBag import | `python -m pip install 'pypaimon[rosbag]'` | 3.10+ |
| LeRobot datasets | `python -m pip install 'pypaimon[lerobot]'` | 3.10+ |
| ACT benchmark | `python -m pip install 'pypaimon[act]'` | 3.10+ |
| Mosaic files | `python -m pip install 'pypaimon[mosaic]'` | 3.9+ |
| Lance files | `python -m pip install 'pypaimon[lance]'` | 3.8+ |
| Vortex files | `python -m pip install 'pypaimon[vortex]'` | 3.11+ |
| IVF-PQ vector indexes | `python -m pip install 'pypaimon[vindex]'` | 3.9+ |
| Full-text indexes | `python -m pip install 'pypaimon[full-text]'` | 3.8+ |
| Native HDFS access | `python -m pip install 'pypaimon[hdfs]'` | 3.10+, non-Windows |

Extras can be combined, for example:

```shell
python -m pip install 'pypaimon[daft,ray]'
```

For OSS access options, see [Catalogs and Tables](./catalogs) and
[PyJindoSDK](./pyjindosdk-support). Parquet and the multimodal API's default file
formats do not require a file-format extra.

## Install from source

These docs describe the current source tree. A published package may not yet
include every feature described here. From the repository root, install the
Python module with:

```shell
python -m pip install ./paimon-python
```

For local Python development, use an editable installation:

```shell
python -m pip install -e './paimon-python'
```

To produce a source archive:

```shell
cd paimon-python
python setup.py sdist
```

Archives are written to `paimon-python/dist/`. Development builds can produce
both a base development archive and a dated archive; select one archive when
installing instead of passing `dist/*.tar.gz` to pip.

## Verify the installation

```shell
python -c "from pypaimon import CatalogFactory, Schema; print('PyPaimon is ready')"
paimon --help
```

Continue with the [Quick Start](./quick-start). Install compute or media extras
in the worker environment as well when running distributed jobs.
