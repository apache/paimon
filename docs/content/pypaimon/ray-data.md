---
title: "Ray Data"
weight: 3
type: docs
aliases:
  - /pypaimon/ray-data.html
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

# Ray Data

## Read

This requires `ray` to be installed.

You can convert the splits into a Ray Dataset and handle it by Ray Data API for distributed processing:

```python
table_read = read_builder.new_read()
ray_dataset = table_read.to_ray(splits)

print(ray_dataset)
# MaterializedDataset(num_blocks=1, num_rows=9, schema={f0: int32, f1: string})

print(ray_dataset.take(3))
# [{'f0': 1, 'f1': 'a'}, {'f0': 2, 'f1': 'b'}, {'f0': 3, 'f1': 'c'}]

print(ray_dataset.to_pandas())
#    f0 f1
# 0   1  a
# 1   2  b
# 2   3  c
# 3   4  d
# ...
```

The `to_ray()` method supports Ray Data API parameters for distributed processing:

```python
# Basic usage
ray_dataset = table_read.to_ray(splits)

# Specify number of output blocks
ray_dataset = table_read.to_ray(splits, override_num_blocks=4)

# Configure Ray remote arguments
ray_dataset = table_read.to_ray(
    splits,
    override_num_blocks=4,
    ray_remote_args={"num_cpus": 2, "max_retries": 3}
)

# Use Ray Data operations
mapped_dataset = ray_dataset.map(lambda row: {'value': row['value'] * 2})
filtered_dataset = ray_dataset.filter(lambda row: row['score'] > 80)
df = ray_dataset.to_pandas()
```

**Parameters:**
- `override_num_blocks`: Optional override for the number of output blocks. By default,
  Ray automatically determines the optimal number.
- `ray_remote_args`: Optional kwargs passed to `ray.remote()` in read tasks
  (e.g., `{"num_cpus": 2, "max_retries": 3}`).
- `concurrency`: Optional max number of Ray tasks to run concurrently. By default,
  dynamically decided based on available resources.
- `**read_args`: Additional kwargs passed to the datasource (e.g., `per_task_row_limit`
  in Ray 2.52.0+).

**Ray Block Size Configuration:**

If you need to configure Ray's block size (e.g., when Paimon splits exceed Ray's default
128MB block size), set it before calling `to_ray()`:

```python
from ray.data import DataContext

ctx = DataContext.get_current()
ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB (default is 128MB)
ray_dataset = table_read.to_ray(splits)
```

See [Ray Data API Documentation](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_datasource.html) for more details.

## Write

```python
table = catalog.get_table('database_name.table_name')

# 1. Create table write and commit
write_builder = table.new_batch_write_builder()
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()

# 2 Write Ray Dataset (requires ray to be installed)
import ray
ray_dataset = ray.data.read_json("/path/to/data.jsonl")
table_write.write_ray(ray_dataset, overwrite=False, concurrency=2)
# Parameters:
#   - dataset: Ray Dataset to write
#   - overwrite: Whether to overwrite existing data (default: False)
#   - concurrency: Optional max number of concurrent Ray tasks
#   - ray_remote_args: Optional kwargs passed to ray.remote() (e.g., {"num_cpus": 2})
# Note: write_ray() handles commit internally through Ray Datasink API.
#       Skip steps 3-4 if using write_ray() - just close the writer.

# 3. Commit data (required for write_pandas/write_arrow/write_arrow_batch only)
commit_messages = table_write.prepare_commit()
table_commit.commit(commit_messages)

# 4. Close resources
table_write.close()
table_commit.close()
```

By default, the data will be appended to table. If you want to overwrite table, you should use `TableWrite#overwrite`
API:

```python
# overwrite whole table
write_builder = table.new_batch_write_builder().overwrite()

# overwrite partition 'dt=2024-01-01'
write_builder = table.new_batch_write_builder().overwrite({'dt': '2024-01-01'})
```
