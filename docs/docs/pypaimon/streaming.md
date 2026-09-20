---
title: "Streaming Reads and Consumers"
description: "Follow new table snapshots and manage saved consumer positions."
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

# Streaming Reads and Consumers

Follow new table snapshots and manage saved consumer positions. These examples assume an existing `table`; see [Catalogs and Tables](./catalogs) for setup. A streaming scan continues polling, so stop the loop when your application has finished.

## Streaming Read

Streaming reads allow you to continuously read new data as it arrives in a Paimon table. This is useful for building
real-time data pipelines and ETL jobs.

### Basic Streaming Read

Use `StreamReadBuilder` to create a streaming scan that continuously polls for new snapshots:

```python
table = catalog.get_table('database_name.table_name')

# Create streaming read builder
stream_builder = table.new_stream_read_builder()
stream_builder.with_poll_interval_ms(1000)  # Poll every 1 second

# Create streaming scan and table read
scan = stream_builder.new_streaming_scan()
table_read = stream_builder.new_read()

# Async streaming (recommended for ETL pipelines)
import asyncio

async def process_stream():
    async for plan in scan.stream():
        for split in plan.splits():
            arrow_batch = table_read.to_arrow([split])
            # Process the data
            print(f"Received {arrow_batch.num_rows} rows")

asyncio.run(process_stream())
```

### Synchronous Streaming

For simpler use cases, you can use the synchronous wrapper:

```python
# Synchronous streaming
for plan in scan.stream_sync():
    arrow_table = table_read.to_arrow(plan.splits())
    process(arrow_table)
```

### Manual Position Control

You can directly read and set the scan position via `next_snapshot_id`:

```python
# Save current position
saved_position = scan.next_snapshot_id

# Later, restore position
scan.next_snapshot_id = saved_position

# Or start from a specific snapshot
scan.next_snapshot_id = 42
```

### Filtering Streaming Data

You can apply predicates and projections to streaming reads:

```python
stream_builder = table.new_stream_read_builder()

# Build predicate
predicate_builder = stream_builder.new_predicate_builder()
predicate = predicate_builder.greater_than('timestamp', 1704067200000)

# Apply filter and projection
stream_builder.with_filter(predicate)
stream_builder.with_projection(['id', 'name', 'timestamp'])

scan = stream_builder.new_streaming_scan()
```

Key points about streaming reads:

- **Poll Interval**: Controls how often to check for new snapshots (default: 1000ms)
- **Initial Scan**: First iteration returns all existing data, subsequent iterations return only new data
- **Commit Types**: By default, only APPEND commits are processed; COMPACT and OVERWRITE are skipped

### Parallel Consumption

For high-throughput streaming, you can run multiple consumers in parallel, each reading a disjoint subset of buckets.
This is similar to Kafka consumer groups.

**Using `with_buckets()` for explicit bucket assignment**:

```python
# Consumer 0 reads buckets 0, 1, 2
stream_builder.with_buckets([0, 1, 2])

# Consumer 1 reads buckets 3, 4, 5
stream_builder.with_buckets([3, 4, 5])
```

**Using `with_bucket_filter()` for custom filtering**:

```python
# Read only even buckets
stream_builder.with_bucket_filter(lambda b: b % 2 == 0)
```

### Row Kind Support

For changelog streams, you can include the row kind to distinguish between inserts, updates, and deletes:

```python
stream_builder = table.new_stream_read_builder()
stream_builder.with_include_row_kind(True)

scan = stream_builder.new_streaming_scan()
table_read = stream_builder.new_read()

async for plan in scan.stream():
    arrow_table = table_read.to_arrow(plan.splits())
    for row in arrow_table.to_pylist():
        row_kind = row['_row_kind']  # +I, -U, +U, or -D
        if row_kind == '+I':
            handle_insert(row)
        elif row_kind == '-D':
            handle_delete(row)
        elif row_kind in ('-U', '+U'):
            handle_update(row)
```

Row kind values:
- `+I`: Insert
- `-U`: Update before (old value)
- `+U`: Update after (new value)
- `-D`: Delete

## Consumer Management

Consumer management allows you to track consumption progress, prevent snapshot expiration, and resume from breakpoints.

### Create ConsumerManager

```python
from pypaimon import CatalogFactory

# Get table and file_io
catalog = CatalogFactory.create({'warehouse': 'file:///path/to/warehouse'})
table = catalog.get_table('database_name.table_name')
file_io = table.file_io

# Create consumer manager
manager = table.consumer_manager()
```

### Get Consumer

Retrieve a consumer by its ID:

```python
from pypaimon.consumer.consumer import Consumer

consumer = manager.consumer('consumer_id')
if consumer:
    print(f"Next snapshot: {consumer.next_snapshot}")
else:
    print("Consumer not found")
```

### Reset Consumer

Create or reset a consumer with a new snapshot ID:

```python
# Reset consumer to snapshot 10
manager.reset_consumer('consumer_id', Consumer(next_snapshot=10))
```

### Delete Consumer

Delete a consumer by its ID:

```python
manager.delete_consumer('consumer_id')
```

### List Consumers

Get all consumers with their next snapshot IDs:

```python
consumers = manager.consumers()
for consumer_id, next_snapshot in consumers.items():
    print(f"Consumer {consumer_id}: next snapshot {next_snapshot}")
```

### List All Consumer IDs

List all consumer IDs:

```python
consumer_ids = manager.list_all_ids()
for consumer_id in consumer_ids:
    print(consumer_id)
```

### Get Minimum Next Snapshot

Get the minimum next snapshot across all consumers:

```python
min_snapshot = manager.min_next_snapshot()
if min_snapshot:
    print(f"Minimum next snapshot: {min_snapshot}")
```

### Expire Consumers

Expire consumers modified before a given datetime:

```python
from datetime import datetime, timedelta

# Expire consumers older than 1 day
expire_time = datetime.now() - timedelta(days=1)
manager.expire(expire_time)
```

### Clear Consumers

Clear consumers matching regular expression patterns:

```python
# Clear all consumers starting with "test_"
manager.clear_consumers('test_.*')

# Clear all consumers except those starting with "prod_"
manager.clear_consumers(
    '.*',
    'prod_.*'
)
```

### Branch Support

ConsumerManager supports multiple branches:

```python
# Custom branch
branch_manager = manager.with_branch('feature_branch')

# Each branch maintains its own consumers
print(branch_manager.consumers())  # Consumers on feature branch
```
