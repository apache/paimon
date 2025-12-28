---
title: "Archive"
weight: 15
type: docs
aliases:
- /concepts/archive.html
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

# Archive

Paimon supports archiving partition files to different storage tiers in object stores, enabling cost optimization by moving infrequently accessed data to lower-cost storage tiers.

## Overview

The archive feature allows you to:

- **Archive partitions** to Archive or ColdArchive storage tiers
- **Restore archived partitions** when you need to access the data
- **Unarchive partitions** to move them back to standard storage

This feature is particularly useful for:
- Cost optimization by moving old or infrequently accessed data to cheaper storage
- Compliance requirements for long-term data retention
- Managing data lifecycle in data lakes

## Storage Tiers

Paimon supports three storage tiers:

- **Standard**: Normal storage with standard access times and costs
- **Archive**: Lower cost storage with longer access times (requires restore before access)
- **ColdArchive**: Lowest cost storage with longest access times (requires restore before access)

## Supported Object Stores

Currently supported object stores:
- **Amazon S3**: Supports Archive (Glacier) and ColdArchive (Deep Archive) storage classes
- **Alibaba Cloud OSS**: Supports Archive and ColdArchive storage classes

## How It Works

When you archive a partition:

1. Paimon identifies all files in the partition (data files, manifest files, extra files)
2. Files are archived to the specified storage tier using the object store's API
3. Metadata paths remain unchanged - FileIO handles storage tier mapping transparently
4. Files can be accessed normally, but may require restore first depending on storage tier

## SQL Syntax

### Archive Partition

```sql
ALTER TABLE table_name PARTITION (partition_spec) ARCHIVE;
```

### Cold Archive Partition

```sql
ALTER TABLE table_name PARTITION (partition_spec) COLD ARCHIVE;
```

### Restore Archived Partition

```sql
ALTER TABLE table_name PARTITION (partition_spec) RESTORE ARCHIVE;
```

With duration:

```sql
ALTER TABLE table_name PARTITION (partition_spec) RESTORE ARCHIVE WITH DURATION 7 DAYS;
```

### Unarchive Partition

```sql
ALTER TABLE table_name PARTITION (partition_spec) UNARCHIVE;
```

## Examples

### Basic Archive Workflow

```sql
-- Create a partitioned table
CREATE TABLE sales (id INT, amount DOUBLE, dt STRING) 
PARTITIONED BY (dt) USING paimon;

-- Insert data
INSERT INTO sales VALUES (1, 100.0, '2024-01-01');
INSERT INTO sales VALUES (2, 200.0, '2024-01-02');

-- Archive old partition
ALTER TABLE sales PARTITION (dt='2024-01-01') ARCHIVE;

-- Restore when needed
ALTER TABLE sales PARTITION (dt='2024-01-01') RESTORE ARCHIVE;

-- Query data (may require restore first)
SELECT * FROM sales WHERE dt='2024-01-01';

-- Move back to standard storage
ALTER TABLE sales PARTITION (dt='2024-01-01') UNARCHIVE;
```

### Cold Archive for Long-term Retention

```sql
-- Archive to lowest cost tier
ALTER TABLE sales PARTITION (dt='2023-01-01') COLD ARCHIVE;

-- Restore when needed (may take longer for cold archive)
ALTER TABLE sales PARTITION (dt='2023-01-01') RESTORE ARCHIVE WITH DURATION 30 DAYS;
```

## Implementation Details

### File Discovery

When archiving a partition, Paimon:
1. Uses `PartitionFileLister` to discover all files in the partition
2. Includes data files, manifest files, and extra files (like indexes)
3. Processes files in parallel using Spark's distributed execution

### Metadata Handling

- **Original paths are preserved** in metadata
- FileIO implementations handle storage tier changes transparently
- No metadata rewriting is required for in-place archiving

### Distributed Execution

Archive operations use Spark's distributed execution:
- Files are processed in parallel across Spark executors
- Scalable to large partitions with many files
- Progress is tracked and failures are handled gracefully

## Limitations

1. **Object Store Only**: Archive operations are only supported for object stores (S3, OSS). Local file systems are not supported.

2. **Storage Tier Support**: Not all object stores support all storage tiers. Check your object store documentation for supported tiers.

3. **Restore Time**: Accessing archived data may require restore operations, which can take time depending on the storage tier:
   - Archive: Typically minutes to hours
   - ColdArchive: Typically hours to days

4. **Cost Considerations**: 
   - Archive tiers have lower storage costs but may have restore costs
   - Frequent restore operations can negate cost savings
   - Plan your archive strategy based on access patterns

## Best Practices

1. **Archive Old Data**: Archive partitions that are rarely accessed but need to be retained
2. **Use Cold Archive for Compliance**: Use ColdArchive for data that must be retained but is almost never accessed
3. **Plan Restore Operations**: Batch restore operations when possible to minimize costs
4. **Monitor Costs**: Track storage and restore costs to optimize your archive strategy
5. **Test Restore Process**: Ensure your restore process works correctly before archiving critical data

## Future Enhancements

Future enhancements may include:
- Support for more object stores (Azure Blob, GCS, etc.)
- Automatic archive policies based on partition age
- Archive status tracking in table metadata
- Flink SQL support for archive operations
- Batch archive operations for multiple partitions

