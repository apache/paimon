---
title: "Incremental Clustering"
weight: 4
type: docs
aliases:
- /append-table/incremental-clustering.html
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

# Incremental Clustering

Paimon currently supports ordering append tables using SFC (Space-Filling Curve)(see [sort compact]({{< ref "maintenance/dedicated-compaction#sort-compact" >}}) for more info). 
The resulting data layout typically delivers better performance for queries that target clustering keys. 
However, with the current SortCompaction, even when neither the data nor the clustering keys have changed, 
each run still rewrites the entire dataset, which is extremely costly. 

To address this, Paimon introduced a more flexible, incremental clustering mechanism—Incremental Clustering. 
On each run, it selects only a specific subset of files to cluster, avoiding a full rewrite. This enables low-cost, 
sort-based optimization of the data layout and improves query performance. In addition, with Incremental Clustering, 
you can adjust clustering keys without rewriting existing data, the layout evolves dynamically as cluster runs and 
gradually converges to an optimal state, significantly reducing the decision-making complexity around data layout.


Incremental Clustering supports:
- Support incremental clustering; minimizing write amplification as possible.
- Support small-file compaction; during rewrites, respect target-file-size.
- Support changing clustering keys; newly ingested data is clustered according to the latest clustering keys.
- Provide a full mode; when selected, the entire dataset will be reclustered.

**Only append unaware-bucket table supports Incremental Clustering.**

## Enable Incremental Clustering

To enable Incremental Clustering, the following configuration needs to be set for the table:
<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 10%">Value</th>
      <th class="text-left" style="width: 5%">Required</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 55%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>clustering.incremental</h5></td>
      <td>true</td>
      <td style="word-wrap: break-word;">Yes</td>
      <td>Boolean</td>
      <td>Must be set to true to enable incremental clustering. Default is false.</td>
    </tr>
    <tr>
      <td><h5>clustering.columns</h5></td>
      <td>'clustering-columns'</td>
      <td style="word-wrap: break-word;">Yes</td>
      <td>String</td>
      <td>The clustering columns, in the format 'columnName1,columnName2'. It is not recommended to use partition keys as clustering keys.</td>
    </tr>
    <tr>
      <td><h5>clustering.strategy</h5></td>
      <td>'zorder' or 'hilbert' or 'order'</td>
      <td style="word-wrap: break-word;">No</td>
      <td>String</td>
      <td>The ordering algorithm used for clustering. If not set, It'll decided from the number of clustering columns. 'order' is used for 1 column, 'zorder' for less than 5 columns, and 'hilbert' for 5 or more columns.</td>
    </tr>
    </tbody>

</table>

Once Incremental Clustering for a table is enabled, you can run Incremental Clustering in batch mode periodically 
to continuously optimizes data layout of the table and deliver better query performance.

**Note**: Since common compaction also rewrites files, it may disrupt the ordered data layout built by Incremental Clustering. 
Therefore, when Incremental Clustering is enabled, the table no longer supports write-time compaction or dedicated compaction; 
clustering and small-file merging must be performed exclusively via Incremental Clustering runs.

## Run Incremental Clustering
{{< hint info >}}

only support running Incremental Clustering in batch mode.

{{< /hint >}}

To run a Incremental Clustering job, follow these instructions. 

You don’t need to specify any clustering-related parameters when running Incremental Clustering,
these options are already defined as table options. If you need to change clustering settings, please update the corresponding table options.

{{< tabs "incremental-clustering" >}}

{{< tab "Spark SQL" >}}

Run the following sql:

```sql
--set the write parallelism, if too big, may generate a large number of small files.
SET spark.sql.shuffle.partitions=10;

-- run incremental clustering
CALL sys.compact(table => 'T')

-- run incremental clustering with full mode, this will recluster all data
CALL sys.compact(table => 'T', compact_strategy => 'full')
```
{{< /tab >}}

{{< tab "Flink Action" >}}

Run the following command to submit a incremental clustering job for the table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--compact_strategy <minor / full>] \
    [--table_conf <table_conf>] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
```

Example: run incremental clustering

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    compact \
    --warehouse s3:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --table_conf sink.parallelism=2 \
    --compact_strategy minor \
    --catalog_conf s3.endpoint=https://****.com \
    --catalog_conf s3.access-key=***** \
    --catalog_conf s3.secret-key=*****
```
* `--compact_strategy` Determines how to pick files to be cluster, the default is `minor`.
    * `full` : All files will be selected for clustered.
    * `minor` : Pick the set of files that need to be clustered based on specified conditions.

Note: write parallelism is set by `sink.parallelism`, if too big, may generate a large number of small files.

You can use `-D execution.runtime-mode=batch` or `-yD execution.runtime-mode=batch` (for the ON-YARN scenario) to use batch mode.
{{< /tab >}}

{{< /tabs >}}

## Auto-Clustering For Historical Partition
While performing incremental clustering on recently active partitions, Paimon can automatically detect historical and 
inactive partitions and evaluate whether their data layout has reached an optimal state. 
For those historical partitions that have not yet achieved optimal layout, Paimon will also perform full clustering on them 
during the same operation, thereby improving their query performance.

To enable auto-clustering for historical partitions, the following configuration needs to be set for the table:
<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 10%">Value</th>
      <th class="text-left" style="width: 5%">Required</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 55%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>clustering.history-partition.idle-to-full-sort</h5></td>
      <td>3d</td>
      <td style="word-wrap: break-word;">Yes</td>
      <td>Duration</td>
      <td>The duration after which a partition without new updates is considered a historical partition. Default is null.</td>
    </tr>
    <tr>
      <td><h5>clustering.history-partition.limit</h5></td>
      <td>5</td>
      <td style="word-wrap: break-word;">Yes</td>
      <td>Integer</td>
      <td>The limit of history partition number for automatically performing full clustering. Default value is 5.</td>
    </tr>
    </tbody>

</table>


## Implement
To balance write amplification and sorting effectiveness, Paimon leverages the LSM Tree notion of levels to stratify data files 
and uses the Universal Compaction strategy to select files for clustering.
- Newly written data lands in level-0; files in level-0 are unclustered.
- All files in level-i are produced by sorting within the same sorting set.
- By analogy with Universal Compaction: in level-0, each file is a sorted run; in level-i, all files together constitute a single sorted run. During clustering, the sorted run is the basic unit of work.

By introducing more levels, we can control the amount of data processed in each clustering run. 
Data at higher levels is more stably clustered and less likely to be rewritten, thereby mitigating write amplification while maintaining good sorting effectiveness.
