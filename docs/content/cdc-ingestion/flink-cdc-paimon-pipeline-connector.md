---
title: "Flink CDC Paimon Pipeline Connector"
weight: 10
type: docs
aliases:
  - /cdc-ingestion/flink-cdc-paimon-pipeline-connector.html
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

# Flink CDC Paimon Pipeline Connector

Flink CDC is a streaming data integration tool for the Flink engine. It allows users to describe their ETL pipeline 
logic via YAML elegantly and help users automatically generating customized Flink operators and submitting job.

The Paimon Pipeline connector can be used as both the Data Source or the Data Sink of the Flink CDC pipeline. This 
document describes how to set up the Paimon Pipeline connector as the Data Source. If you are interested in using 
Paimon as the Data Sink, please refer to Flink CDC's 
[Paimon Pipeline Connector](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/docs/connectors/pipeline-connectors/paimon/) 
document.

## What can the connector do?

* Synchronizes data from a Paimon warehouse, database or table to an external system supported by Flink CDC
* Synchronizes schema changes
* Automatically discovers newly created tables in the source Paimon warehouse.

How to create Pipeline
----------------

The pipeline for reading data from Paimon and sink to Doris can be defined as follows:

```yaml
source:
  type: paimon
  name: Paimon Source
  database: default
  table: test_table
  catalog.properties.metastore: filesystem
  catalog.properties.warehouse: /path/warehouse

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: pass

pipeline:
  name: Paimon to Doris Pipeline
  parallelism: 2
```

Pipeline Connector Options
----------------

{{< generated/cdc_configuration >}}

Catalog Options
----------------

Apart from the pipeline connector options described above, in the CDC yaml file you can also configure options that 
starts with `catalog.properties.`. For example, `catalog.properties.warehouse` or `catalog.properties.metastore`. Such 
options will have their prefix removed and the rest be regarded as catalog options. Please refer to the 
[Configurations]({{< ref "maintenance/configurations" >}}) section for catalog options available.

Usage Notes
--------

* Data updates for primary key tables (-U, +U) will be replaced with -D and +I.
* Does not support dropping tables. If you need to drop a table from the Paimon warehouse, please restart the Flink CDC job after performing the drop operation. When the job restarts, it will stop reading data from the dropped table, and the target table in the external system will remain unchanged from its state before the job was stopped.
* Data from the same table will be consumed by the same Flink source subtask. If the amount of data varies significantly across different tables, performance bottlenecks caused by data skew may be observed in Flink CDC jobs.
* If the CDC job has consumed up to the latest snapshot of a table and the next snapshot is not available yet, the monitoring and consumption of this table may be temporarily paused until `continuous.discovery-interval` has passed. 

Data Type Mapping
----------------
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Paimon type</th>
        <th class="text-left">CDC type</th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>INT</td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP_LTZ</td>
      <td>TIMESTAMP_LTZ</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    </tbody>
</table>
</div>

{{< top >}}