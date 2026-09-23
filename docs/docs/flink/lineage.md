---
title: "Data Lineage"
sidebar_position: 13
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

# Data Lineage

Paimon implements [FLIP-314](https://cwiki.apache.org/confluence/spaces/FLINK/pages/255070913/FLIP-314+Support+Customized+Job+Lineage+Listener) to expose lineage information through Flink's native [LineageVertexProvider](https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/data_lineage/) interface. This allows lineage consumers (such as [OpenLineage](https://openlineage.io/docs/integrations/flink/flink2)) to automatically discover Paimon datasets in the Flink job graph.

:::info
Data Lineage support is available only for Flink 2.0 and above.
:::

## Table API / SQL

When using Paimon tables via Flink SQL or Table API, lineage is automatically reported for both source and sink tables.

Apart from the table lineage information provided by Flink natively (name, schema), Paimon enriches each dataset with a namespace and a **config facet** containing core table options and Iceberg metadata options.

:::warning
Full Table API lineage support requires [the upstream Flink change](https://github.com/apache/flink/pull/27727). Check whether your Flink distribution includes it. Without it, Flink will not call `getLineageVertex()` for `DataStreamScanProvider` and `DataStreamSinkProvider`, which can lead to potentially missing Paimon input or output datasets. This applies to all Flink 2.x versions that do not yet include this change.
:::

## DataStream API

Paimon also supports lineage for the DataStream API. Lineage is automatically reported for sources built via `FlinkSourceBuilder` and for all sinks that extend `FlinkSink`.

Similar to the Table API, it reports all lineage information including name, namespace, schema facet, and config facet.

## Configuration

To consume lineage events, a [JobStatusChangedListener](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/advanced/job_status_listener/#configuration) must be added in your Flink configuration.

For example, to configure with OpenLineage:

```
execution.job-status-changed-listeners = io.openlineage.flink.listener.OpenLineageJobStatusChangedListenerFactory
```
