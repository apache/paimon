---
title: "Data Lineage"
sidebar_position: 13
---

# Data Lineage

Paimon implements [FLIP-314](https://cwiki.apache.org/confluence/spaces/FLINK/pages/240884116/FLIP-294+Support+Customized+Catalog+Modification+Listener) to expose lineage information through Flink's native `LineageVertexProvider` interface. This allows lineage consumers (such as [OpenLineage](https://openlineage.io/docs/integrations/flink/flink2)) to automatically discover Paimon datasets in the Flink job graph.

## Table API / SQL

When using Paimon tables via Flink SQL or Table API, lineage is automatically reported for both source and sink tables.

Apart from the table lineage information provided by Flink natively (name, schema), Paimon enriches each dataset with a namespace and a **config facet** containing core table options and Iceberg metadata options.

**Note:** For Flink's planner to extract Paimon's lineage datasets from `DataStreamScanProvider` and `DataStreamSinkProvider`, [this upstream Flink change](https://github.com/apache/flink/pull/27727) is pending merge. Without it, Table API lineage events may have missing Paimon table datasets.

## DataStream API

Paimon also supports lineage for the DataStream API. Lineage is automatically reported for sources built via `FlinkSourceBuilder` and for all sinks that extend `FlinkSink`.

Similar to the Table API, it reports all lineage information including name, namespace, schema facet, and config facet.

## Configuration

To consume lineage events, a [JobStatusChangedListener](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/advanced/job_status_listener/#configuration) must be added in your Flink configuration.

For example, to configure with OpenLineage:

```
execution.job-status-changed-listeners = io.openlineage.flink.listener.OpenLineageJobStatusChangedListenerFactory
```
