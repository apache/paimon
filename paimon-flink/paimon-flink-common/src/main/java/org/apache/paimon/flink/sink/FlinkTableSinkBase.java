/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.flink.PaimonDataStreamSinkProvider;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.types.RowKind;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.CLUSTERING_COLUMNS;
import static org.apache.paimon.CoreOptions.CLUSTERING_INCREMENTAL;
import static org.apache.paimon.CoreOptions.CLUSTERING_STRATEGY;
import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTERING_SAMPLE_FACTOR;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTERING_SORT_IN_CLUSTER;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_PARALLELISM;

/** Table sink to create sink. */
public abstract class FlinkTableSinkBase
        implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning {

    protected final ObjectIdentifier tableIdentifier;
    protected final DynamicTableFactory.Context context;

    protected final Table table;

    protected Map<String, String> staticPartitions = new HashMap<>();
    protected boolean overwrite = false;

    public FlinkTableSinkBase(
            ObjectIdentifier tableIdentifier, Table table, DynamicTableFactory.Context context) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.context = context;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (table.primaryKeys().isEmpty()) {
            // Don't check this, for example, only inserts are available from the database, but the
            // plan phase contains all changelogs
            return requestedMode;
        } else {
            Options options = Options.fromMap(table.options());
            if (options.get(CHANGELOG_PRODUCER) == ChangelogProducer.INPUT) {
                return requestedMode;
            }

            if (options.get(MERGE_ENGINE) == MergeEngine.AGGREGATE) {
                return requestedMode;
            }

            if (options.get(MERGE_ENGINE) == MergeEngine.PARTIAL_UPDATE
                    && new CoreOptions(options).definedAggFunc()) {
                return requestedMode;
            }

            // with primary key, default sink upsert
            ChangelogMode.Builder builder = ChangelogMode.newBuilder();
            for (RowKind kind : requestedMode.getContainedKinds()) {
                if (kind != RowKind.UPDATE_BEFORE) {
                    builder.addContainedKind(kind);
                }
            }
            return builder.build();
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        if (overwrite && !context.isBounded()) {
            throw new UnsupportedOperationException(
                    "Paimon doesn't support streaming INSERT OVERWRITE.");
        }
        if (table instanceof FormatTable) {
            FormatTable formatTable = (FormatTable) table;
            return new PaimonDataStreamSinkProvider(
                    (dataStream) ->
                            new FlinkFormatTableDataStreamSink(
                                            formatTable, overwrite, staticPartitions)
                                    .sinkFrom(dataStream));
        }

        Options conf = Options.fromMap(table.options());
        // Do not sink to log store when overwrite mode
        return new PaimonDataStreamSinkProvider(
                (dataStream) -> {
                    FlinkSinkBuilder builder = createSinkBuilder();
                    builder.forRowData(
                            new DataStream<>(
                                    dataStream.getExecutionEnvironment(),
                                    dataStream.getTransformation()));
                    if (!conf.get(CLUSTERING_INCREMENTAL)) {
                        builder.clusteringIfPossible(
                                conf.get(CLUSTERING_COLUMNS),
                                conf.get(CLUSTERING_STRATEGY),
                                conf.get(CLUSTERING_SORT_IN_CLUSTER),
                                conf.get(CLUSTERING_SAMPLE_FACTOR));
                    }
                    if (overwrite) {
                        builder.overwrite(staticPartitions);
                    }
                    conf.getOptional(SINK_PARALLELISM).ifPresent(builder::parallelism);
                    return builder.build();
                });
    }

    protected FlinkSinkBuilder createSinkBuilder() {
        return new FlinkSinkBuilder(table);
    }

    @Override
    public DynamicTableSink copy() {
        FlinkTableSink copied = new FlinkTableSink(tableIdentifier, table, context);
        copied.staticPartitions = new HashMap<>(staticPartitions);
        copied.overwrite = overwrite;
        return copied;
    }

    @Override
    public String asSummaryString() {
        return "PaimonSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        table.partitionKeys()
                .forEach(
                        partitionKey -> {
                            if (partition.containsKey(partitionKey)) {
                                this.staticPartitions.put(
                                        partitionKey, partition.get(partitionKey));
                            }
                        });
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }
}
