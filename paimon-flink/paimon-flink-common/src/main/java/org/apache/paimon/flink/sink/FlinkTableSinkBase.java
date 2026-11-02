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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.PaimonDataStreamSinkProvider;
import org.apache.paimon.flink.utils.ChangelogModeUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.GeneratedColumnRow;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.CLUSTERING_COLUMNS;
import static org.apache.paimon.CoreOptions.CLUSTERING_INCREMENTAL;
import static org.apache.paimon.CoreOptions.CLUSTERING_INCREMENTAL_OPTIMIZE_WRITE;
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

    private static final Logger LOG = LoggerFactory.getLogger(FlinkTableSinkBase.class);

    protected Map<String, String> staticPartitions = new HashMap<>();
    protected boolean overwrite = false;
    protected boolean rowLevelUpdate = false;

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
            warnKeyOnlyDeletesIgnored("the table has no primary key");
            return requestedMode;
        } else {
            Options options = Options.fromMap(table.options());
            if (options.get(CHANGELOG_PRODUCER) == ChangelogProducer.INPUT) {
                warnKeyOnlyDeletesIgnored("'changelog-producer' is 'input'");
                return requestedMode;
            }

            if (options.get(MERGE_ENGINE) == MergeEngine.AGGREGATE) {
                warnKeyOnlyDeletesIgnored("'merge-engine' is 'aggregation'");
                return requestedMode;
            }

            if (options.get(MERGE_ENGINE) == MergeEngine.PARTIAL_UPDATE
                    && new CoreOptions(options).definedAggFunc()) {
                warnKeyOnlyDeletesIgnored(
                        "'merge-engine' is 'partial-update' with aggregation functions");
                return requestedMode;
            }

            // with primary key, default sink upsert
            ChangelogMode.Builder builder = ChangelogMode.newBuilder();
            for (RowKind kind : requestedMode.getContainedKinds()) {
                if (kind != RowKind.UPDATE_BEFORE) {
                    builder.addContainedKind(kind);
                }
            }
            if (options.get(FlinkConnectorOptions.SINK_KEY_ONLY_DELETES_ENABLED)) {
                ChangelogModeUtils.enableKeyOnlyDeletes(builder);
            }
            return builder.build();
        }
    }

    private void warnKeyOnlyDeletesIgnored(String reason) {
        if (Options.fromMap(table.options())
                .get(FlinkConnectorOptions.SINK_KEY_ONLY_DELETES_ENABLED)) {
            LOG.warn(
                    "'{}' is set to true for table {}, but it has no effect because {}.",
                    FlinkConnectorOptions.SINK_KEY_ONLY_DELETES_ENABLED.key(),
                    tableIdentifier.asSummaryString(),
                    reason);
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        if (overwrite && !context.isBounded()) {
            throw new UnsupportedOperationException(
                    "Paimon doesn't support streaming INSERT OVERWRITE.");
        }
        String name = tableIdentifier.asSummaryString();
        int[] generatedColumns = omittedGeneratedColumns(context);

        if (table instanceof FormatTable) {
            FormatTable formatTable = (FormatTable) table;
            return new PaimonDataStreamSinkProvider(
                    (dataStream) ->
                            new FlinkFormatTableDataStreamSink(
                                            formatTable, overwrite, staticPartitions)
                                    .sinkFrom(dataStream),
                    name,
                    table);
        }

        Options conf = Options.fromMap(table.options());
        // Do not sink to log store when overwrite mode
        return new PaimonDataStreamSinkProvider(
                (dataStream) -> {
                    DataStream<RowData> input = dataStream;
                    if (generatedColumns.length > 0) {
                        input =
                                input.map(
                                                new GeneratedColumnMapFunction(
                                                        table.rowType(), generatedColumns),
                                                input.getType())
                                        .name("Paimon generated columns");
                    }
                    FlinkSinkBuilder builder = createSinkBuilder();
                    builder.forRowData(
                            new DataStream<>(
                                    input.getExecutionEnvironment(), input.getTransformation()));
                    if (!conf.get(CLUSTERING_INCREMENTAL)
                            || conf.get(CLUSTERING_INCREMENTAL_OPTIMIZE_WRITE)) {
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
                },
                name,
                table);
    }

    private int[] omittedGeneratedColumns(Context context) {
        if (rowLevelUpdate || !GeneratedColumnRow.hasGeneratedColumns(table.rowType())) {
            return new int[0];
        }
        int[][] targets;
        try {
            // Flink 1.16 and 1.17 do not expose target columns. Keep their full-row writes working.
            targets = context.getTargetColumns().orElse(null);
        } catch (NoSuchMethodError e) {
            return new int[0];
        }
        if (targets == null || targets.length == 0) {
            return new int[0];
        }
        List<String> columnNames =
                this.context.getCatalogTable().getResolvedSchema().getColumnNames();
        Set<String> supplied = new HashSet<>(staticPartitions.keySet());
        for (int[] path : targets) {
            // Target paths include virtual columns, while the runtime row contains physical ones.
            supplied.add(columnNames.get(path[0]));
        }
        int[] omitted =
                IntStream.range(0, table.rowType().getFieldCount())
                        .filter(
                                i ->
                                        table.rowType().getFields().get(i).generationExpression()
                                                != null)
                        .filter(i -> !supplied.contains(table.rowType().getFields().get(i).name()))
                        .toArray();
        for (int index : omitted) {
            String name = table.rowType().getFields().get(index).name();
            if (table.primaryKeys().contains(name)
                    || (!table.primaryKeys().isEmpty() && table.partitionKeys().contains(name))) {
                throw new UnsupportedOperationException(
                        "Flink SQL does not support omitting generated column '"
                                + name
                                + "' used as a primary key or partition column in a primary-key table. "
                                + "Supply its value explicitly.");
            }
        }
        return omitted;
    }

    protected FlinkSinkBuilder createSinkBuilder() {
        return new FlinkSinkBuilder(table);
    }

    @Override
    public DynamicTableSink copy() {
        FlinkTableSink copied = new FlinkTableSink(tableIdentifier, table, context);
        copied.staticPartitions = new HashMap<>(staticPartitions);
        copied.overwrite = overwrite;
        copied.rowLevelUpdate = rowLevelUpdate;
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
