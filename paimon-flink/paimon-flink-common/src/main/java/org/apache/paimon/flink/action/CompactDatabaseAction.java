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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.MultiTableUnawareAppendCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.compact.UnawareBucketCompactionTopoBuilder;
import org.apache.paimon.flink.sink.BucketsRowChannelComputer;
import org.apache.paimon.flink.sink.CombinedTableCompactorSink;
import org.apache.paimon.flink.sink.CompactorSinkBuilder;
import org.apache.paimon.flink.source.CombinedTableCompactorSourceBuilder;
import org.apache.paimon.flink.source.CompactorSourceBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/** Database compact action for Flink. */
public class CompactDatabaseAction extends ActionBase {
    private static final Logger LOG = LoggerFactory.getLogger(CompactDatabaseAction.class);

    private Pattern includingPattern = Pattern.compile(".*");
    @Nullable private Pattern excludingPattern;
    private Pattern databasePattern = Pattern.compile(".*");

    private MultiTablesSinkMode databaseCompactMode = MultiTablesSinkMode.DIVIDED;

    private final Map<String, FileStoreTable> tableMap = new HashMap<>();

    private Options tableOptions = new Options();

    public CompactDatabaseAction(String warehouse, Map<String, String> catalogConfig) {
        super(warehouse, catalogConfig);
    }

    public CompactDatabaseAction includingDatabases(@Nullable String includingDatabases) {
        if (includingDatabases != null) {
            this.databasePattern = Pattern.compile(includingDatabases);
        }
        return this;
    }

    public CompactDatabaseAction includingTables(@Nullable String includingTables) {
        if (includingTables != null) {
            this.includingPattern = Pattern.compile(includingTables);
        }
        return this;
    }

    public CompactDatabaseAction excludingTables(@Nullable String excludingTables) {
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        return this;
    }

    public CompactDatabaseAction withDatabaseCompactMode(@Nullable String mode) {
        this.databaseCompactMode = MultiTablesSinkMode.fromString(mode);
        return this;
    }

    public CompactDatabaseAction withTableOptions(Map<String, String> tableOptions) {
        this.tableOptions = Options.fromMap(tableOptions);
        return this;
    }

    private boolean shouldCompactionTable(String paimonFullTableName) {
        boolean shouldCompaction = includingPattern.matcher(paimonFullTableName).matches();
        if (excludingPattern != null) {
            shouldCompaction =
                    shouldCompaction && !excludingPattern.matcher(paimonFullTableName).matches();
        }
        if (!shouldCompaction) {
            LOG.debug("Source table '{}' is excluded.", paimonFullTableName);
        }
        return shouldCompaction;
    }

    @Override
    public void build() {
        if (databaseCompactMode == MultiTablesSinkMode.DIVIDED) {
            buildForDividedMode();
        } else {
            buildForCombinedMode();
        }
    }

    private void buildForDividedMode() {
        try {
            List<String> databases = catalog.listDatabases();
            for (String databaseName : databases) {
                Matcher databaseMatcher = databasePattern.matcher(databaseName);
                if (databaseMatcher.matches()) {
                    List<String> tables = catalog.listTables(databaseName);
                    for (String tableName : tables) {
                        String fullTableName = String.format("%s.%s", databaseName, tableName);
                        if (shouldCompactionTable(fullTableName)) {
                            Table table =
                                    catalog.getTable(Identifier.create(databaseName, tableName));
                            if (!(table instanceof FileStoreTable)) {
                                LOG.error(
                                        String.format(
                                                "Only FileStoreTable supports compact action. The table type is '%s'.",
                                                table.getClass().getName()));
                                continue;
                            }
                            Map<String, String> dynamicOptions =
                                    new HashMap<>(tableOptions.toMap());
                            dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
                            FileStoreTable fileStoreTable =
                                    (FileStoreTable) table.copy(dynamicOptions);
                            tableMap.put(fullTableName, fileStoreTable);
                        } else {
                            LOG.debug("The table {} is excluded.", fullTableName);
                        }
                    }
                }
            }
        } catch (Catalog.DatabaseNotExistException | Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }

        Preconditions.checkState(
                !tableMap.isEmpty(),
                "no tables to be compacted. possible cause is that there are no tables detected after pattern matching");

        ReadableConfig conf = env.getConfiguration();
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        for (Map.Entry<String, FileStoreTable> entry : tableMap.entrySet()) {
            FileStoreTable fileStoreTable = entry.getValue();
            switch (fileStoreTable.bucketMode()) {
                case BUCKET_UNAWARE:
                    {
                        buildForUnawareBucketCompaction(
                                env, entry.getKey(), fileStoreTable, isStreaming);
                        break;
                    }
                case HASH_FIXED:
                case HASH_DYNAMIC:
                default:
                    {
                        buildForTraditionalCompaction(
                                env, entry.getKey(), fileStoreTable, isStreaming);
                    }
            }
        }
    }

    private void buildForCombinedMode() {

        ReadableConfig conf = env.getConfiguration();
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        CombinedTableCompactorSourceBuilder sourceBuilder =
                new CombinedTableCompactorSourceBuilder(
                        catalogLoader(),
                        databasePattern,
                        includingPattern,
                        excludingPattern,
                        tableOptions.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis());

        // multi bucket table which has multi bucket in a partition like fix bucket and dynamic
        // bucket
        DataStream<RowData> awareBucketTableSource =
                partition(
                        sourceBuilder
                                .withEnv(env)
                                .withContinuousMode(isStreaming)
                                .buildAwareBucketTableSource(),
                        new BucketsRowChannelComputer(),
                        tableOptions.get(FlinkConnectorOptions.SINK_PARALLELISM));

        // unaware bucket table
        DataStream<MultiTableUnawareAppendCompactionTask> unawareBucketTableSource =
                sourceBuilder
                        .withEnv(env)
                        .withContinuousMode(isStreaming)
                        .buildForUnawareBucketsTableSource();

        new CombinedTableCompactorSink(catalogLoader(), tableOptions)
                .sinkFrom(awareBucketTableSource, unawareBucketTableSource);
    }

    private void buildForTraditionalCompaction(
            StreamExecutionEnvironment env,
            String fullName,
            FileStoreTable table,
            boolean isStreaming) {

        CompactorSourceBuilder sourceBuilder = new CompactorSourceBuilder(fullName, table);
        CompactorSinkBuilder sinkBuilder = new CompactorSinkBuilder(table);

        DataStreamSource<RowData> source =
                sourceBuilder.withEnv(env).withContinuousMode(isStreaming).build();
        sinkBuilder.withInput(source).build();
    }

    private void buildForUnawareBucketCompaction(
            StreamExecutionEnvironment env,
            String fullName,
            FileStoreTable table,
            boolean isStreaming) {
        UnawareBucketCompactionTopoBuilder unawareBucketCompactionTopoBuilder =
                new UnawareBucketCompactionTopoBuilder(env, fullName, table);

        unawareBucketCompactionTopoBuilder.withContinuousMode(isStreaming);
        unawareBucketCompactionTopoBuilder.build();
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Compact database job");
    }
}
