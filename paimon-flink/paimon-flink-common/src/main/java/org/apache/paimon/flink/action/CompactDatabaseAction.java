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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.compact.UnawareBucketCompactionTopoBuilder;
import org.apache.paimon.flink.sink.CompactorSinkBuilder;
import org.apache.paimon.flink.sink.MultiTablesCompactorSinkBuilder;
import org.apache.paimon.flink.source.CompactorSourceBuilder;
import org.apache.paimon.flink.source.MultiTablesCompactorSourceBuilder;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Database compact action for Flink. */
public class CompactDatabaseAction extends ActionBase {
    private static final Logger LOG = LoggerFactory.getLogger(CompactDatabaseAction.class);

    private final Pattern includingPattern;
    private final Pattern excludingPattern;
    private final Pattern databasePattern;

    private MultiTablesSinkMode databaseCompactMode = MultiTablesSinkMode.DIVIDED;

    private Map<String, Table> tableMap = new HashMap<>();

    @Nullable private Options compactOptions = null;

    public CompactDatabaseAction(
            String warehouse,
            @Nullable String includingDatabases,
            @Nullable String includingTables,
            @Nullable String excludingTables,
            Map<String, String> catalogConfig) {
        super(warehouse, catalogConfig);
        this.databasePattern =
                Pattern.compile(includingDatabases == null ? ".*" : includingDatabases);
        this.includingPattern = Pattern.compile(includingTables == null ? ".*" : includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
    }

    public CompactDatabaseAction withDatabaseCompactMode(String mode) {
        this.databaseCompactMode = MultiTablesSinkMode.fromString(mode);
        return this;
    }

    public CompactDatabaseAction withCompactOptions(Map<String, String> compactOptions) {
        this.compactOptions = Options.fromMap(compactOptions);
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
    public void build(StreamExecutionEnvironment env) {
        if (databaseCompactMode == MultiTablesSinkMode.DIVIDED) {
            buildForDividedMode(env);
        } else {
            buildForCombinedMode(env);
        }
    }

    private void buildForDividedMode(StreamExecutionEnvironment env) {
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
                            table =
                                    table.copy(
                                            Collections.singletonMap(
                                                    CoreOptions.WRITE_ONLY.key(), "false"));
                            tableMap.put(fullTableName, table);
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

        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        for (Map.Entry<String, Table> entry : tableMap.entrySet()) {
            FileStoreTable fileStoreTable = (FileStoreTable) entry.getValue();
            switch (fileStoreTable.bucketMode()) {
                case UNAWARE:
                    {
                        buildForUnawareBucketCompaction(
                                env,
                                entry.getKey(),
                                (AppendOnlyFileStoreTable) entry.getValue(),
                                isStreaming);
                        break;
                    }
                case FIXED:
                case DYNAMIC:
                default:
                    {
                        buildForTraditionalCompaction(
                                env, entry.getKey(), fileStoreTable, isStreaming);
                    }
            }
        }
    }

    private void buildForCombinedMode(StreamExecutionEnvironment env) {
        Preconditions.checkArgument(
                compactOptions != null, "in combined mode, compact options need to be set.");

        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        // Currently, multi-tables compaction do not support tables which bucketmode is UNWARE.
        MultiTablesCompactorSourceBuilder sourceBuilder =
                new MultiTablesCompactorSourceBuilder(
                        catalogLoader(),
                        databasePattern,
                        includingPattern,
                        excludingPattern,
                        compactOptions.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis());

        MultiTablesCompactorSinkBuilder sinkBuilder =
                new MultiTablesCompactorSinkBuilder(catalogLoader(), compactOptions);

        DataStream<RowData> source =
                sourceBuilder.withEnv(env).withContinuousMode(isStreaming).build();

        sinkBuilder.withInput(source).build();
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
            AppendOnlyFileStoreTable table,
            boolean isStreaming) {
        UnawareBucketCompactionTopoBuilder unawareBucketCompactionTopoBuilder =
                new UnawareBucketCompactionTopoBuilder(env, fullName, table);

        unawareBucketCompactionTopoBuilder.withContinuousMode(isStreaming);
        unawareBucketCompactionTopoBuilder.build();
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        execute(env, "Compact database job");
    }
}
