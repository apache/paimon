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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.flink.action.cdc.kafka.KafkaSyncDatabaseActionFactory;
import org.apache.paimon.flink.action.cdc.kafka.KafkaSyncTableActionFactory;
import org.apache.paimon.flink.action.cdc.mongodb.MongoDBSyncDatabaseActionFactory;
import org.apache.paimon.flink.action.cdc.mongodb.MongoDBSyncTableActionFactory;
import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncDatabaseActionFactory;
import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableActionFactory;
import org.apache.paimon.flink.action.cdc.postgres.PostgresSyncTableActionFactory;
import org.apache.paimon.flink.action.cdc.pulsar.PulsarSyncDatabaseActionFactory;
import org.apache.paimon.flink.action.cdc.pulsar.PulsarSyncTableActionFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** CDC IT case base. */
@SuppressWarnings("BusyWait")
public class CdcActionITCaseBase extends ActionITCaseBase {

    private static final Logger LOG = LoggerFactory.getLogger(CdcActionITCaseBase.class);

    protected StreamExecutionEnvironment env;

    @BeforeEach
    public void setEnv() {
        env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(2)
                        .checkpointIntervalMs(1000)
                        .build();
    }

    @AfterEach
    public void closeEnv() throws Exception {
        env.close();
    }

    protected void waitingTables(String... tables) throws Exception {
        waitingTables(Arrays.asList(tables));
    }

    protected void waitingTables(List<String> tables) throws Exception {
        LOG.info("Waiting for tables '{}'", tables);

        while (true) {
            List<String> actualTables = catalog.listTables(database);
            if (actualTables.containsAll(tables)) {
                break;
            }
            Thread.sleep(100);
        }
    }

    protected void assertExactlyExistTables(List<String> tableNames) throws Exception {
        assertExactlyExistTables(tableNames.toArray(new String[0]));
    }

    protected void assertExactlyExistTables(String... tableNames) throws Exception {
        assertThat(catalog.listTables(database)).containsExactlyInAnyOrder(tableNames);
    }

    protected void assertTableNotExists(List<String> tableNames) throws Exception {
        assertTableNotExists(tableNames.toArray(new String[0]));
    }

    protected void assertTableNotExists(String... tableNames) throws Exception {
        assertThat(catalog.listTables(database)).doesNotContain(tableNames);
    }

    protected void waitForResult(
            List<String> expected, FileStoreTable table, RowType rowType, List<String> primaryKeys)
            throws Exception {
        assertThat(table.schema().primaryKeys()).isEqualTo(primaryKeys);

        // wait for table schema to become our expected schema
        while (true) {
            if (rowType.getFieldCount() == table.schema().fields().size()) {
                int cnt = 0;
                for (int i = 0; i < table.schema().fields().size(); i++) {
                    DataField field = table.schema().fields().get(i);
                    boolean sameName = field.name().equals(rowType.getFieldNames().get(i));
                    boolean sameType = field.type().equals(rowType.getFieldTypes().get(i));
                    if (sameName && sameType) {
                        cnt++;
                    } else {
                        System.out.println("ss");
                    }
                }
                if (cnt == rowType.getFieldCount()) {
                    break;
                }
            }
            table = table.copyWithLatestSchema();
            Thread.sleep(1000);
        }

        // wait for data to become expected
        List<String> sortedExpected = new ArrayList<>(expected);
        Collections.sort(sortedExpected);
        while (true) {
            ReadBuilder readBuilder = table.newReadBuilder();
            TableScan.Plan plan = readBuilder.newScan().plan();
            List<String> result =
                    getResult(
                            readBuilder.newRead(),
                            plan == null ? Collections.emptyList() : plan.splits(),
                            rowType);
            List<String> sortedActual = new ArrayList<>(result);
            Collections.sort(sortedActual);
            if (sortedExpected.equals(sortedActual)) {
                break;
            }
            Thread.sleep(1000);
        }
    }

    protected Map<String, String> getBasicTableConfig() {
        Map<String, String> config = new HashMap<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        config.put("bucket", String.valueOf(random.nextInt(3) + 1));
        config.put("sink.parallelism", String.valueOf(random.nextInt(3) + 1));
        return config;
    }

    protected List<String> mapToArgs(String argKey, Map<String, String> map) {
        List<String> args = new ArrayList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            args.add(argKey);
            args.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
        }
        return args;
    }

    protected List<String> listToArgs(String argKey, List<String> list) {
        if (list.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.asList(argKey, String.join(",", list));
    }

    protected List<String> listToMultiArgs(String argKey, List<String> list) {
        List<String> args = new ArrayList<>();
        for (String v : list) {
            args.add(argKey);
            args.add(v);
        }
        return args;
    }

    protected <T> List<String> nullableToArgs(String argKey, @Nullable T nullable) {
        if (nullable == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(argKey, nullable.toString());
    }

    public JobClient runActionWithDefaultEnv(ActionBase action) throws Exception {
        action.withStreamExecutionEnvironment(env).build();
        JobClient client = env.executeAsync();
        waitJobRunning(client);
        return client;
    }

    protected void waitJobRunning(JobClient client) throws Exception {
        while (true) {
            JobStatus status = client.getJobStatus().get();
            if (status == JobStatus.RUNNING) {
                break;
            }
            Thread.sleep(1000);
        }
    }

    private <T> String getActionName(Class<T> clazz) {
        switch (clazz.getSimpleName()) {
            case "MySqlSyncTableAction":
                return MySqlSyncTableActionFactory.IDENTIFIER;
            case "MySqlSyncDatabaseAction":
                return MySqlSyncDatabaseActionFactory.IDENTIFIER;
            case "KafkaSyncTableAction":
                return KafkaSyncTableActionFactory.IDENTIFIER;
            case "KafkaSyncDatabaseAction":
                return KafkaSyncDatabaseActionFactory.IDENTIFIER;
            case "MongoDBSyncTableAction":
                return MongoDBSyncTableActionFactory.IDENTIFIER;
            case "MongoDBSyncDatabaseAction":
                return MongoDBSyncDatabaseActionFactory.IDENTIFIER;
            case "PulsarSyncTableAction":
                return PulsarSyncTableActionFactory.IDENTIFIER;
            case "PulsarSyncDatabaseAction":
                return PulsarSyncDatabaseActionFactory.IDENTIFIER;
            case "PostgresSyncTableAction":
                return PostgresSyncTableActionFactory.IDENTIFIER;
            default:
                throw new UnsupportedOperationException(
                        "Unknown sync action: " + clazz.getSimpleName());
        }
    }

    private <T> String getConfKey(Class<T> clazz) {
        switch (clazz.getSimpleName()) {
            case "MySqlSyncTableAction":
            case "MySqlSyncDatabaseAction":
                return "--" + CdcActionCommonUtils.MYSQL_CONF;
            case "KafkaSyncTableAction":
            case "KafkaSyncDatabaseAction":
                return "--" + CdcActionCommonUtils.KAFKA_CONF;
            case "MongoDBSyncTableAction":
            case "MongoDBSyncDatabaseAction":
                return "--" + CdcActionCommonUtils.MONGODB_CONF;
            case "PulsarSyncTableAction":
            case "PulsarSyncDatabaseAction":
                return "--" + CdcActionCommonUtils.PULSAR_CONF;
            case "PostgresSyncTableAction":
                return "--" + CdcActionCommonUtils.POSTGRES_CONF;
            default:
                throw new UnsupportedOperationException(
                        "Unknown sync action: " + clazz.getSimpleName());
        }
    }

    /** Base builder to build table synchronization action from action arguments. */
    protected abstract class SyncTableActionBuilder<T extends SynchronizationActionBase> {

        private final Class<T> clazz;
        private final Map<String, String> sourceConfig;

        private Map<String, String> catalogConfig = Collections.emptyMap();
        private Map<String, String> tableConfig = Collections.emptyMap();
        private final List<String> partitionKeys = new ArrayList<>();
        private final List<String> primaryKeys = new ArrayList<>();
        private final List<String> computedColumnArgs = new ArrayList<>();
        private final List<String> typeMappingModes = new ArrayList<>();
        private final List<String> metadataColumns = new ArrayList<>();

        public SyncTableActionBuilder(Class<T> clazz, Map<String, String> sourceConfig) {
            this.clazz = clazz;
            this.sourceConfig = sourceConfig;
        }

        public SyncTableActionBuilder<T> withCatalogConfig(Map<String, String> catalogConfig) {
            this.catalogConfig = catalogConfig;
            return this;
        }

        public SyncTableActionBuilder<T> withTableConfig(Map<String, String> tableConfig) {
            this.tableConfig = tableConfig;
            return this;
        }

        public SyncTableActionBuilder<T> withPartitionKeys(String... partitionKeys) {
            this.partitionKeys.addAll(Arrays.asList(partitionKeys));
            return this;
        }

        public SyncTableActionBuilder<T> withPrimaryKeys(String... primaryKeys) {
            this.primaryKeys.addAll(Arrays.asList(primaryKeys));
            return this;
        }

        public SyncTableActionBuilder<T> withComputedColumnArgs(String... computedColumnArgs) {
            return withComputedColumnArgs(Arrays.asList(computedColumnArgs));
        }

        public SyncTableActionBuilder<T> withComputedColumnArgs(List<String> computedColumnArgs) {
            this.computedColumnArgs.addAll(computedColumnArgs);
            return this;
        }

        public SyncTableActionBuilder<T> withTypeMappingModes(String... typeMappingModes) {
            this.typeMappingModes.addAll(Arrays.asList(typeMappingModes));
            return this;
        }

        public SyncTableActionBuilder<T> withMetadataColumns(String... metadataColumns) {
            this.metadataColumns.addAll(Arrays.asList(metadataColumns));
            return this;
        }

        public T build() {
            List<String> args =
                    new ArrayList<>(
                            Arrays.asList(
                                    getActionName(clazz),
                                    "--warehouse",
                                    warehouse,
                                    "--database",
                                    database,
                                    "--table",
                                    tableName));

            args.addAll(mapToArgs(getConfKey(clazz), sourceConfig));
            args.addAll(mapToArgs("--catalog-conf", catalogConfig));
            args.addAll(mapToArgs("--table-conf", tableConfig));

            args.addAll(listToArgs("--partition-keys", partitionKeys));
            args.addAll(listToArgs("--primary-keys", primaryKeys));
            args.addAll(listToArgs("--type-mapping", typeMappingModes));

            args.addAll(listToMultiArgs("--computed-column", computedColumnArgs));
            args.addAll(listToMultiArgs("--metadata-column", metadataColumns));

            return createAction(clazz, args);
        }
    }

    /** Base Builder to build database synchronization from action arguments. */
    protected abstract class SyncDatabaseActionBuilder<T extends SynchronizationActionBase> {

        private final Class<T> clazz;
        private final Map<String, String> sourceConfig;

        private Map<String, String> catalogConfig = Collections.emptyMap();
        private Map<String, String> tableConfig = Collections.emptyMap();
        @Nullable private Boolean ignoreIncompatible;
        @Nullable private Boolean mergeShards;
        @Nullable private String tablePrefix;
        @Nullable private String tableSuffix;
        @Nullable private String includingTables;
        @Nullable private String excludingTables;
        @Nullable private String mode;
        private final List<String> typeMappingModes = new ArrayList<>();
        private final List<String> partitionKeys = new ArrayList<>();
        private final List<String> primaryKeys = new ArrayList<>();
        private final List<String> metadataColumn = new ArrayList<>();

        public SyncDatabaseActionBuilder(Class<T> clazz, Map<String, String> sourceConfig) {
            this.clazz = clazz;
            this.sourceConfig = sourceConfig;
        }

        public SyncDatabaseActionBuilder<T> withCatalogConfig(Map<String, String> catalogConfig) {
            this.catalogConfig = catalogConfig;
            return this;
        }

        public SyncDatabaseActionBuilder<T> withTableConfig(Map<String, String> tableConfig) {
            this.tableConfig = tableConfig;
            return this;
        }

        public SyncDatabaseActionBuilder<T> ignoreIncompatible(boolean ignoreIncompatible) {
            this.ignoreIncompatible = ignoreIncompatible;
            return this;
        }

        public SyncDatabaseActionBuilder<T> mergeShards(boolean mergeShards) {
            this.mergeShards = mergeShards;
            return this;
        }

        public SyncDatabaseActionBuilder<T> withTablePrefix(String tablePrefix) {
            this.tablePrefix = tablePrefix;
            return this;
        }

        public SyncDatabaseActionBuilder<T> withTableSuffix(String tableSuffix) {
            this.tableSuffix = tableSuffix;
            return this;
        }

        public SyncDatabaseActionBuilder<T> includingTables(String includingTables) {
            this.includingTables = includingTables;
            return this;
        }

        public SyncDatabaseActionBuilder<T> excludingTables(String excludingTables) {
            this.excludingTables = excludingTables;
            return this;
        }

        public SyncDatabaseActionBuilder<T> withMode(String mode) {
            this.mode = mode;
            return this;
        }

        public SyncDatabaseActionBuilder<T> withTypeMappingModes(String... typeMappingModes) {
            this.typeMappingModes.addAll(Arrays.asList(typeMappingModes));
            return this;
        }

        public SyncDatabaseActionBuilder<T> withPartitionKeys(String... partitionKeys) {
            this.partitionKeys.addAll(Arrays.asList(partitionKeys));
            return this;
        }

        public SyncDatabaseActionBuilder<T> withPrimaryKeys(String... primaryKeys) {
            this.primaryKeys.addAll(Arrays.asList(primaryKeys));
            return this;
        }

        public SyncDatabaseActionBuilder<T> withMetadataColumn(List<String> metadataColumn) {
            this.metadataColumn.addAll(metadataColumn);
            return this;
        }

        public T build() {
            List<String> args =
                    new ArrayList<>(
                            Arrays.asList(
                                    getActionName(clazz),
                                    "--warehouse",
                                    warehouse,
                                    "--database",
                                    database));

            args.addAll(mapToArgs(getConfKey(clazz), sourceConfig));
            args.addAll(mapToArgs("--catalog-conf", catalogConfig));
            args.addAll(mapToArgs("--table-conf", tableConfig));

            args.addAll(nullableToArgs("--ignore-incompatible", ignoreIncompatible));
            args.addAll(nullableToArgs("--merge-shards", mergeShards));
            args.addAll(nullableToArgs("--table-prefix", tablePrefix));
            args.addAll(nullableToArgs("--table-suffix", tableSuffix));
            args.addAll(nullableToArgs("--including-tables", includingTables));
            args.addAll(nullableToArgs("--excluding-tables", excludingTables));
            args.addAll(nullableToArgs("--mode", mode));

            args.addAll(listToArgs("--type-mapping", typeMappingModes));
            args.addAll(listToArgs("--partition-keys", partitionKeys));
            args.addAll(listToArgs("--primary-keys", primaryKeys));
            args.addAll(listToArgs("--metadata-column", metadataColumn));

            return createAction(clazz, args);
        }
    }
}
