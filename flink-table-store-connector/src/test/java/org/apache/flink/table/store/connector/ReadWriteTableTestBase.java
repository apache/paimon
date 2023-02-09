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

package org.apache.flink.table.store.connector;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.table.store.connector.FlinkConnectorOptions.LOG_SYSTEM;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.ROOT_PATH;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.prepareHelperSourceWithChangelogRecords;
import static org.apache.flink.table.store.connector.ReadWriteTableTestUtil.prepareHelperSourceWithInsertOnlyRecords;
import static org.apache.flink.table.store.connector.ShowCreateUtil.buildInsertIntoQuery;
import static org.apache.flink.table.store.connector.ShowCreateUtil.buildSelectQuery;
import static org.apache.flink.table.store.connector.ShowCreateUtil.createTableLikeDDL;
import static org.apache.flink.table.store.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.assertj.core.api.Assertions.assertThat;

/** Table store read write test base. */
public class ReadWriteTableTestBase extends KafkaTableTestBase {
    protected String rootPath;

    // ------------------------ Tools ----------------------------------

    protected static void assertNoMoreRecords(BlockingIterator<Row, Row> iterator)
            throws Exception {
        List<Row> expectedRecords = Collections.emptyList();
        try {
            // set expectation size to 1 to let time pass by until timeout
            // just wait 5s to avoid too long time
            expectedRecords = iterator.collect(1, 5L, TimeUnit.SECONDS);
            iterator.close();
        } catch (TimeoutException ignored) {
            // don't throw exception
        }
        assertThat(expectedRecords).isEmpty();
    }

    protected Tuple2<String, String> createSourceAndManagedTable(
            boolean streaming,
            boolean enableLogStore,
            boolean insertOnly,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<Tuple2<String, String>> computedColumnExpressions,
            @Nullable WatermarkSpec watermarkSpec)
            throws Exception {
        Map<String, String> tableOptions = new HashMap<>();
        rootPath = TEMPORARY_FOLDER.newFolder().getPath();
        tableOptions.put(ROOT_PATH.key(), rootPath);
        if (enableLogStore) {
            tableOptions.put(LOG_SYSTEM.key(), "kafka");
            tableOptions.put(BOOTSTRAP_SERVERS.key(), getBootstrapServers());
        }
        String sourceTable = "source_table_" + UUID.randomUUID();
        String managedTable = "managed_table_" + UUID.randomUUID();
        EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance().inStreamingMode();
        String helperTableDdl;
        if (streaming) {
            helperTableDdl =
                    insertOnly
                            ? prepareHelperSourceWithInsertOnlyRecords(
                                    sourceTable, partitionKeys, primaryKeys, watermarkSpec != null)
                            : prepareHelperSourceWithChangelogRecords(
                                    sourceTable, partitionKeys, primaryKeys, watermarkSpec != null);
            env = buildStreamEnv();
            builder.inStreamingMode();
        } else {
            helperTableDdl =
                    prepareHelperSourceWithInsertOnlyRecords(
                            sourceTable, partitionKeys, primaryKeys, watermarkSpec != null);
            env = buildBatchEnv();
            builder.inBatchMode();
        }
        tEnv = StreamTableEnvironment.create(env, builder.build());
        tEnv.executeSql(helperTableDdl);

        String managedTableDdl;
        if (computedColumnExpressions.isEmpty()) {
            managedTableDdl =
                    createTableLikeDDL(sourceTable, managedTable, tableOptions, watermarkSpec);
            tEnv.executeSql(managedTableDdl);
        } else {
            String cat = tEnv.getCurrentCatalog();
            String db = tEnv.getCurrentDatabase();
            ObjectPath objectPath = new ObjectPath(db, sourceTable);
            ResolvedCatalogTable helperSource =
                    (ResolvedCatalogTable) tEnv.getCatalog(cat).get().getTable(objectPath);
            Schema.Builder schemaBuilder =
                    Schema.newBuilder().fromSchema(helperSource.getUnresolvedSchema());
            computedColumnExpressions.forEach(
                    tuple -> schemaBuilder.columnByExpression(tuple.f0, tuple.f1));

            if (watermarkSpec != null) {
                schemaBuilder.watermark(watermarkSpec.columnName, watermarkSpec.expressionAsString);
            }

            TableDescriptor.Builder descriptorBuilder =
                    TableDescriptor.forManaged()
                            .partitionedBy(helperSource.getPartitionKeys().toArray(new String[0]))
                            .schema(schemaBuilder.build());
            tableOptions.forEach(descriptorBuilder::option);
            tEnv.createTable(
                    ObjectIdentifier.of(cat, db, managedTable).asSerializableString(),
                    descriptorBuilder.build());
        }
        return new Tuple2<>(sourceTable, managedTable);
    }

    protected Tuple2<String, BlockingIterator<Row, Row>> collectAndCheckUnderSameEnv(
            boolean streaming,
            boolean enableLogStore,
            boolean insertOnly,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<Tuple2<String, String>> computedColumnExpressions,
            @Nullable WatermarkSpec watermarkSpec,
            boolean writeFirst,
            Map<String, String> readHints,
            @Nullable String filter,
            List<String> projection,
            List<Row> expected)
            throws Exception {
        Tuple2<String, String> tables =
                createSourceAndManagedTable(
                        streaming,
                        enableLogStore,
                        insertOnly,
                        partitionKeys,
                        primaryKeys,
                        computedColumnExpressions,
                        watermarkSpec);
        String sourceTable = tables.f0;
        String managedTable = tables.f1;

        String insertQuery = buildInsertIntoQuery(sourceTable, managedTable);
        String selectQuery = buildSelectQuery(managedTable, readHints, filter, projection);

        BlockingIterator<Row, Row> iterator;
        if (writeFirst) {
            tEnv.executeSql(insertQuery).await();
            iterator = BlockingIterator.of(tEnv.executeSql(selectQuery).collect());
        } else {
            iterator = BlockingIterator.of(tEnv.executeSql(selectQuery).collect());
            tEnv.executeSql(insertQuery).await();
        }
        if (expected.isEmpty()) {
            assertNoMoreRecords(iterator);
        } else {
            assertThat(iterator.collect(expected.size(), 10, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
        return Tuple2.of(managedTable, iterator);
    }

    protected static StreamExecutionEnvironment buildStreamEnv() {
        return buildStreamEnv(2);
    }

    protected static StreamExecutionEnvironment buildStreamEnv(int parallelism) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);
        env.setParallelism(parallelism);
        return env;
    }

    protected static StreamExecutionEnvironment buildBatchEnv() {
        return buildBatchEnv(2);
    }

    protected static StreamExecutionEnvironment buildBatchEnv(int parallelism) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(parallelism);
        return env;
    }

    /** A POJO class to help assign watermark by string. */
    protected static class WatermarkSpec {
        String columnName;
        String expressionAsString;

        private WatermarkSpec(String columnName, String expressionAsString) {
            this.columnName = columnName;
            this.expressionAsString = expressionAsString;
        }

        protected static WatermarkSpec of(String columnName, String expressionAsString) {
            return new WatermarkSpec(columnName, expressionAsString);
        }
    }
}
