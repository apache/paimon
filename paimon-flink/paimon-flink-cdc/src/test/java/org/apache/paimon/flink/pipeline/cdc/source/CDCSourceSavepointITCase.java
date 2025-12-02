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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** The IT Case that verifies the checkpoint-restore functionalities for the cdc source. */
public class CDCSourceSavepointITCase extends CDCSourceITCaseBase {
    private static final TestTable[] TEST_TABLES =
            new TestTable[] {
                new TestTable(
                        "test1",
                        "test_table1",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("string_value", DataTypes.STRING())
                                .primaryKey("id")
                                .build(),
                        id -> String.format("(%d, 'test_string_%s')", id, id),
                        id -> String.format("%d, test_string_%s", id, id)),
                new TestTable(
                        "test2",
                        "test_table2",
                        Schema.newBuilder()
                                .column("append_index", DataTypes.INT())
                                .column("int_value", DataTypes.INT())
                                .build(),
                        id -> String.format("(%d, %d)", id, id),
                        id -> String.format("%d, %d", id, id)),
            };

    @TempDir private java.nio.file.Path savepointFolder;

    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();
    private final PrintStream standardOut = System.out;

    @BeforeEach
    public void initialize() throws Exception {
        super.initialize();
        for (TestTable testTable : TEST_TABLES) {
            catalog.createDatabase(testTable.database, true);
            catalog.createTable(testTable.identifier(), testTable.schema, true);
        }

        // Take over STDOUT as we need to check the output of values sink
        System.setOut(new PrintStream(outCaptor));
    }

    @AfterEach
    @Override
    public void afterEach() throws Exception {
        System.setOut(standardOut);
        super.afterEach();

        for (TestTable testTable : TEST_TABLES) {
            catalog.dropTable(testTable.identifier(), true);
        }

        for (TestTable testTable : TEST_TABLES) {
            catalog.dropDatabase(testTable.database, true, false);
        }
    }

    @Test
    public void testRescaleAndRecoverFromSavepoint() throws Exception {
        Configuration sinkConfig = new Configuration();

        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 2);

        jobClient = createAndExecutePipeline(env, sinkConfig, pipelineConfig);

        insertInto(TEST_TABLES[0], 0, 10);
        insertInto(TEST_TABLES[1], 0, 10);

        waitUtilExpectedResult(TEST_TABLES[0], 10);
        waitUtilExpectedResult(TEST_TABLES[1], 10);

        String savepointPath =
                jobClient
                        .stopWithSavepoint(
                                false,
                                savepointFolder.toAbsolutePath().toString(),
                                SavepointFormatType.DEFAULT)
                        .get(1, TimeUnit.MINUTES);

        // Before recovering from the savepoint, the following cases are constructed to be verified:
        // 1. Change the parallelism of the source
        // 2. Delete a table that was once synchronized

        catalog.dropTable(TEST_TABLES[0].identifier(), false);
        insertInto(TEST_TABLES[1], 10, 20);

        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath(savepointPath, false);
        SavepointRestoreSettings.toConfiguration(savepointRestoreSettings, conf);
        StreamExecutionEnvironment restoredEnv =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);

        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        jobClient = createAndExecutePipeline(restoredEnv, sinkConfig, pipelineConfig);

        waitUtilExpectedResult(TEST_TABLES[1], 20);

        insertInto(TEST_TABLES[1], 20, 30);

        waitUtilExpectedResult(TEST_TABLES[1], 30);

        waitUtilExpectedResult(TEST_TABLES[0], 10);
        // The deleted Paimon source table does not result in deleting the target table.
        assertThat(outCaptor.toString().split("\n")).noneMatch(x -> x.contains("DropTableEvent"));
    }

    private void insertInto(TestTable testTable, int from, int to) throws Exception {
        List<String> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(testTable.sqlLiteralGenerator.apply(i));
        }
        insertInto(testTable.tableId(), rows.toArray(new String[0]));
    }

    private void waitUtilExpectedResult(TestTable testTable, int endNum)
            throws InterruptedException {
        List<String> expectedResult = new ArrayList<>();
        for (int i = 0; i < endNum; i++) {
            expectedResult.add(
                    String.format(
                            "DataChangeEvent{tableId=%s, before=[], after=[%s], op=INSERT, meta=()}",
                            testTable.tableId(), testTable.valuesDataGenerator.apply(i)));
        }
        waitUtilExpectedResultInternal(testTable.tableId(), expectedResult);
    }

    @Override
    protected List<String> getActualResult(TableId tableId) {
        return Arrays.stream(outCaptor.toString().split("\n"))
                .filter(
                        x ->
                                x.contains(
                                        String.format(
                                                "DataChangeEvent{tableId=%s",
                                                tableId.getSchemaName())))
                .map(x -> x.substring(x.indexOf("DataChangeEvent")))
                .collect(Collectors.toList());
    }

    private static class TestTable {
        private final String database;
        private final String tableName;
        private final Schema schema;
        private final Function<Integer, String> sqlLiteralGenerator;
        private final Function<Integer, String> valuesDataGenerator;

        private TestTable(
                String database,
                String tableName,
                Schema schema,
                Function<Integer, String> sqlLiteralGenerator,
                Function<Integer, String> valuesDataGenerator) {
            this.database = database;
            this.tableName = tableName;
            this.schema = schema;
            this.sqlLiteralGenerator = sqlLiteralGenerator;
            this.valuesDataGenerator = valuesDataGenerator;
        }

        private Identifier identifier() {
            return Identifier.create(database, tableName);
        }

        private TableId tableId() {
            return TableId.tableId(database, tableName);
        }
    }
}
