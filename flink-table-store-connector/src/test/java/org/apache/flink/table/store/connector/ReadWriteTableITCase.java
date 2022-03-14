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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.collection.JavaConverters;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.registerData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for testing querying managed table dml. */
@RunWith(Parameterized.class)
public class ReadWriteTableITCase extends TableStoreTestBase {

    private final boolean hasPk;
    @Nullable private final Boolean duplicate;

    public ReadWriteTableITCase(
            RuntimeExecutionMode executionMode,
            String tableName,
            boolean enableChangeTracking,
            boolean hasPk,
            @Nullable Boolean duplicate,
            ExpectedResult expectedResult) {
        super(executionMode, tableName, enableChangeTracking, expectedResult);
        this.hasPk = hasPk;
        this.duplicate = duplicate;
    }

    @Override
    public void after() {
        tEnv.executeSql("DROP TABLE `source_table`");
        super.after();
    }

    @Test
    public void testReadWriteNonPartitioned() throws Exception {
        String statement =
                String.format("INSERT INTO %s \nSELECT * FROM `source_table`", tableIdentifier);
        if (expectedResult.success) {
            tEnv.executeSql(statement).await();
            TableResult result =
                    tEnv.executeSql(String.format("SELECT * FROM %s", tableIdentifier));
            List<Row> actual = new ArrayList<>();
            try (CloseableIterator<Row> iterator = result.collect()) {
                while (iterator.hasNext()) {
                    actual.add(iterator.next());
                }
            }
            assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedResult.expectedRecords);
            String relativeFilePath = getRelativeFileStoreTablePath(tableIdentifier);
            // check snapshot file path
            assertThat(Paths.get(rootPath, relativeFilePath, "snapshot")).exists();
            // check manifest file path
            assertThat(Paths.get(rootPath, relativeFilePath, "manifest")).exists();

            if (enableChangeTracking) {
                assertThat(topicExists(tableIdentifier.asSummaryString())).isTrue();
            }
        } else {
            assertThatThrownBy(
                            () -> {
                                tEnv.executeSql(statement).await();
                                tEnv.executeSql(String.format("SELECT * FROM %s", tableIdentifier))
                                        .collect();
                            })
                    .isInstanceOf(expectedResult.expectedType)
                    .hasMessageContaining(expectedResult.expectedMessage);
        }
    }

    @Parameterized.Parameters(
            name =
                    "executionMode-{0}, tableName-{1}, "
                            + "enableChangeTracking-{2}, hasPk-{3},"
                            + " duplicate-{4}, expectedResult-{5}")
    public static List<Object[]> data() {
        List<Object[]> specs = new ArrayList<>();
        // batch cases
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.BATCH,
                    "table_" + UUID.randomUUID(),
                    false, // enable change-tracking
                    false, // has pk
                    false, // without duplicate
                    new ExpectedResult().success(true).expectedRecords(insertOnlyCities(false))
                });
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.BATCH,
                    "table_" + UUID.randomUUID(),
                    false, // enable change-tracking
                    false, // has pk
                    true, //  with duplicate
                    new ExpectedResult().success(true).expectedRecords(insertOnlyCities(true))
                });
        List<Row> expected = new ArrayList<>(rates());
        expected.remove(1);
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.BATCH,
                    "table_" + UUID.randomUUID(),
                    false, // enable change-tracking
                    true, // has pk
                    null, // without delete
                    new ExpectedResult().success(true).expectedRecords(expected)
                });
        // TODO: streaming with change-tracking

        // exception case
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.STREAMING,
                    "table_" + UUID.randomUUID(),
                    false, // enable change-tracking
                    false, // has pk
                    null, //  with duplicate
                    new ExpectedResult()
                            .success(false)
                            .expectedType(UnsupportedOperationException.class)
                            .expectedMessage("File store continuous mode is not supported yet.")
                });

        return specs;
    }

    @Override
    protected void prepareEnv() {
        if (hasPk) {
            if (executionMode == RuntimeExecutionMode.STREAMING) {
                registerUpsertRecordsWithPk();
            } else {
                registerInsertOnlyRecordsWithPk();
            }
        } else {
            if (duplicate != null) {
                registerInsertOnlyRecordsWithoutPk(duplicate);
            } else {
                registerInsertUpdateDeleteRecordsWithoutPk();
            }
        }
    }

    private void registerInsertUpdateDeleteRecordsWithoutPk() {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table (\n"
                                + "  user_id STRING,\n"
                                + "  user_name STRING,\n"
                                + "  email STRING,\n"
                                + "  balance DECIMAL(18,2)\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = '%s',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'changelog-mode' = 'I,UA,UB,D',\n"
                                + " 'disable-lookup' = 'true'\n"
                                + ")",
                        executionMode == RuntimeExecutionMode.BATCH,
                        registerData(TestData.userChangelog())));
        registerTableStoreSink();
    }

    private void registerInsertOnlyRecordsWithoutPk(boolean duplicate) {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table (\n"
                                + "  name STRING NOT NULL,\n"
                                + "  state STRING NOT NULL,\n"
                                + "  pop INT NOT NULL\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = '%s',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'changelog-mode' = 'I'\n"
                                + ")",
                        executionMode == RuntimeExecutionMode.BATCH,
                        registerData(insertOnlyCities(duplicate))));
        registerTableStoreSink();
    }

    private void registerInsertOnlyRecordsWithPk() {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table (\n"
                                + "  currency STRING,\n"
                                + "  rate BIGINT,\n"
                                + "  PRIMARY KEY (currency) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + " 'bounded' = '%s',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'changelog-mode' = 'I',\n"
                                + "  'disable-lookup' = 'true'\n"
                                + ")",
                        executionMode == RuntimeExecutionMode.BATCH, registerData(rates())));
        registerTableStoreSink();
    }

    private void registerUpsertRecordsWithPk() {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE source_table (\n"
                                + "  currency STRING,\n"
                                + "  rate BIGINT,\n"
                                + "  PRIMARY KEY (currency) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + " 'bounded' = '%s',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'changelog-mode' = 'UA,D',\n"
                                + "  'disable-lookup' = 'true'\n"
                                + ")",
                        executionMode == RuntimeExecutionMode.BATCH,
                        registerData(ratesChangelog())));
        registerTableStoreSink();
    }

    private static List<Row> rates() {
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("+I", "Euro", 119L));
    }

    private static List<Row> ratesChangelog() {
        return Arrays.asList(
                changelogRow("+I", "US Dollar", 102L),
                changelogRow("+I", "Euro", 114L),
                changelogRow("+I", "Yen", 1L),
                changelogRow("+U", "Euro", 116L),
                changelogRow("-D", "Euro", 116L),
                changelogRow("+I", "Euro", 119L),
                changelogRow("+U", "Euro", 119L),
                changelogRow("-D", "Yen", 1L));
    }

    private static List<Row> insertOnlyCities(boolean duplicate) {
        List<Row> cities = JavaConverters.seqAsJavaList(TestData.citiesData());
        return duplicate
                ? Stream.concat(cities.stream(), cities.stream()).collect(Collectors.toList())
                : cities;
    }

    private static List<Row> userChangelog() {
        return Arrays.asList(
                changelogRow("+I", "user1", "Tom", "tom123@gmail.com", new BigDecimal("8.10")),
                changelogRow("+I", "user3", "Bailey", "bailey@qq.com", new BigDecimal("9.99")),
                changelogRow("+I", "user4", "Tina", "tina@gmail.com", new BigDecimal("11.30")));
    }

    private void registerTableStoreSink() {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s LIKE `source_table` (EXCLUDING OPTIONS)",
                        tableIdentifier.asSerializableString()));
    }
}
