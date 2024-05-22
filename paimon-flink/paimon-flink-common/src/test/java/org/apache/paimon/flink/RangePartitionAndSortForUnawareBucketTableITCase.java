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

package org.apache.paimon.flink;

import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * The IT test for the range partitioning and sort batch writing for append-only bucket unaware
 * tables.
 */
public class RangePartitionAndSortForUnawareBucketTableITCase extends CatalogITCaseBase {

    private static final int SINK_ROW_NUMBER = 1000;

    @Test
    public void testSortConfigurationChecks() {
        batchSql(
                "CREATE TEMPORARY TABLE source1 (col1 INT, col2 INT, col3 INT, col4 INT) "
                        + "WITH ('connector'='values', 'bounded'='true')");
        batchSql("CREATE TABLE IF NOT EXISTS sink1 (col1 INT, col2 INT, col3 INT, col4 INT)");
        streamSqlIter(
                "CREATE TEMPORARY TABLE source2 (col1 INT, col2 INT, col3 INT, col4 INT) WITH ('connector'='values')");
        streamSqlIter("CREATE TABLE IF NOT EXISTS sink2 (col1 INT, col2 INT, col3 INT, col4 INT)");
        // 1. Check the sort columns.
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(
                        () ->
                                batchSql(
                                        "INSERT INTO sink1 /*+ OPTIONS('sink.clustering.by-columns' = 'col1,xx1') */ "
                                                + "SELECT * FROM source1"))
                .withMessageContaining("should contains all clustering column names");
        // 2. Check the sample factor.
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(
                        () ->
                                batchSql(
                                        "INSERT INTO sink1 /*+ OPTIONS('sink.clustering.by-columns' = 'col1', "
                                                + "'sink.clustering.sample-factor' = '10') */ SELECT * "
                                                + "FROM source1"))
                .withMessageContaining("The minimum allowed sink.clustering.sample-factor");
        // 3. Check the sink parallelism.
        batchSql(
                "CREATE TEMPORARY TABLE source3 (col1 INT, col2 INT) WITH ('connector'='values', 'bounded'='true')");
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(
                        () ->
                                batchSql(
                                        "INSERT INTO sink1 /*+ OPTIONS('sink.clustering.by-columns' = 'col1') */ "
                                                + "SELECT source1.col1, source1.col2, source3.col1, "
                                                + "source3.col2 FROM source1 JOIN source3 ON source1.col1 = source3.col1"))
                .withMessageContaining(
                        "The sink parallelism must be specified when sorting the table data");
    }

    @Test
    public void testRangePartition() throws Exception {
        List<Row> inputRows = generateSinkRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        batchSql(
                "CREATE TEMPORARY TABLE test_source (col1 INT, col2 INT, col3 INT, col4 INT) WITH "
                        + "('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);
        batchSql(
                "INSERT INTO test_table /*+ OPTIONS('sink.clustering.by-columns' = 'col1', "
                        + "'sink.parallelism' = '10', 'sink.clustering.sort-in-cluster' = 'false') */ "
                        + "SELECT * FROM test_source");
        List<Row> sinkRows = batchSql("SELECT * FROM test_table");
        assertThat(sinkRows.size()).isEqualTo(SINK_ROW_NUMBER);
        FileStoreTable testStoreTable = paimonTable("test_table");
        List<ManifestEntry> files = testStoreTable.store().newScan().plan().files();
        assertThat(files.size()).isEqualTo(10);
        List<Tuple2<Integer, Integer>> minMaxOfEachFile = new ArrayList<>();
        for (ManifestEntry file : files) {
            DataSplit dataSplit =
                    DataSplit.builder()
                            .withPartition(file.partition())
                            .withBucket(file.bucket())
                            .withDataFiles(Collections.singletonList(file.file()))
                            .withBucketPath("/temp/xxx")
                            .build();
            final AtomicInteger min = new AtomicInteger(Integer.MAX_VALUE);
            final AtomicInteger max = new AtomicInteger(Integer.MIN_VALUE);
            testStoreTable
                    .newReadBuilder()
                    .newRead()
                    .createReader(dataSplit)
                    .forEachRemaining(
                            internalRow -> {
                                int result = internalRow.getInt(0);
                                min.set(Math.min(min.get(), result));
                                max.set(Math.max(max.get(), result));
                            });
            minMaxOfEachFile.add(Tuple2.of(min.get(), max.get()));
        }
        minMaxOfEachFile.sort(Comparator.comparing(o -> o.f0));
        Tuple2<Integer, Integer> preResult = minMaxOfEachFile.get(0);
        for (int index = 1; index < minMaxOfEachFile.size(); ++index) {
            Tuple2<Integer, Integer> currentResult = minMaxOfEachFile.get(index);
            assertThat(currentResult.f0).isGreaterThanOrEqualTo(0);
            assertThat(currentResult.f1).isLessThanOrEqualTo(SINK_ROW_NUMBER);
            assertThat(currentResult.f0).isGreaterThanOrEqualTo(preResult.f1);
        }
    }

    @Test
    public void testRangePartitionAndSortWithOrderStrategy() throws Exception {
        List<Row> inputRows = generateSinkRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        batchSql(
                "CREATE TEMPORARY TABLE test_source (col1 INT, col2 INT, col3 INT, col4 INT) WITH "
                        + "('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);
        batchSql(
                "INSERT INTO test_table /*+ OPTIONS('sink.clustering.by-columns' = 'col1', "
                        + "'sink.parallelism' = '10', 'sink.clustering.strategy' = 'order') */ "
                        + "SELECT * FROM test_source");
        List<Row> sinkRows = batchSql("SELECT * FROM test_table");
        assertThat(sinkRows.size()).isEqualTo(SINK_ROW_NUMBER);
        FileStoreTable testStoreTable = paimonTable("test_table");
        List<ManifestEntry> files = testStoreTable.store().newScan().plan().files();
        assertThat(files.size()).isEqualTo(10);
        List<Tuple2<Integer, Integer>> minMaxOfEachFile = new ArrayList<>();
        for (ManifestEntry file : files) {
            DataSplit dataSplit =
                    DataSplit.builder()
                            .withPartition(file.partition())
                            .withBucket(file.bucket())
                            .withDataFiles(Collections.singletonList(file.file()))
                            .withBucketPath("/temp/xxx")
                            .build();
            final AtomicInteger min = new AtomicInteger(Integer.MAX_VALUE);
            final AtomicInteger max = new AtomicInteger(Integer.MIN_VALUE);
            final AtomicInteger current = new AtomicInteger(Integer.MIN_VALUE);
            testStoreTable
                    .newReadBuilder()
                    .newRead()
                    .createReader(dataSplit)
                    .forEachRemaining(
                            internalRow -> {
                                int result = internalRow.getInt(0);
                                min.set(Math.min(min.get(), result));
                                max.set(Math.max(max.get(), result));
                                Assertions.assertThat(result).isGreaterThanOrEqualTo(current.get());
                                current.set(result);
                            });
            minMaxOfEachFile.add(Tuple2.of(min.get(), max.get()));
        }
        minMaxOfEachFile.sort(Comparator.comparing(o -> o.f0));
        Tuple2<Integer, Integer> preResult = minMaxOfEachFile.get(0);
        for (int index = 1; index < minMaxOfEachFile.size(); ++index) {
            Tuple2<Integer, Integer> currentResult = minMaxOfEachFile.get(index);
            assertThat(currentResult.f0).isGreaterThanOrEqualTo(0);
            assertThat(currentResult.f1).isLessThanOrEqualTo(SINK_ROW_NUMBER);
            assertThat(currentResult.f0).isGreaterThanOrEqualTo(preResult.f1);
        }
    }

    @Test
    public void testRangePartitionAndSortWithZOrderStrategy() throws Exception {
        List<Row> inputRows = generateSinkRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        batchSql(
                "CREATE TEMPORARY TABLE test_source (col1 INT, col2 INT, col3 INT, col4 INT) WITH "
                        + "('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);
        batchSql(
                "INSERT INTO test_table /*+ OPTIONS('sink.clustering.by-columns' = 'col1', "
                        + "'sink.parallelism' = '10', 'sink.clustering.strategy' = 'zorder') */ "
                        + "SELECT * FROM test_source");
        List<Row> sinkRows = batchSql("SELECT * FROM test_table");
        assertThat(sinkRows.size()).isEqualTo(SINK_ROW_NUMBER);
        FileStoreTable testStoreTable = paimonTable("test_table");
        PredicateBuilder predicateBuilder = new PredicateBuilder(testStoreTable.rowType());
        Predicate predicate = predicateBuilder.between(0, 100, 200);
        List<ManifestEntry> files = testStoreTable.store().newScan().plan().files();
        assertThat(files.size()).isEqualTo(10);
        List<ManifestEntry> filesFilter =
                ((AppendOnlyFileStoreScan) testStoreTable.store().newScan())
                        .withFilter(predicate)
                        .plan()
                        .files();
        Assertions.assertThat(files.size()).isGreaterThan(filesFilter.size());
    }

    @Test
    public void testRangePartitionAndSortWithHilbertStrategy() throws Exception {
        List<Row> inputRows = generateSinkRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        batchSql(
                "CREATE TEMPORARY TABLE test_source (col1 INT, col2 INT, col3 INT, col4 INT) WITH "
                        + "('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);
        batchSql(
                "INSERT INTO test_table /*+ OPTIONS('sink.clustering.by-columns' = 'col1,col2', "
                        + "'sink.parallelism' = '10', 'sink.clustering.strategy' = 'hilbert') */ "
                        + "SELECT * FROM test_source");
        List<Row> sinkRows = batchSql("SELECT * FROM test_table");
        assertThat(sinkRows.size()).isEqualTo(SINK_ROW_NUMBER);
        FileStoreTable testStoreTable = paimonTable("test_table");
        PredicateBuilder predicateBuilder = new PredicateBuilder(testStoreTable.rowType());
        Predicate predicate = predicateBuilder.between(0, 100, 200);
        List<ManifestEntry> files = testStoreTable.store().newScan().plan().files();
        assertThat(files.size()).isEqualTo(10);
        List<ManifestEntry> filesFilter =
                ((AppendOnlyFileStoreScan) testStoreTable.store().newScan())
                        .withFilter(predicate)
                        .plan()
                        .files();
        Assertions.assertThat(files.size()).isGreaterThan(filesFilter.size());
    }

    private List<Row> generateSinkRows() {
        List<Row> sinkRows = new ArrayList<>();
        Random random = new Random();
        for (int round = 0; round < SINK_ROW_NUMBER; round++) {
            sinkRows.add(
                    Row.ofKind(
                            RowKind.INSERT,
                            random.nextInt(SINK_ROW_NUMBER),
                            random.nextInt(SINK_ROW_NUMBER),
                            random.nextInt(SINK_ROW_NUMBER),
                            random.nextInt(SINK_ROW_NUMBER)));
        }
        return sinkRows;
    }

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS test_table (col1 INT, col2 INT, col3 INT, col4 INT)");
    }
}
