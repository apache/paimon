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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/** Order Rewrite Action tests for {@link SortCompactAction}. */
public class SortCompactActionForUnawareBucketITCase extends ActionITCaseBase {

    private static final Random RANDOM = new Random();

    private void prepareData(int size, int loop) throws Exception {
        createTable();
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (int i = 0; i < loop; i++) {
            commitMessages.addAll(writeData(size));
        }
        commit(commitMessages);
    }

    private void prepareSameData(int size) throws Exception {
        createTable();
        BatchWriteBuilder builder = getTable().newBatchWriteBuilder();
        try (BatchTableWrite batchTableWrite = builder.newWrite()) {
            for (int i = 0; i < size; i++) {
                batchTableWrite.write(data(0, 0, 0));
            }
            commit(batchTableWrite.prepareCommit());
        }
    }

    @Test
    public void testOrderBy() throws Exception {
        prepareData(300, 1);
        Assertions.assertThatCode(
                        () ->
                                order(
                                        Arrays.asList(
                                                "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7",
                                                "f8", "f9", "f10", "f11", "f12", "f13", "f14",
                                                "f15")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testOrderResult() throws Exception {
        prepareData(300, 2);
        Assertions.assertThatCode(() -> order(Arrays.asList("f1", "f2")))
                .doesNotThrowAnyException();

        List<ManifestEntry> files = getTable().store().newScan().plan().files();

        ManifestEntry entry = files.get(0);
        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(entry.partition())
                        .withBucket(entry.bucket())
                        .withDataFiles(Collections.singletonList(entry.file()))
                        .build();

        final AtomicInteger i = new AtomicInteger(Integer.MIN_VALUE);
        getTable()
                .newReadBuilder()
                .newRead()
                .createReader(dataSplit)
                .forEachRemaining(
                        a -> {
                            Integer current = a.getInt(1);
                            Assertions.assertThat(current).isGreaterThanOrEqualTo(i.get());
                            i.set(current);
                        });

        Assertions.assertThatCode(() -> order(Arrays.asList("f2", "f1")))
                .doesNotThrowAnyException();

        files = getTable().store().newScan().plan().files();

        entry = files.get(0);
        dataSplit =
                DataSplit.builder()
                        .withPartition(entry.partition())
                        .withBucket(entry.bucket())
                        .withDataFiles(Collections.singletonList(entry.file()))
                        .build();

        i.set(Integer.MIN_VALUE);
        getTable()
                .newReadBuilder()
                .newRead()
                .createReader(dataSplit)
                .forEachRemaining(
                        a -> {
                            Integer current = a.getInt(2);
                            Assertions.assertThat(current).isGreaterThanOrEqualTo(i.get());
                            i.set(current);
                        });
    }

    @Test
    public void testAllBasicTypeWorksWithZorder() throws Exception {
        prepareData(300, 1);
        // All the basic types should support zorder
        Assertions.assertThatCode(
                        () ->
                                zorder(
                                        Arrays.asList(
                                                "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7",
                                                "f8", "f9", "f10", "f11", "f12", "f13", "f14",
                                                "f15")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testAllBasicTypeWorksWithHilbert() throws Exception {
        prepareData(300, 1);
        // All the basic types should support hilbert
        Assertions.assertThatCode(
                        () ->
                                hilbert(
                                        Arrays.asList(
                                                "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7",
                                                "f8", "f9", "f10", "f11", "f12", "f13", "f14",
                                                "f15")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testZorderActionWorks() throws Exception {
        prepareData(300, 2);
        PredicateBuilder predicateBuilder = new PredicateBuilder(getTable().rowType());
        Predicate predicate = predicateBuilder.between(1, 100, 200);

        List<ManifestEntry> files = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilter =
                ((AppendOnlyFileStoreScan) getTable().store().newScan())
                        .withFilter(predicate)
                        .plan()
                        .files();
        // before zorder, we don't filter any file
        Assertions.assertThat(files.size()).isEqualTo(filesFilter.size());

        zorder(Arrays.asList("f2", "f1"));

        files = getTable().store().newScan().plan().files();
        filesFilter =
                ((AppendOnlyFileStoreScan) getTable().store().newScan())
                        .withFilter(predicate)
                        .plan()
                        .files();
        Assertions.assertThat(files.size()).isGreaterThan(filesFilter.size());
    }

    @Test
    public void testHilbertActionWorks() throws Exception {
        prepareData(300, 2);
        PredicateBuilder predicateBuilder = new PredicateBuilder(getTable().rowType());
        Predicate predicate = predicateBuilder.between(1, 100, 200);

        List<ManifestEntry> files = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilter =
                ((AppendOnlyFileStoreScan) getTable().store().newScan())
                        .withFilter(predicate)
                        .plan()
                        .files();

        // before hilbert, we don't filter any file
        Assertions.assertThat(files.size()).isEqualTo(filesFilter.size());

        hilbert(Arrays.asList("f2", "f1"));

        files = getTable().store().newScan().plan().files();
        filesFilter =
                ((AppendOnlyFileStoreScan) getTable().store().newScan())
                        .withFilter(predicate)
                        .plan()
                        .files();
        Assertions.assertThat(files.size()).isGreaterThan(filesFilter.size());
    }

    @Test
    public void testCompareZorderAndOrder() throws Exception {
        prepareData(300, 10);
        zorder(Arrays.asList("f2", "f1"));

        PredicateBuilder predicateBuilder = new PredicateBuilder(getTable().rowType());
        Predicate predicate = predicateBuilder.between(1, 10, 20);

        List<ManifestEntry> filesZorder = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilterZorder =
                ((AppendOnlyFileStoreScan) getTable().store().newScan())
                        .withFilter(predicate)
                        .plan()
                        .files();

        order(Arrays.asList("f2", "f1"));
        List<ManifestEntry> filesOrder = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilterOrder =
                ((AppendOnlyFileStoreScan) getTable().store().newScan())
                        .withFilter(predicate)
                        .plan()
                        .files();

        Assertions.assertThat(filesFilterZorder.size() / (double) filesZorder.size())
                .isLessThan(filesFilterOrder.size() / (double) filesOrder.size());
    }

    @Test
    public void testCompareHilbertAndOrder() throws Exception {
        prepareData(300, 10);

        hilbert(Arrays.asList("f2", "f1"));
        PredicateBuilder predicateBuilder = new PredicateBuilder(getTable().rowType());
        Predicate predicate = predicateBuilder.between(1, 10, 20);

        List<ManifestEntry> filesHilbert = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilterHilbert =
                ((AppendOnlyFileStoreScan) getTable().store().newScan())
                        .withFilter(predicate)
                        .plan()
                        .files();

        order(Arrays.asList("f2", "f1"));
        List<ManifestEntry> filesOrder = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilterOrder =
                ((AppendOnlyFileStoreScan) getTable().store().newScan())
                        .withFilter(predicate)
                        .plan()
                        .files();

        Assertions.assertThat(filesFilterHilbert.size() / (double) filesHilbert.size())
                .isLessThan(filesFilterOrder.size() / (double) filesOrder.size());
    }

    @Test
    public void testTableConf() throws Exception {
        createTable();
        SortCompactAction sortCompactAction =
                new SortCompactAction(
                                warehouse,
                                database,
                                tableName,
                                Collections.emptyMap(),
                                Collections.singletonMap(
                                        FlinkConnectorOptions.SINK_PARALLELISM.key(), "20"))
                        .withOrderStrategy("zorder")
                        .withOrderColumns(Collections.singletonList("f0"));

        Assertions.assertThat(
                        sortCompactAction
                                .table
                                .options()
                                .get(FlinkConnectorOptions.SINK_PARALLELISM.key()))
                .isEqualTo("20");
    }

    @Test
    public void testRandomSuffixWorks() throws Exception {
        prepareSameData(200);
        Assertions.assertThatCode(() -> order(Collections.singletonList("f1")))
                .doesNotThrowAnyException();
        List<ManifestEntry> files = getTable().store().newScan().plan().files();
        Assertions.assertThat(files.size()).isEqualTo(3);

        dropTable();
        prepareSameData(200);
        Assertions.assertThatCode(() -> zorder(Arrays.asList("f1", "f2")))
                .doesNotThrowAnyException();
        files = getTable().store().newScan().plan().files();
        Assertions.assertThat(files.size()).isEqualTo(3);
    }

    @Test
    public void testSortCompactionOnEmptyData() throws Exception {
        createTable();
        SortCompactAction sortCompactAction =
                new SortCompactAction(
                                warehouse,
                                database,
                                tableName,
                                Collections.emptyMap(),
                                Collections.emptyMap())
                        .withOrderStrategy("zorder")
                        .withOrderColumns(Collections.singletonList("f0"));

        sortCompactAction.run();
    }

    private void zorder(List<String> columns) throws Exception {
        String rangeStrategy = RANDOM.nextBoolean() ? "size" : "quantity";
        createAction("zorder", rangeStrategy, columns).run();
    }

    private void hilbert(List<String> columns) throws Exception {
        String rangeStrategy = RANDOM.nextBoolean() ? "size" : "quantity";
        createAction("hilbert", rangeStrategy, columns).run();
    }

    private void order(List<String> columns) throws Exception {
        String rangeStrategy = RANDOM.nextBoolean() ? "size" : "quantity";
        createAction("order", rangeStrategy, columns).run();
    }

    private SortCompactAction createAction(
            String orderStrategy, String rangeStrategy, List<String> columns) {
        return createAction(orderStrategy, rangeStrategy, columns, Lists.newArrayList());
    }

    private SortCompactAction createAction(
            String orderStrategy,
            String rangeStrategy,
            List<String> columns,
            List<String> extraConfigs) {
        ArrayList<String> args =
                Lists.newArrayList(
                        "compact",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--order_strategy",
                        orderStrategy,
                        "--order_by",
                        String.join(",", columns),
                        "--table_conf",
                        "sort-compaction.range-strategy=" + rangeStrategy);
        args.addAll(extraConfigs);
        return createAction(SortCompactAction.class, args.toArray(new String[0]));
    }

    @Test
    public void testvalidSampleConfig() throws Exception {
        prepareData(300, 1);
        {
            ArrayList<String> extraCompactionConfig =
                    Lists.newArrayList(
                            "--table_conf", "sort-compaction.local-sample.magnification=1");
            Assertions.assertThatCode(
                            () -> {
                                createAction(
                                                "order",
                                                "size",
                                                Arrays.asList(
                                                        "f0", "f1", "f2", "f3", "f4", "f5", "f6",
                                                        "f7", "f8", "f9", "f10", "f11", "f12",
                                                        "f13", "f14", "f15"),
                                                extraCompactionConfig)
                                        .run();
                            })
                    .hasMessage(
                            "the config 'sort-compaction.local-sample.magnification=1' should not be set too small,greater than or equal to 20 is needed.");
        }
    }

    private void createTable() throws Exception {
        catalog.createDatabase(database, true);
        catalog.createTable(identifier(), schema(), true);
    }

    private void dropTable() throws Exception {
        catalog.dropTable(identifier(), true);
    }

    private Identifier identifier() {
        return Identifier.create(database, tableName);
    }

    private void commit(List<CommitMessage> messages) throws Exception {
        BatchTableCommit commit = getTable().newBatchWriteBuilder().newCommit();
        commit.commit(messages);
        commit.close();
    }

    // schema with all the basic types.
    private static Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.TINYINT());
        schemaBuilder.column("f1", DataTypes.INT());
        schemaBuilder.column("f2", DataTypes.SMALLINT());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.column("f4", DataTypes.DOUBLE());
        schemaBuilder.column("f5", DataTypes.CHAR(10));
        schemaBuilder.column("f6", DataTypes.VARCHAR(10));
        schemaBuilder.column("f7", DataTypes.BOOLEAN());
        schemaBuilder.column("f8", DataTypes.DATE());
        schemaBuilder.column("f9", DataTypes.TIME());
        schemaBuilder.column("f10", DataTypes.TIMESTAMP());
        schemaBuilder.column("f11", DataTypes.DECIMAL(10, 2));
        schemaBuilder.column("f12", DataTypes.BYTES());
        schemaBuilder.column("f13", DataTypes.FLOAT());
        schemaBuilder.column("f14", DataTypes.BINARY(10));
        schemaBuilder.column("f15", DataTypes.VARBINARY(10));
        schemaBuilder.option("bucket", "-1");
        schemaBuilder.option("scan.parallelism", "6");
        schemaBuilder.option("sink.parallelism", "3");
        schemaBuilder.option("target-file-size", "1 M");
        schemaBuilder.partitionKeys("f0");
        return schemaBuilder.build();
    }

    private List<CommitMessage> writeData(int size) throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            messages.addAll(writeOnce(getTable(), i, size));
        }

        return messages;
    }

    private FileStoreTable getTable() throws Exception {
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private static List<CommitMessage> writeOnce(Table table, int p, int size) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite batchTableWrite = builder.newWrite()) {
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    batchTableWrite.write(data(p, i, j));
                }
            }
            return batchTableWrite.prepareCommit();
        }
    }

    private static InternalRow data(int p, int i, int j) {
        return GenericRow.of(
                (byte) p,
                j,
                (short) i,
                BinaryString.fromString(String.valueOf(j)),
                0.1 + i,
                BinaryString.fromString(String.valueOf(j)),
                BinaryString.fromString(String.valueOf(i)),
                j % 2 == 1,
                i,
                j,
                Timestamp.fromEpochMillis(i),
                Decimal.zero(10, 2),
                String.valueOf(i).getBytes(),
                (float) 0.1 + j,
                randomBytes(),
                randomBytes());
    }

    private static byte[] randomBytes() {
        byte[] binary = new byte[RANDOM.nextInt(10)];
        RANDOM.nextBytes(binary);
        return binary;
    }
}
