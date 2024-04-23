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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/** Sort Compact Action tests for dynamic bucket table. */
public class SortCompactActionForDynamicBucketITCase extends ActionITCaseBase {

    private static final Random RANDOM = new Random();

    @Test
    public void testDynamicBucketSort() throws Exception {
        createTable();

        commit(writeData(100));
        PredicateBuilder predicateBuilder = new PredicateBuilder(getTable().rowType());
        Predicate predicate = predicateBuilder.between(1, 100L, 200L);

        List<ManifestEntry> files = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilter =
                ((KeyValueFileStoreScan) getTable().store().newScan())
                        .withValueFilter(predicate)
                        .plan()
                        .files();

        zorder(Arrays.asList("f2", "f1"));

        List<ManifestEntry> filesZorder = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilterZorder =
                ((KeyValueFileStoreScan) getTable().store().newScan())
                        .withValueFilter(predicate)
                        .plan()
                        .files();
        Assertions.assertThat(filesFilterZorder.size() / (double) filesZorder.size())
                .isLessThan(filesFilter.size() / (double) files.size());
    }

    @Test
    public void testDynamicBucketSortWithOrderAndZorder() throws Exception {
        createTable();

        commit(writeData(100));
        PredicateBuilder predicateBuilder = new PredicateBuilder(getTable().rowType());
        Predicate predicate = predicateBuilder.between(1, 100L, 200L);

        // order f2,f1 will make predicate of f1 perform worse.
        order(Arrays.asList("f2", "f1"));
        List<ManifestEntry> files = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilter =
                ((KeyValueFileStoreScan) getTable().store().newScan())
                        .withValueFilter(predicate)
                        .plan()
                        .files();

        zorder(Arrays.asList("f2", "f1"));

        List<ManifestEntry> filesZorder = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilterZorder =
                ((KeyValueFileStoreScan) getTable().store().newScan())
                        .withValueFilter(predicate)
                        .plan()
                        .files();

        Assertions.assertThat(filesFilterZorder.size() / (double) filesZorder.size())
                .isLessThan(filesFilter.size() / (double) files.size());
    }

    @Test
    public void testDynamicBucketSortWithOrderAndHilbert() throws Exception {
        createTable();

        commit(writeData(100));
        PredicateBuilder predicateBuilder = new PredicateBuilder(getTable().rowType());
        Predicate predicate = predicateBuilder.between(1, 100L, 200L);

        // order f2,f1 will make predicate of f1 perform worse.
        order(Arrays.asList("f2", "f1"));
        List<ManifestEntry> files = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilter =
                ((KeyValueFileStoreScan) getTable().store().newScan())
                        .withValueFilter(predicate)
                        .plan()
                        .files();

        hilbert(Arrays.asList("f2", "f1"));

        List<ManifestEntry> filesHilbert = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilterHilbert =
                ((KeyValueFileStoreScan) getTable().store().newScan())
                        .withValueFilter(predicate)
                        .plan()
                        .files();

        Assertions.assertThat(filesFilterHilbert.size() / (double) filesHilbert.size())
                .isLessThan(filesFilter.size() / (double) files.size());
    }

    @Test
    public void testDynamicBucketSortWithStringType() throws Exception {
        createTable();

        commit(writeData(100));
        PredicateBuilder predicateBuilder = new PredicateBuilder(getTable().rowType());
        Predicate predicate =
                predicateBuilder.between(
                        4,
                        BinaryString.fromString("000000000" + 100),
                        BinaryString.fromString("000000000" + 200));

        List<ManifestEntry> files = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilter =
                ((KeyValueFileStoreScan) getTable().store().newScan())
                        .withValueFilter(predicate)
                        .plan()
                        .files();

        zorder(Collections.singletonList("f4"));

        List<ManifestEntry> filesZorder = getTable().store().newScan().plan().files();
        List<ManifestEntry> filesFilterZorder =
                ((KeyValueFileStoreScan) getTable().store().newScan())
                        .withValueFilter(predicate)
                        .plan()
                        .files();
        Assertions.assertThat(filesFilterZorder.size() / (double) filesZorder.size())
                .isLessThan(filesFilter.size() / (double) files.size());
    }

    private void zorder(List<String> columns) throws Exception {
        createAction("zorder", columns).run();
    }

    private void hilbert(List<String> columns) throws Exception {
        createAction("hilbert", columns).run();
    }

    private void order(List<String> columns) throws Exception {
        createAction("order", columns).run();
    }

    private SortCompactAction createAction(String orderStrategy, List<String> columns) {
        return createAction(
                SortCompactAction.class,
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
                String.join(",", columns));
    }

    // schema with all the basic types.
    private static Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.BIGINT());
        schemaBuilder.column("f1", DataTypes.BIGINT());
        schemaBuilder.column("f2", DataTypes.BIGINT());
        schemaBuilder.column("f3", DataTypes.BIGINT());
        schemaBuilder.column("f4", DataTypes.STRING());
        schemaBuilder.option("bucket", "-1");
        schemaBuilder.option("scan.parallelism", "6");
        schemaBuilder.option("sink.parallelism", "3");
        schemaBuilder.option("dynamic-bucket.target-row-num", "100");
        schemaBuilder.option(CoreOptions.ZORDER_VAR_LENGTH_CONTRIBUTION.key(), "14");
        schemaBuilder.primaryKey("f0");
        return schemaBuilder.build();
    }

    private List<CommitMessage> writeData(int size) throws Exception {
        List<CommitMessage> messages;
        Table table = getTable();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite batchTableWrite = builder.newWrite()) {
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < 100; j++) {
                    Pair<InternalRow, Integer> rowWithBucket = data(i);
                    batchTableWrite.write(rowWithBucket.getKey(), rowWithBucket.getValue());
                }
            }
            messages = batchTableWrite.prepareCommit();
        }

        return messages;
    }

    private void commit(List<CommitMessage> messages) throws Exception {
        BatchTableCommit commit = getTable().newBatchWriteBuilder().newCommit();
        commit.commit(messages);
        commit.close();
    }

    private void createTable() throws Exception {
        catalog.createDatabase(database, true);
        catalog.createTable(identifier(), schema(), true);
    }

    private FileStoreTable getTable() throws Exception {
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private Identifier identifier() {
        return Identifier.create(database, tableName);
    }

    private static Pair<InternalRow, Integer> data(int bucket) {
        String in = String.valueOf(Math.abs(RANDOM.nextInt(10000)));
        int count = 4 - in.length();
        for (int i = 0; i < count; i++) {
            in = "0" + in;
        }
        assert in.length() == 4;
        GenericRow row =
                GenericRow.of(
                        RANDOM.nextLong(),
                        (long) RANDOM.nextInt(10000),
                        (long) RANDOM.nextInt(10000),
                        (long) RANDOM.nextInt(10000),
                        BinaryString.fromString("00000000" + in));
        return Pair.of(row, bucket);
    }
}
