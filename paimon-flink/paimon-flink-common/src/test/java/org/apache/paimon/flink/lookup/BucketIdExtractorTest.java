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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.flink.lookup.partitioner.BucketIdExtractor;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** The test for {@link BucketIdExtractor}. */
public class BucketIdExtractorTest extends CatalogITCaseBase {

    private static final int BUCKET_NUMBER = 5;

    private static final int COL_NUMBER = 5;

    private static final int ROW_NUMBER = 100;

    @Test
    public void testJoinKeyEqualToSingleBucketKey() throws Exception {
        Random seed = new Random();
        int bucketId = seed.nextInt(COL_NUMBER);
        int bucketKeyIndex = seed.nextInt(5);
        String bucketKeyName = "col" + (bucketKeyIndex + 1);
        FileStoreTable table = createTestTable(bucketKeyName);
        List<List<Object>> bucketKeyRows =
                getGroundTruthBucketKeyRows(
                        table,
                        bucketId,
                        createBucketKeyGetter(Collections.singletonList(bucketKeyIndex)));
        List<String> joinKeyNames = Collections.singletonList(bucketKeyName);
        List<RowData> joinKeyRows =
                generateJoinKeyRows(
                        bucketKeyRows, bucketKeyRow -> GenericRowData.of(bucketKeyRow.get(0)));
        BucketIdExtractor bucketIdExtractor =
                new BucketIdExtractor(BUCKET_NUMBER, table.schema(), joinKeyNames, joinKeyNames);
        checkCorrectness(bucketIdExtractor, bucketId, joinKeyRows);
    }

    @Test
    public void testJoinKeyEqualToSingleBucketKeyWithBucketStrategy() throws Exception {
        String bucketKeyName = "col1";
        Random seed = new Random();
        int bucketId = seed.nextInt(COL_NUMBER);
        FileStoreTable table = createTestTable("col1", "truncate[1]");
        List<List<Object>> bucketKeyRows =
                getGroundTruthBucketKeyRows(
                        table, bucketId, createBucketKeyGetter(Collections.singletonList(0)));
        List<String> joinKeyNames = Collections.singletonList(bucketKeyName);
        List<RowData> joinKeyRows =
                generateJoinKeyRows(
                        bucketKeyRows, bucketKeyRow -> GenericRowData.of(bucketKeyRow.get(0)));
        BucketIdExtractor bucketIdExtractor =
                new BucketIdExtractor(BUCKET_NUMBER, table.schema(), joinKeyNames, joinKeyNames);
        checkCorrectness(bucketIdExtractor, bucketId, joinKeyRows);
    }

    @Test
    public void testJoinKeysContainSingleKey() throws Exception {
        Random seed = new Random();
        List<List<Integer>> joinKeyIndexes = getColumnIndexCombinations();
        int bucketId = seed.nextInt(COL_NUMBER);
        List<Integer> joinKeyIndex = joinKeyIndexes.get(seed.nextInt(joinKeyIndexes.size()));
        int bucketKeyIndex = joinKeyIndex.get(1);
        String bucketKeyName = "col" + bucketKeyIndex;
        FileStoreTable table = createTestTable(bucketKeyName);
        List<List<Object>> bucketKeyRows =
                getGroundTruthBucketKeyRows(
                        table,
                        bucketId,
                        createBucketKeyGetter(Collections.singletonList(bucketKeyIndex - 1)));
        List<String> bucketKeyNames = Collections.singletonList(bucketKeyName);
        List<String> joinKeyNames = Arrays.asList("col" + joinKeyIndex.get(0), bucketKeyName);
        List<RowData> joinKeyRows =
                generateJoinKeyRows(
                        bucketKeyRows,
                        bucketKeyRow ->
                                GenericRowData.of(
                                        generateFakeColumnValue(joinKeyIndex.get(0)),
                                        bucketKeyRow.get(0)));
        BucketIdExtractor bucketIdExtractor =
                new BucketIdExtractor(BUCKET_NUMBER, table.schema(), joinKeyNames, bucketKeyNames);
        checkCorrectness(bucketIdExtractor, bucketId, joinKeyRows);
    }

    @Test
    public void testJoinKeysEqualToMultiBucketKeys() throws Exception {
        Random seed = new Random();
        List<List<Integer>> bucketKeyIndexes = getColumnIndexCombinations();
        int bucketId = seed.nextInt(COL_NUMBER);
        List<Integer> bucketKeyIndex = bucketKeyIndexes.get(seed.nextInt(bucketKeyIndexes.size()));
        String bucketKeyName = "col" + bucketKeyIndex.get(0) + ",col" + bucketKeyIndex.get(1);
        FileStoreTable table = createTestTable(bucketKeyName);
        List<List<Object>> bucketKeyRows =
                getGroundTruthBucketKeyRows(
                        table,
                        bucketId,
                        createBucketKeyGetter(
                                Arrays.asList(
                                        bucketKeyIndex.get(0) - 1, bucketKeyIndex.get(1) - 1)));
        List<String> joinKeyNames = Arrays.asList(bucketKeyName.split(","));
        List<RowData> joinKeyRows =
                generateJoinKeyRows(
                        bucketKeyRows,
                        bucketKeyRow ->
                                GenericRowData.of(bucketKeyRow.get(0), bucketKeyRow.get(1)));
        BucketIdExtractor bucketIdExtractor =
                new BucketIdExtractor(BUCKET_NUMBER, table.schema(), joinKeyNames, joinKeyNames);
        checkCorrectness(bucketIdExtractor, bucketId, joinKeyRows);
    }

    private List<RowData> generateJoinKeyRows(
            List<List<Object>> bucketKeyRows, Function<List<Object>, RowData> converter) {
        List<RowData> joinKeyRows = new ArrayList<>();
        for (List<Object> bucketKeyRow : bucketKeyRows) {
            joinKeyRows.add(converter.apply(bucketKeyRow));
        }
        return joinKeyRows;
    }

    private void checkCorrectness(
            BucketIdExtractor extractor, int targetBucketId, List<RowData> joinKeyRows) {
        for (RowData joinKeyRow : joinKeyRows) {
            assertThat(extractor.extractBucketId(joinKeyRow)).isEqualTo(targetBucketId);
        }
    }

    private List<List<Object>> getGroundTruthBucketKeyRows(
            FileStoreTable table, int bucketId, Function<InternalRow, List<Object>> bucketKeyGetter)
            throws IOException {
        List<ManifestEntry> files = table.store().newScan().withBucket(bucketId).plan().files();
        List<List<Object>> bucketKeyRows = new ArrayList<>();
        for (ManifestEntry file : files) {
            DataSplit dataSplit =
                    DataSplit.builder()
                            .withPartition(file.partition())
                            .withBucket(file.bucket())
                            .withDataFiles(Collections.singletonList(file.file()))
                            .withBucketPath("not used")
                            .build();
            table.newReadBuilder()
                    .newRead()
                    .createReader(dataSplit)
                    .forEachRemaining(
                            internalRow -> {
                                bucketKeyRows.add(bucketKeyGetter.apply(internalRow));
                            });
        }
        return bucketKeyRows;
    }

    private FileStoreTable createTestTable(String bucketKey) throws Exception {
        return createTestTable(bucketKey, null);
    }

    private FileStoreTable createTestTable(String bucketKey, @Nullable String bucketStrategy)
            throws Exception {
        String tableName = "Test";
        String ddl =
                String.format(
                        "CREATE TABLE %s (col1 INT, col2 STRING, col3 FLOAT, col4 INT, col5 BOOLEAN ) WITH"
                                + " ('bucket'='%s'",
                        tableName, BUCKET_NUMBER);
        if (bucketStrategy != null) {
            ddl += ", 'bucket-strategy'='" + bucketStrategy + "'";
        }
        if (bucketKey != null) {
            ddl += ", 'bucket-key' = '" + bucketKey + "')";
        }
        batchSql(ddl);
        Random seed = new Random();
        StringBuilder dml = new StringBuilder(String.format("INSERT INTO %s VALUES ", tableName));
        for (int index = 1; index < ROW_NUMBER; ++index) {
            dml.append(
                    String.format(
                            "(%s, '%s', %s, %s, %s), ",
                            seed.nextInt(ROW_NUMBER),
                            seed.nextInt(ROW_NUMBER),
                            101.1F + seed.nextInt(50),
                            seed.nextInt(ROW_NUMBER),
                            (seed.nextBoolean() ? "true" : "false")));
        }
        dml.append(
                String.format(
                        "(%s, '%s', %s, %s, %s)",
                        seed.nextInt(ROW_NUMBER),
                        seed.nextInt(ROW_NUMBER),
                        101.1F + seed.nextInt(50),
                        seed.nextInt(ROW_NUMBER),
                        (seed.nextBoolean() ? "true" : "false")));
        batchSql(dml.toString());
        return paimonTable(tableName);
    }

    private Function<InternalRow, List<Object>> createBucketKeyGetter(
            List<Integer> bucketKeyIndexes) {
        return row -> {
            List<Object> bucketKeys = new ArrayList<>();
            for (Integer bucketKeyIndex : bucketKeyIndexes) {
                switch (bucketKeyIndex) {
                    case 0:
                        bucketKeys.add(row.getInt(0));
                        break;
                    case 1:
                        bucketKeys.add(StringData.fromString(row.getString(1).toString()));
                        break;
                    case 2:
                        bucketKeys.add(row.getFloat(2));
                        break;
                    case 3:
                        bucketKeys.add(row.getInt(3));
                        break;
                    case 4:
                        bucketKeys.add(row.getBoolean(4));
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            return bucketKeys;
        };
    }

    private List<List<Integer>> getColumnIndexCombinations() {
        List<List<Integer>> bucketIndexes = new ArrayList<>();
        for (int i = 1; i <= BUCKET_NUMBER; ++i) {
            for (int j = i + 1; j <= BUCKET_NUMBER; ++j) {
                bucketIndexes.add(Arrays.asList(i, j));
            }
        }
        return bucketIndexes;
    }

    private Object generateFakeColumnValue(Integer columnIndex) {
        switch (columnIndex) {
            case 0:
                return 1;
            case 1:
                return StringData.fromString("Test");
            case 2:
                return 1.21F;
            case 3:
                return 10;
            case 4:
                return false;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
