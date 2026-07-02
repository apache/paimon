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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link CoreOptions#SCAN_BUCKET}. */
public class ScanBucketITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return singletonList(
                "CREATE TABLE IF NOT EXISTS T (id INT, v INT, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('bucket' = '4')");
    }

    @Nullable
    @Override
    protected Boolean sqlSyncMode() {
        return true;
    }

    @Test
    public void testScanBucketFilter() throws Exception {
        FileStoreTable table = paimonTable("T");
        writeRows(table, 1, 10, 2, 20, 3, 30, 4, 40, 5, 50, 6, 60, 7, 70, 8, 80);

        int targetBucket = 0;
        for (int bucket = 0; bucket < 4; bucket++) {
            List<ManifestEntry> files = table.store().newScan().withBucket(bucket).plan().files();
            if (!files.isEmpty()) {
                targetBucket = bucket;
                break;
            }
        }

        List<Row> expected = readRowsFromBucket(table, targetBucket);

        List<Row> actual =
                batchSql(
                        String.format(
                                "SELECT * FROM T /*+ OPTIONS('scan.bucket' = '%s') */",
                                targetBucket));

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT COUNT(*) FROM T /*+ OPTIONS('scan.bucket' = '%s') */",
                                        targetBucket)))
                .containsExactly(Row.of((long) expected.size()));
    }

    @Test
    public void testScanBucketRejectsDynamicBucketTable() {
        sql(
                "CREATE TABLE dynamic_t (id INT, v INT, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('bucket' = '-1')");

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "SELECT * FROM dynamic_t /*+ OPTIONS('scan.bucket' = '0') */"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Bucket scan is only supported for fixed-bucket tables"));
    }

    @Test
    public void testScanBucketRejectsBucketUnawareTable() {
        sql("CREATE TABLE append_t (id INT, v INT) WITH ('bucket' = '-1')");

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "SELECT * FROM append_t /*+ OPTIONS('scan.bucket' = '0') */"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Bucket scan is only supported for primary key tables"));
    }

    @Test
    public void testScanBucketRejectsPostponeBucketTable() {
        sql(
                "CREATE TABLE postpone_t (id INT, v INT, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('bucket' = '-2')");

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "SELECT * FROM postpone_t /*+ OPTIONS('scan.bucket' = '0') */"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Bucket scan is only supported for fixed-bucket tables"));
    }

    private void writeRows(FileStoreTable table, int... idAndValues) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        for (int i = 0; i < idAndValues.length; i += 2) {
            write.write(GenericRow.of(idAndValues[i], idAndValues[i + 1]));
        }
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(write.prepareCommit());
        write.close();
        commit.close();
    }

    private List<Row> readRowsFromBucket(FileStoreTable table, int bucket) throws Exception {
        List<ManifestEntry> files = table.store().newScan().withBucket(bucket).plan().files();
        List<Row> rows = new ArrayList<>();
        for (ManifestEntry file : files) {
            DataSplit split =
                    DataSplit.builder()
                            .withPartition(file.partition())
                            .withBucket(file.bucket())
                            .withDataFiles(Collections.singletonList(file.file()))
                            .withBucketPath("not used")
                            .build();
            RecordReader<InternalRow> reader = table.newReadBuilder().newRead().createReader(split);
            RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader);
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                rows.add(Row.of(row.getInt(0), row.getInt(1)));
            }
            iterator.close();
        }
        return rows;
    }
}
